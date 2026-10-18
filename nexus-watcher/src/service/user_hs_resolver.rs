//! # User Homeserver Resolver
//!
//! Periodic task that reads each user's PKDNS record and keeps the
//! `(:User)-[:HOSTED_BY]->(:Homeserver)` relationship in Neo4j in step with it.
//!
//! The edge follows the record forward in record time only. Every PKDNS packet
//! carries a timestamp authored by the key holder, and the newest one nexus has
//! acted on is kept on the `User` node as `hs_record_ts`. A record newer than
//! that clock naming another homeserver moves the edge; one that is not newer is
//! ignored, so a replayed or lagging old record cannot drag a user back to a
//! homeserver they left. A record naming no homeserver, or no record at all,
//! says nothing about where the user's data is and leaves the edge alone. The
//! one way for a user to stop being indexed is a record naming a key that is no
//! homeserver: a plain switch to a homeserver that answers nothing.
//!
//! A switch to the primary homeserver flags the new edge `catch_up`: the primary
//! is read in bulk through one global cursor, which has already passed the
//! events this user wrote there while bound elsewhere, so they are read per user
//! by the external loop before the bulk stream takes over. A switch away from
//! the primary stores the bulk cursor as the user's per-user floor there, so a
//! later return replays only the gap.

use nexus_common::db::kv::RedisError;
use nexus_common::db::{
    fetch_key_from_graph, fetch_row_from_graph, queries, GraphResult, PubkyClientResult,
    PubkyConnector,
};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::user::{
    refresh_user_homeserver, set_user_homeserver_from_record, UserHsCursor,
};
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::{global, KeyValue};
use pubky::pkarr::dns::rdata::RData;
use pubky::pkarr::SignedPacket;
use pubky::PublicKey;
use pubky_app_specs::PubkyId;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch::Receiver;
use tracing::{debug, error, info, warn};

static HS_RESOLVER_METRICS: LazyLock<HsResolverMetrics> = LazyLock::new(HsResolverMetrics::new);

/// What a user's PKDNS record says about their homeserver.
// `Missing` carries nothing and `Published` a `PubkyId`; one record is held at a
// time, so the size difference costs nothing worth a `Box`.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkdnsRecord {
    /// No packet was found. On pubky 0.9.3 DHT and relay failures surface this
    /// way too, so it carries no information about the user's homeserver.
    Missing,
    /// The user's packet.
    Published {
        /// The homeserver its `_pubky` record names, or `None` when it names
        /// none: no `_pubky` entry, or a target that is not a public key.
        homeserver: Option<PubkyId>,
        /// The packet's timestamp in microseconds, authored by the key holder.
        /// Orders records of one key against each other and nothing else.
        timestamp: u64,
    },
}

/// Resolves a user's currently published homeserver from PKDNS/DHT.
///
/// Abstracted behind a trait so the resolver loop can be driven with a mock in
/// tests instead of hitting the network.
#[async_trait::async_trait]
pub trait PkdnsHomeserverResolver: Send + Sync {
    /// Looks up the PKDNS record of `user_pk`.
    async fn resolve_homeserver(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord>;
}

/// First wait before an unbound user is looked up again. Doubles per
/// consecutive empty lookup, capped at the resolution TTL.
const UNBOUND_RETRY_INITIAL: Duration = Duration::from_secs(60);

/// In-memory backoff for users with no `HOSTED_BY` edge whose lookup returned
/// nothing published.
///
/// The graph TTL (`HOSTED_BY.resolved_at`) only gates bound users, so without
/// this an unbound user is looked up on every tick. An empty answer can also be
/// a transient DHT or relay miss, so the wait starts short and doubles per
/// consecutive empty lookup up to the TTL: a miss costs a minute, a key that
/// publishes nothing settles at one lookup per TTL. Kept in memory so the
/// resolver writes nothing for users it could not resolve; a restart only costs
/// one extra round of lookups.
#[derive(Default)]
pub struct UnboundBackoff {
    entries: Mutex<HashMap<String, UnboundEntry>>,
}

struct UnboundEntry {
    next_attempt: Instant,
    wait: Duration,
}

impl UnboundBackoff {
    /// Keeps the users whose next lookup is due, and forgets entries for users
    /// no longer in `user_ids` (bound or deleted since), so the map never
    /// outgrows the unbound population.
    fn retain_due(&self, user_ids: Vec<String>, now: Instant) -> Vec<String> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let present: HashSet<&str> = user_ids.iter().map(String::as_str).collect();
        entries.retain(|user_id, _| present.contains(user_id.as_str()));
        user_ids
            .into_iter()
            .filter(|user_id| {
                entries
                    .get(user_id)
                    .is_none_or(|entry| entry.next_attempt <= now)
            })
            .collect()
    }

    /// Records an empty lookup: the user waits twice as long as last time,
    /// starting at [`UNBOUND_RETRY_INITIAL`] and capped at `ttl`.
    fn record_unbound(&self, user_id: &str, now: Instant, ttl: Duration) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let wait = match entries.get(user_id) {
            Some(entry) => entry.wait.saturating_mul(2),
            None => UNBOUND_RETRY_INITIAL,
        }
        .min(ttl);
        entries.insert(
            user_id.to_string(),
            UnboundEntry {
                next_attempt: now + wait,
                wait,
            },
        );
    }

    /// Clears the user's backoff after a lookup that returned a homeserver.
    fn clear(&self, user_id: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(user_id);
    }
}

/// Production resolver backed by the shared [`PubkyConnector`].
pub struct PubkyConnectorResolver;

#[async_trait::async_trait]
impl PkdnsHomeserverResolver for PubkyConnectorResolver {
    async fn resolve_homeserver(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
        let pubky = PubkyConnector::get()?;
        // The lookup behind `Pubky::get_homeserver_of`, which folds a missing
        // packet and a packet naming no homeserver into one `None`.
        let record = match pubky.client().pkarr().resolve(user_pk).await {
            Some(packet) => PkdnsRecord::Published {
                homeserver: published_homeserver(&packet),
                timestamp: packet.timestamp().as_u64(),
            },
            None => PkdnsRecord::Missing,
        };
        Ok(record)
    }
}

/// The homeserver a PKDNS record names: the target of its `_pubky` SVCB/HTTPS
/// record, if that is a public key. Mirrors `Pubky::get_homeserver_of`, whose
/// parsing the SDK does not export.
fn published_homeserver(packet: &SignedPacket) -> Option<PubkyId> {
    let target = packet
        .resource_records("_pubky")
        .find_map(|rr| match &rr.rdata {
            RData::SVCB(svcb) => Some(svcb.target.to_string()),
            RData::HTTPS(https) => Some(https.0.target.to_string()),
            _ => None,
        })?;
    PublicKey::try_from_z32(&target).ok().map(PubkyId::from)
}

pub struct UserHsResolverRunner {
    resolver: Box<dyn PkdnsHomeserverResolver>,
    ttl_ms: u64,
    /// The bulk-indexed homeserver. A switch to it flags the edge `catch_up`; a
    /// switch away from it stores the bulk cursor as the user's floor there.
    primary_homeserver: PubkyId,
    unbound_backoff: UnboundBackoff,
    shutdown_rx: Receiver<bool>,
}

impl UserHsResolverRunner {
    pub fn from_config(
        config: &WatcherConfig,
        resolver: Box<dyn PkdnsHomeserverResolver>,
        shutdown_rx: Receiver<bool>,
    ) -> Self {
        Self {
            resolver,
            ttl_ms: config.hs_resolver_ttl,
            primary_homeserver: config.homeserver.clone(),
            unbound_backoff: UnboundBackoff::default(),
            shutdown_rx,
        }
    }

    pub async fn run(&self) -> Result<(), DynError> {
        let mut shutdown_rx = self.shutdown_rx.clone();
        run(
            self.resolver.as_ref(),
            self.ttl_ms,
            &self.primary_homeserver,
            &self.unbound_backoff,
            &mut shutdown_rx,
        )
        .await
    }
}

/// Main entry point for one cycle of the periodic task.
///
/// `ttl_ms` controls the minimum time before a user's mapping is re-resolved.
/// Users whose `HOSTED_BY.resolved_at` is newer than `ttl_ms` are skipped, and
/// so are unbound users still waiting out their `unbound_backoff`.
///
/// `shutdown_rx` cancels the in-flight resolution on shutdown; cancelled users
/// get re-picked-up on the next run via TTL.
pub async fn run(
    resolver: &dyn PkdnsHomeserverResolver,
    ttl_ms: u64,
    primary_homeserver: &PubkyId,
    unbound_backoff: &UnboundBackoff,
    shutdown_rx: &mut Receiver<bool>,
) -> Result<(), DynError> {
    let user_ids = get_users_needing_resolution(ttl_ms).await?;
    let user_ids = unbound_backoff.retain_due(user_ids, Instant::now());
    let user_pks: Vec<PublicKey> = user_ids
        .iter()
        .filter_map(|user_id| {
            // For the user_ids that fail to convert, we log an error message and skip them
            user_id
                .parse::<PublicKey>()
                .map_err(|e| error!(%user_id, error = %e, "Failed to parse user_id"))
                .ok()
        })
        .collect();
    if user_pks.is_empty() {
        debug!("No users need homeserver resolution");
        HS_RESOLVER_METRICS.run_total.record(0, &[]);
        HS_RESOLVER_METRICS.run_failed.record(0, &[]);
        // Empty runs cannot change the mapping counts, but the gauges still
        // need a first value after startup, which may otherwise be up to a
        // TTL away if every mapping is still fresh. Skip it on shutdown, as
        // the non-empty path does.
        if !HS_RESOLVER_METRICS.gauges_populated() && !*shutdown_rx.borrow() {
            refresh_mapping_gauges().await;
        }
        HS_RESOLVER_METRICS.record_heartbeat();
        return Ok(());
    }

    let total = user_pks.len() as u64;
    debug!(user_count = total, "Resolving homeservers");

    let mut failed = 0u64;
    let mut processed = 0u64;
    let mut shutting_down = false;

    // As of pubky 0.7.0 parallel resolution is possible but unreliable. This was tried:
    // - with the singleton Pubky client (up to 10% unresolved nodes with 10 req. in parallel)
    // - with a relay-only Pubky client (up to 95% unresolved nodes with 10 req. in parallel)
    // "unresolved nodes" = no HS was found using `get_homeserver_of(&user_pk)` for users with a known HS.
    //
    // The most reliable method remains sequential querying.
    //
    // To minimize the chance that User PKs are too close to each other and therefore might hit
    // the same DHT node, which can cause that node to interpret this as spammy requests and therefore
    // fail / refuse to resolve some of the queries, we order the User PKs such that every new query lands
    // as far as possible from all previous queries in the PK keyspace.
    //
    // To achieve this, we use bisection ordering.
    for user_pk in bisection_order_user_pks(user_pks) {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                info!(processed, total, "Shutdown detected; HS resolver stopping");
                shutting_down = true;
                break;
            }
            result = resolve_user(resolver, primary_homeserver, &user_pk) => {
                let user_id = user_pk.z32();
                processed += 1;
                let (outcome, mapping) = match result {
                    // Graph read or write failed: a Neo4j problem, not a resolution one.
                    Err(e) => {
                        warn!(%user_id, error = %e, "Failed to read or update HS mapping");
                        (Outcome::Error, None)
                    }
                    Ok(resolution) => {
                        match resolution {
                            Resolution::Unbound => unbound_backoff.record_unbound(
                                &user_id,
                                Instant::now(),
                                Duration::from_millis(ttl_ms),
                            ),
                            Resolution::LookupFailed { .. } => {}
                            _ => unbound_backoff.clear(&user_id),
                        }
                        HS_RESOLVER_METRICS.record_resolution(resolution);
                        let outcome = resolution.outcome();
                        if outcome == Outcome::Unresolved {
                            warn!(%user_id, "PKDNS lookup found no HS");
                        }
                        (outcome, Some(resolution.mapping()))
                    }
                };
                // Anything short of a resolved HS is a failure for the run summary.
                if outcome != Outcome::Resolved {
                    failed += 1;
                }
                HS_RESOLVER_METRICS.resolutions.add(
                    1,
                    &[
                        KeyValue::new("outcome", outcome.as_str()),
                        mapping_attribute(mapping),
                    ],
                );
                HS_RESOLVER_METRICS.record_heartbeat();
            }
        }
    }

    HS_RESOLVER_METRICS.run_total.record(total, &[]);
    HS_RESOLVER_METRICS.run_failed.record(failed, &[]);
    if !shutting_down {
        // Don't hold up shutdown with a graph scan; the next run refreshes the gauges.
        refresh_mapping_gauges().await;
    }
    HS_RESOLVER_METRICS.record_heartbeat();

    Ok(())
}

/// Refreshes the population gauges from the graph.
///
/// A failed count is logged but does not fail the run: the metrics are an
/// observer of the resolver, not part of it.
async fn refresh_mapping_gauges() {
    match count_mappings().await {
        Ok(counts) => HS_RESOLVER_METRICS.record_mapping_counts(counts),
        Err(e) => warn!(error = %e, "Failed to count homeserver mappings"),
    }
}

// Bisection ordering sorts the User PKs such that every new PK is as far as possible from all
// previously queried PKs in the keyspace.
//
// The algorithm:
//   1. Sort all PKs lexicographically.
//   2. Reorder via BFS over the implicit binary-search-tree layout of the sorted array:
//      emit the midpoint of each interval, then recurse into left and right halves.
//
// For a sorted array [K0..=K7] this produces [K4, K2, K6, K1, K3, K5, K7, K0], ensuring
// each successive query lands as far as possible from all previous ones in the keyspace.
fn bisection_order_user_pks(unsorted_pks: Vec<PublicKey>) -> Vec<PublicKey> {
    let mut sorted_pks = unsorted_pks;
    sorted_pks.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let n = sorted_pks.len();
    let mut bisection_result = Vec::with_capacity(n);
    // Each entry is a half-open interval [lo, hi) of the sorted slice to process.
    let mut queue = std::collections::VecDeque::new();
    if n > 0 {
        queue.push_back((0usize, n));
    }
    while let Some((lo, hi)) = queue.pop_front() {
        if lo >= hi {
            continue;
        }
        let mid = lo + (hi - lo) / 2;
        bisection_result.push(sorted_pks[mid].clone());
        queue.push_back((lo, mid));
        queue.push_back((mid + 1, hi));
    }
    bisection_result
}

/// Fetches user IDs whose homeserver mapping is due for re-resolution or missing.
///
/// A mapping is due when its `resolved_at` timestamp is older than `ttl_ms`
/// milliseconds ago.
async fn get_users_needing_resolution(ttl_ms: u64) -> GraphResult<Vec<String>> {
    let query = queries::get::get_users_needing_hs_resolution(ttl_ms);
    let maybe_user_ids = fetch_key_from_graph(query, "user_ids").await?;
    Ok(maybe_user_ids.unwrap_or_default())
}

/// State of a user's stored `HOSTED_BY` mapping before a resolution.
///
/// Exported as the `mapping` attribute of the `resolutions` counter. Unbound
/// users with no published record are re-resolved on every tick, so only
/// resolutions of previously `active` mappings carry an outage signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MappingState {
    /// No `HOSTED_BY` edge yet.
    Unbound,
    /// Bound to a homeserver.
    Active,
}

impl MappingState {
    fn of(stored: &StoredMapping) -> Self {
        match stored.hs_id {
            None => Self::Unbound,
            Some(_) => Self::Active,
        }
    }
}

/// `mapping` attribute of the `resolutions` counter. `None` is exported as
/// `unknown`: the graph read or write failed before the state was known.
fn mapping_attribute(mapping: Option<MappingState>) -> KeyValue {
    let mapping = match mapping {
        Some(MappingState::Unbound) => "unbound",
        Some(MappingState::Active) => "active",
        None => "unknown",
    };
    KeyValue::new("mapping", mapping)
}

/// How a resolution ended, as exported in the `outcome` attribute of the
/// `resolutions` counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// PKDNS returned a homeserver.
    Resolved,
    /// PKDNS returned none: no record, or a record naming no homeserver. On
    /// pubky 0.9.3 DHT and relay failures surface as a missing record.
    Unresolved,
    /// The lookup failed (pubky 0.10+ surfaces transport errors) or the graph read/write failed.
    Error,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::Unresolved => "unresolved",
            Self::Error => "error",
        }
    }
}

/// What resolving one user did to its `HOSTED_BY` mapping.
///
/// One variant per reachable case, so the metric labels below are exhaustive
/// matches and a new case forces a labelling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolution {
    /// No edge, and no record or a record naming no homeserver; graph untouched.
    Unbound,
    /// No edge before; bound to the homeserver the record names.
    Bound,
    /// The record names the stored homeserver; `resolved_at` refreshed.
    Confirmed,
    /// A record newer than the clock names another homeserver; edge moved there.
    /// `catch_up` is set when the target is the primary homeserver.
    Switched { catch_up: bool },
    /// A record not newer than the clock names another homeserver; edge kept.
    IgnoredOlder,
    /// The record names no homeserver; edge kept, clock advanced.
    NoHomeserver,
    /// No record found; edge kept.
    Kept,
    /// The lookup itself failed (pubky 0.10+ surfaces transport errors);
    /// graph untouched.
    LookupFailed { mapping: MappingState },
}

impl Resolution {
    /// `outcome` label: whether PKDNS returned a homeserver.
    fn outcome(self) -> Outcome {
        match self {
            Self::Bound | Self::Confirmed | Self::Switched { .. } | Self::IgnoredOlder => {
                Outcome::Resolved
            }
            Self::Unbound | Self::NoHomeserver | Self::Kept => Outcome::Unresolved,
            Self::LookupFailed { .. } => Outcome::Error,
        }
    }

    /// `mapping` label: the stored mapping's state before this resolution.
    fn mapping(self) -> MappingState {
        match self {
            Self::Unbound | Self::Bound => MappingState::Unbound,
            Self::Confirmed
            | Self::Switched { .. }
            | Self::IgnoredOlder
            | Self::NoHomeserver
            | Self::Kept => MappingState::Active,
            Self::LookupFailed { mapping } => mapping,
        }
    }
}

/// A user's stored mapping, as returned by [`get_user_homeserver`].
#[derive(Debug, Default, PartialEq, Eq)]
struct StoredMapping {
    /// The homeserver the user is bound to, if any.
    hs_id: Option<String>,
    /// Whether the binding is flagged for the per-user catch-up.
    catch_up: bool,
    /// The record clock: timestamp of the newest PKDNS record applied, if any.
    record_ts: Option<i64>,
}

impl StoredMapping {
    /// Whether a record with this timestamp is newer than the clock. A user
    /// without a clock accepts any record.
    fn record_is_newer(&self, timestamp: u64) -> bool {
        self.record_ts
            .is_none_or(|clock| record_ts_i64(timestamp) > clock)
    }
}

/// The packet timestamp as stored in the graph.
fn record_ts_i64(timestamp: u64) -> i64 {
    i64::try_from(timestamp).unwrap_or(i64::MAX)
}

/// Resolves a single user's HS and updates the HOSTED_BY relationship to match.
///
/// The relationship only changes on a record newer than the user's clock that
/// names another homeserver. A record naming the stored homeserver, a record
/// naming none and a missing record keep it. A record not newer than the clock
/// is ignored, whatever it names.
///
/// A failed lookup is reported as a [`Resolution`] and leaves the graph
/// untouched; only graph errors are returned as `Err`.
async fn resolve_user(
    resolver: &dyn PkdnsHomeserverResolver,
    primary_homeserver: &PubkyId,
    user_pk: &PublicKey,
) -> Result<Resolution, DynError> {
    let user_id = user_pk.z32();

    // Read the stored mapping first so a failed lookup can still be attributed
    // to the mapping state it would have affected.
    let stored = get_user_homeserver(&user_id).await?;

    let record = match resolver.resolve_homeserver(user_pk).await {
        Ok(record) => record,
        Err(e) => {
            warn!(%user_id, error = %e, "PKDNS lookup failed");
            return Ok(Resolution::LookupFailed {
                mapping: MappingState::of(&stored),
            });
        }
    };

    let resolution = match (stored.hs_id.as_deref(), record) {
        // Nothing published. The caller logs it and applies the unbound backoff.
        (
            None,
            PkdnsRecord::Missing
            | PkdnsRecord::Published {
                homeserver: None, ..
            },
        ) => Resolution::Unbound,

        (
            None,
            PkdnsRecord::Published {
                homeserver: Some(resolved_hs_id),
                timestamp,
            },
        ) => {
            set_user_homeserver_from_record(
                &user_id,
                &resolved_hs_id,
                record_ts_i64(timestamp),
                false,
            )
            .await?;
            debug!(%user_id, homeserver = %resolved_hs_id, "HS mapping created");
            Resolution::Bound
        }

        (
            Some(stored_hs_id),
            PkdnsRecord::Published {
                homeserver: Some(resolved_hs_id),
                timestamp,
            },
        ) if resolved_hs_id.as_ref() == stored_hs_id => {
            refresh_user_homeserver(&user_id, Some(record_ts_i64(timestamp))).await?;
            debug!(%user_id, homeserver = %stored_hs_id, "HS mapping confirmed");
            Resolution::Confirmed
        }

        (
            Some(stored_hs_id),
            PkdnsRecord::Published {
                homeserver: Some(resolved_hs_id),
                timestamp,
            },
        ) => {
            if !stored.record_is_newer(timestamp) {
                // An old record, replayed or served by a lagging node: the user
                // already published something newer, so it moves nothing.
                refresh_user_homeserver(&user_id, None).await?;
                warn!(
                    %user_id,
                    stored_homeserver = %stored_hs_id,
                    homeserver = %resolved_hs_id,
                    record_ts = timestamp,
                    clock = ?stored.record_ts,
                    "PKDNS record names another homeserver but is not newer than the applied one; ignored"
                );
                return Ok(Resolution::IgnoredOlder);
            }

            let catch_up = resolved_hs_id.as_ref() == primary_homeserver.as_ref();
            // Leaving the primary with the bulk stream covering the user: its cursor
            // is the floor for a later return. Not while a catch-up is still on, when
            // the bulk stream skipped the user and the per-user cursor is the truth.
            if stored_hs_id == primary_homeserver.as_ref() && !stored.catch_up {
                store_primary_cursor_floor(&user_id, primary_homeserver).await;
            }
            set_user_homeserver_from_record(
                &user_id,
                &resolved_hs_id,
                record_ts_i64(timestamp),
                catch_up,
            )
            .await?;
            info!(
                %user_id,
                stored_homeserver = %stored_hs_id,
                homeserver = %resolved_hs_id,
                catch_up,
                "User published another homeserver; HS mapping switched"
            );
            Resolution::Switched { catch_up }
        }

        // The record carries a time but no homeserver: it says nothing about
        // where the data is, so the binding stands. The clock still advances,
        // so an older record naming another homeserver cannot follow it.
        (
            Some(stored_hs_id),
            PkdnsRecord::Published {
                homeserver: None,
                timestamp,
            },
        ) => {
            refresh_user_homeserver(&user_id, Some(record_ts_i64(timestamp))).await?;
            debug!(%user_id, homeserver = %stored_hs_id, "PKDNS record names no homeserver; HS mapping kept");
            Resolution::NoHomeserver
        }

        // A missing record says nothing about the HS, so the stored one stands.
        // `resolved_at` is still refreshed, deferring the next lookup by a TTL.
        (Some(stored_hs_id), PkdnsRecord::Missing) => {
            refresh_user_homeserver(&user_id, None).await?;
            debug!(%user_id, homeserver = %stored_hs_id, "No PKDNS record found; HS mapping kept");
            Resolution::Kept
        }
    };

    Ok(resolution)
}

/// Stores the primary homeserver's bulk cursor as the user's per-user cursor
/// there, when the user's edge leaves the primary.
///
/// Everything up to the bulk cursor was processed by the bulk stream while the
/// user was bound there, so a later return to the primary catches up only the
/// gap instead of replaying the user's whole history. Best effort: a missing or
/// failing cursor costs a longer replay, never lost events, since the per-user
/// cursor is only ever moved forward.
async fn store_primary_cursor_floor(user_id: &str, primary_homeserver: &PubkyId) {
    let cursor = match Homeserver::get_from_index(primary_homeserver.as_ref()).await {
        Ok(Some(homeserver)) => homeserver.cursor,
        Ok(None) => {
            debug!(%user_id, "No bulk cursor stored for the primary homeserver; no floor set");
            return;
        }
        Err(e) => {
            warn!(%user_id, error = %e, "Failed to read the primary homeserver's cursor; no floor set");
            return;
        }
    };
    match UserHsCursor::write(user_id, primary_homeserver.as_ref(), cursor).await {
        Ok(()) => {
            debug!(%user_id, cursor, "Stored the primary's bulk cursor as the user's floor there")
        }
        Err(RedisError::InvalidInput(_)) => {
            debug!(%user_id, cursor, "User's cursor on the primary is already past the bulk cursor")
        }
        Err(e) => {
            warn!(%user_id, error = %e, "Failed to store the user's cursor floor on the primary")
        }
    }
}

/// Returns the user's stored mapping. A user with no `HOSTED_BY` edge, or no
/// node at all, reads as unbound.
async fn get_user_homeserver(user_id: &str) -> GraphResult<StoredMapping> {
    let query = queries::get::get_user_homeserver(user_id);
    let Some(row) = fetch_row_from_graph(query).await? else {
        return Ok(StoredMapping::default());
    };
    Ok(StoredMapping {
        hs_id: row.get("homeserver_id")?,
        catch_up: row.get("catch_up")?,
        record_ts: row.get("record_ts")?,
    })
}

/// Population counts over non-deleted users with a `HOSTED_BY` mapping.
#[derive(Debug, Default)]
struct MappingCounts {
    /// Users bound to a homeserver.
    mapped: u64,
    /// Subset of `mapped` whose binding is flagged `catch_up`.
    catch_up: u64,
}

/// Counts users with a `HOSTED_BY` mapping and how many of them are catching up.
async fn count_mappings() -> GraphResult<MappingCounts> {
    let query = queries::get::count_user_homeserver_mappings();
    let Some(row) = fetch_row_from_graph(query).await? else {
        return Ok(MappingCounts::default());
    };
    Ok(MappingCounts {
        mapped: row.get("mapped_users")?,
        catch_up: row.get("catch_up_users")?,
    })
}

/// Returns all user IDs hosted on a given homeserver.
pub async fn get_user_ids_by_homeserver(hs_id: &str) -> GraphResult<Vec<String>> {
    let query = queries::get::get_active_users_by_homeserver(hs_id);
    let maybe_user_ids = fetch_key_from_graph(query, "user_ids").await?;
    Ok(maybe_user_ids.unwrap_or_default())
}

/// Returns the IDs of the users hosted on a given homeserver whose binding is
/// flagged `catch_up`.
pub async fn get_catch_up_user_ids_by_homeserver(hs_id: &str) -> GraphResult<Vec<String>> {
    let query = queries::get::get_catch_up_users_by_homeserver(hs_id);
    let maybe_user_ids = fetch_key_from_graph(query, "user_ids").await?;
    Ok(maybe_user_ids.unwrap_or_default())
}

struct HsResolverMetrics {
    run_total: Histogram<u64>,
    run_failed: Histogram<u64>,
    /// PKDNS resolutions, labelled by `outcome` in {resolved, unresolved, error}
    /// and `mapping` in {unbound, active, unknown}. Incremented per user
    /// rather than per run so a failing share is visible while a long run is
    /// still in progress. `mapping="unknown"` means a graph read or write failed;
    /// those are Neo4j incidents, visible via `neo4j.query.errors`, and are kept
    /// out of the `mapping="active"` onset ratio on purpose.
    resolutions: Counter<u64>,
    /// Users whose edge moved to another homeserver, labelled by `target` in
    /// {primary, external}.
    switched: Counter<u64>,
    /// Records naming another homeserver that were not newer than the user's
    /// clock: replays or lagging nodes, never applied.
    ignored_older_record: Counter<u64>,
    /// Records naming no homeserver for a bound user; the binding was kept.
    no_homeserver_record: Counter<u64>,
    /// Non-deleted users with a `HOSTED_BY` mapping.
    mapped_users: Gauge<u64>,
    /// Subset of `mapped_users` flagged `catch_up`: their history on the primary
    /// is still being read per user. Refreshed after every run that processed
    /// users.
    catch_up_users: Gauge<u64>,
    /// Whether the population gauges have been recorded since startup.
    gauges_populated: AtomicBool,
    /// Unix time of the last user handled or run finished. Stamped per user so
    /// a long run keeps reporting while a single hung lookup does not; gauges
    /// otherwise keep exporting their last value for as long as the process lives.
    heartbeat: Gauge<u64>,
}

impl HsResolverMetrics {
    fn new() -> Self {
        let meter = global::meter("hs-resolver-meter");

        Self {
            run_total: meter
                .u64_histogram("nexus.task.hs-resolver.total")
                .with_description("Number of attempted HS resolutions in each resolver run")
                .build(),
            run_failed: meter
                .u64_histogram("nexus.task.hs-resolver.failed")
                .with_description("Number of failed HS resolutions in each resolver run")
                .build(),
            resolutions: meter
                .u64_counter("nexus.task.hs-resolver.resolutions")
                .with_description("PKDNS homeserver resolutions, by outcome")
                .build(),
            switched: meter
                .u64_counter("nexus.task.hs-resolver.switched")
                .with_description("Users whose homeserver mapping moved to another homeserver, by target")
                .build(),
            ignored_older_record: meter
                .u64_counter("nexus.task.hs-resolver.ignored_older_record")
                .with_description("PKDNS records naming another homeserver that were not newer than the applied one")
                .build(),
            no_homeserver_record: meter
                .u64_counter("nexus.task.hs-resolver.no_homeserver_record")
                .with_description("PKDNS records naming no homeserver for a bound user")
                .build(),
            mapped_users: meter
                .u64_gauge("nexus.task.hs-resolver.mapped_users")
                .with_description("Users with a homeserver mapping")
                .build(),
            catch_up_users: meter
                .u64_gauge("nexus.task.hs-resolver.catch_up_users")
                .with_description("Users whose history on the primary homeserver is still being read per user")
                .build(),
            heartbeat: meter
                .u64_gauge("nexus.task.hs-resolver.heartbeat_timestamp")
                .with_description("Unix time of the resolver's most recent progress")
                .with_unit("s")
                .build(),
            gauges_populated: AtomicBool::new(false),
        }
    }

    /// Counts the resolutions that changed or deliberately kept a mapping.
    fn record_resolution(&self, resolution: Resolution) {
        match resolution {
            Resolution::Switched { catch_up } => {
                let target = if catch_up { "primary" } else { "external" };
                self.switched.add(1, &[KeyValue::new("target", target)]);
            }
            Resolution::IgnoredOlder => self.ignored_older_record.add(1, &[]),
            Resolution::NoHomeserver => self.no_homeserver_record.add(1, &[]),
            Resolution::Unbound
            | Resolution::Bound
            | Resolution::Confirmed
            | Resolution::Kept
            | Resolution::LookupFailed { .. } => {}
        }
    }

    fn record_mapping_counts(&self, counts: MappingCounts) {
        self.mapped_users.record(counts.mapped, &[]);
        self.catch_up_users.record(counts.catch_up, &[]);
        self.gauges_populated.store(true, Ordering::Relaxed);
    }

    fn gauges_populated(&self) -> bool {
        self.gauges_populated.load(Ordering::Relaxed)
    }

    fn record_heartbeat(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        self.heartbeat.record(now, &[]);
    }
}

// TODO Move tests to separate module? (switch to WatcherTest::setup())
#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::db::graph::Query;
    use nexus_common::db::{exec_single_row, PubkyClientError};
    use nexus_common::models::user::{set_user_homeserver, set_user_homeserver_catch_up};
    use nexus_common::types::DynError;
    use nexus_common::utils::test_utils::{random_pk, random_pubky_id};
    use nexus_common::{StackConfig, StackManager};
    use pubky::pkarr::dns::{rdata::SVCB, Name};
    use pubky::pkarr::Keypair;

    async fn setup() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await
    }

    /// Resolver stub returning a fixed PKDNS record, so `resolve_user` can be
    /// driven without touching the DHT.
    struct MockResolver {
        record: PkdnsRecord,
    }

    impl MockResolver {
        /// A record naming `homeserver`, authored at `timestamp`.
        fn naming(homeserver: &PubkyId, timestamp: u64) -> Self {
            Self {
                record: PkdnsRecord::Published {
                    homeserver: Some(homeserver.clone()),
                    timestamp,
                },
            }
        }

        /// A record with no homeserver in it, authored at `timestamp`.
        fn naming_none(timestamp: u64) -> Self {
            Self {
                record: PkdnsRecord::Published {
                    homeserver: None,
                    timestamp,
                },
            }
        }

        /// No record found.
        fn missing() -> Self {
            Self {
                record: PkdnsRecord::Missing,
            }
        }
    }

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for MockResolver {
        async fn resolve_homeserver(&self, _user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            Ok(self.record.clone())
        }
    }

    /// Resolver stub whose lookups always fail, as transport errors do on pubky 0.10+.
    struct FailingResolver;

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for FailingResolver {
        async fn resolve_homeserver(&self, _user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            Err(PubkyClientError::RequestFailed {
                message: "dht unreachable".into(),
            })
        }
    }

    /// Helper: create a User node in the graph
    async fn create_test_user(user_id: &str) -> GraphResult<()> {
        let query = Query::new(
            "create_test_user",
            "MERGE (u:User {id: $id})
             SET u.name = 'test', u.indexed_at = 0
             RETURN u;",
        )
        .param("id", user_id);
        exec_single_row(query).await
    }

    /// Helper: clean up test data
    async fn cleanup_test_user(user_id: &str) -> GraphResult<()> {
        let query = queries::del::delete_user(user_id);
        exec_single_row(query).await
    }

    /// Helper: push a user's `resolved_at` two hours into the past, so the user
    /// is due for resolution under the 1-hour TTL the tests use.
    async fn backdate_resolved_at(user_id: &str) -> GraphResult<()> {
        let query = Query::new(
            "backdate_hs_mapping",
            "MATCH (u:User {id: $user_id})-[r:HOSTED_BY]->(:Homeserver)
             SET r.resolved_at = timestamp() - 7200000",
        )
        .param("user_id", user_id.to_string());
        exec_single_row(query).await
    }

    /// Helper: the stored mapping of a bound user, with the given clock, as the
    /// test's starting point.
    fn mapping(hs_id: &PubkyId, catch_up: bool, record_ts: Option<i64>) -> StoredMapping {
        StoredMapping {
            hs_id: Some(hs_id.to_string()),
            catch_up,
            record_ts,
        }
    }

    const TTL_MS: u64 = 3_600_000;

    #[tokio_shared_rt::test(shared)]
    async fn test_set_user_homeserver_graph_query() -> Result<(), DynError> {
        setup().await?;

        let user_id = "hs_resolver_test_user_001";
        let hs_id_a = "hs_resolver_test_hs_aaa";
        let hs_id_b = "hs_resolver_test_hs_bbb";

        create_test_user(user_id).await?;

        // Set initial homeserver
        let query = queries::put::set_user_homeserver(user_id, hs_id_a, Some(1), false);
        exec_single_row(query).await?;

        // Switch to a different homeserver
        let query = queries::put::set_user_homeserver(user_id, hs_id_b, Some(2), false);
        exec_single_row(query).await?;

        // Cleanup
        cleanup_test_user(user_id).await?;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_set_user_homeserver_idempotent() -> Result<(), DynError> {
        setup().await?;

        let user_id = "hs_resolver_test_user_noop";
        let hs_id = "hs_resolver_test_hs_noop";

        create_test_user(user_id).await?;

        // Set homeserver for the first time
        let query = queries::put::set_user_homeserver(user_id, hs_id, None, false);
        exec_single_row(query).await?;

        // Set same homeserver again (should reuse HS, e.g. not create any orphan HS)
        let query = queries::put::set_user_homeserver(user_id, hs_id, None, false);
        exec_single_row(query).await?;

        // Cleanup
        cleanup_test_user(user_id).await?;

        Ok(())
    }

    /// The record clock only moves forward, and a write without a record leaves it.
    #[tokio_shared_rt::test(shared)]
    async fn test_record_clock_only_advances() -> Result<(), DynError> {
        setup().await?;

        let user_id = random_pk().z32();
        let hs_id = random_pubky_id();
        create_test_user(&user_id).await?;

        set_user_homeserver_from_record(&user_id, &hs_id, 100, false).await?;
        assert_eq!(get_user_homeserver(&user_id).await?.record_ts, Some(100));

        set_user_homeserver_from_record(&user_id, &hs_id, 50, false).await?;
        assert_eq!(
            get_user_homeserver(&user_id).await?.record_ts,
            Some(100),
            "an older record must not rewind the clock"
        );

        refresh_user_homeserver(&user_id, Some(200)).await?;
        assert_eq!(get_user_homeserver(&user_id).await?.record_ts, Some(200));

        refresh_user_homeserver(&user_id, None).await?;
        set_user_homeserver(&user_id, &hs_id).await?;
        assert_eq!(
            get_user_homeserver(&user_id).await?.record_ts,
            Some(200),
            "writes without a record leave the clock alone"
        );

        cleanup_test_user(&user_id).await?;
        Ok(())
    }

    /// The catch-up flag is set by a switch that asks for it, survives a plain
    /// refresh of the binding, and is cleared explicitly.
    #[tokio_shared_rt::test(shared)]
    async fn test_catch_up_flag_lifecycle() -> Result<(), DynError> {
        setup().await?;

        let user_id = random_pk().z32();
        let hs_a = random_pubky_id();
        let hs_b = random_pubky_id();
        create_test_user(&user_id).await?;

        set_user_homeserver_from_record(&user_id, &hs_a, 1, false).await?;
        assert!(!get_user_homeserver(&user_id).await?.catch_up);

        set_user_homeserver_from_record(&user_id, &hs_b, 2, true).await?;
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&hs_b, true, Some(2))
        );
        assert_eq!(
            get_catch_up_user_ids_by_homeserver(&hs_b).await?,
            vec![user_id.clone()]
        );
        assert!(get_catch_up_user_ids_by_homeserver(&hs_a).await?.is_empty());

        // Confirming the binding does not end the catch-up.
        set_user_homeserver_from_record(&user_id, &hs_b, 3, false).await?;
        set_user_homeserver(&user_id, &hs_b).await?;
        assert!(get_user_homeserver(&user_id).await?.catch_up);

        set_user_homeserver_catch_up(&user_id, &hs_b, false).await?;
        assert!(!get_user_homeserver(&user_id).await?.catch_up);
        assert!(get_catch_up_user_ids_by_homeserver(&hs_b).await?.is_empty());

        cleanup_test_user(&user_id).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_get_users_needing_resolution_ttl() -> Result<(), DynError> {
        setup().await?;

        let user_fresh = "ttl_test_user_fresh";
        let user_stale = "ttl_test_user_stale";
        let user_no_hs = "ttl_test_user_no_hs";
        let hs_id = "ttl_test_hs";

        create_test_user(user_fresh).await?;
        create_test_user(user_stale).await?;
        create_test_user(user_no_hs).await?;

        // Give user_fresh a recently resolved mapping
        set_user_homeserver(user_fresh, hs_id).await?;

        // Give user_stale a mapping with an old resolved_at (2 hours ago)
        set_user_homeserver(user_stale, hs_id).await?;
        backdate_resolved_at(user_stale).await?;

        // user_no_hs has no HOSTED_BY at all

        // With a 1-hour TTL: user_fresh should be skipped, user_stale and user_no_hs returned
        let mut needing = get_users_needing_resolution(TTL_MS).await?;
        needing.sort();

        assert!(
            !needing.contains(&user_fresh.to_string()),
            "Recently resolved user should be skipped"
        );
        assert!(
            needing.contains(&user_stale.to_string()),
            "User resolved long ago should need resolution"
        );
        assert!(
            needing.contains(&user_no_hs.to_string()),
            "User without HOSTED_BY should need resolution"
        );

        // Cleanup
        cleanup_test_user(user_fresh).await?;
        cleanup_test_user(user_stale).await?;
        cleanup_test_user(user_no_hs).await?;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_get_user_ids_by_homeserver() -> Result<(), DynError> {
        setup().await?;

        let user_a = "hs_users_test_user_aaa";
        let user_b = "hs_users_test_user_bbb";
        let user_c = "hs_users_test_user_ccc";
        let hs_one = "hs_users_test_hs_one";
        let hs_two = "hs_users_test_hs_two";

        create_test_user(user_a).await?;
        create_test_user(user_b).await?;
        create_test_user(user_c).await?;

        // Host user_a and user_b on hs_one, user_c on hs_two
        set_user_homeserver(user_a, hs_one).await?;
        set_user_homeserver(user_b, hs_one).await?;
        set_user_homeserver(user_c, hs_two).await?;

        // Query users on hs_one
        let mut users = get_user_ids_by_homeserver(hs_one).await?;
        users.sort();
        assert_eq!(users, vec![user_a, user_b]);

        // Query users on hs_two
        let users = get_user_ids_by_homeserver(hs_two).await?;
        assert_eq!(users, vec![user_c]);

        // Query unknown HS returns empty
        let users = get_user_ids_by_homeserver("nonexistent_hs").await?;
        assert!(users.is_empty());

        // Cleanup
        cleanup_test_user(user_a).await?;
        cleanup_test_user(user_b).await?;
        cleanup_test_user(user_c).await?;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_get_user_homeserver() -> Result<(), DynError> {
        setup().await?;

        let user_id = random_pk().z32();
        let hs_id = random_pubky_id();

        // No node at all: unbound, no clock.
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            StoredMapping::default()
        );

        create_test_user(&user_id).await?;

        // No HOSTED_BY edge yet
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            StoredMapping::default()
        );

        // Bound without a record: no clock, no catch-up
        set_user_homeserver(&user_id, &hs_id).await?;
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&hs_id, false, None)
        );

        // Bound from a record: the clock holds its timestamp
        set_user_homeserver_from_record(&user_id, &hs_id, 42, false).await?;
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&hs_id, false, Some(42))
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// First-time resolution binds to whatever the record names and starts the clock.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_first_time_sets_homeserver() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let hs_id = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;

        let outcome = resolve_user(&MockResolver::naming(&hs_id, 10), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Bound);

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&hs_id, false, Some(10))
        );
        assert!(get_user_ids_by_homeserver(&hs_id).await?.contains(&user_id));

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A first bind to the primary needs no catch-up: the bulk stream processes
    /// unbound users, so nothing of this user's was skipped.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_first_time_on_primary_does_not_catch_up() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;

        let outcome = resolve_user(&MockResolver::naming(&primary, 10), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Bound);
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&primary, false, Some(10))
        );

        cleanup_test_user(&user_id).await?;
        Ok(())
    }

    /// A user with no stored mapping and nothing published is left alone, whether
    /// no record exists or the record names no homeserver.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_first_time_no_homeserver_noop() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;

        for resolver in [MockResolver::missing(), MockResolver::naming_none(10)] {
            let outcome = resolve_user(&resolver, &primary, &user_pk).await?;
            assert_eq!(outcome, Resolution::Unbound);
            assert_eq!(
                get_user_homeserver(&user_id).await?,
                StoredMapping::default(),
                "nothing is written for an unbound user without a homeserver"
            );
        }
        assert!(
            get_users_needing_resolution(TTL_MS)
                .await?
                .contains(&user_id),
            "users with no HS PKDNS mapping found should be retried on every resolver run"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A user with no published homeserver is not looked up again on the next
    /// tick, while a user with a published one is unaffected.
    #[tokio_shared_rt::test(shared)]
    async fn test_run_backs_off_unbound_user() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        create_test_user(&user_id).await?;

        let resolver = CountingResolver {
            target: user_id.clone(),
            calls: Mutex::new(HashMap::new()),
        };
        let backoff = UnboundBackoff::default();
        let primary = random_pubky_id();
        let (_tx, mut rx) = tokio::sync::watch::channel(false);

        run(&resolver, TTL_MS, &primary, &backoff, &mut rx).await?;
        run(&resolver, TTL_MS, &primary, &backoff, &mut rx).await?;

        assert_eq!(
            resolver.calls_for(&user_pk),
            1,
            "an unbound user must wait out its backoff, not be looked up every tick"
        );

        cleanup_test_user(&user_id).await?;
        Ok(())
    }

    /// Resolver stub that publishes nothing for `target` and counts lookups per
    /// user. Every other user in the shared test graph gets a lookup error,
    /// which leaves its mapping untouched.
    struct CountingResolver {
        target: String,
        calls: Mutex<HashMap<String, usize>>,
    }

    impl CountingResolver {
        fn calls_for(&self, user_pk: &PublicKey) -> usize {
            *self.calls.lock().unwrap().get(&user_pk.z32()).unwrap_or(&0)
        }
    }

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for CountingResolver {
        async fn resolve_homeserver(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            let user_id = user_pk.z32();
            *self
                .calls
                .lock()
                .unwrap()
                .entry(user_id.clone())
                .or_default() += 1;
            if user_id == self.target {
                Ok(PkdnsRecord::Missing)
            } else {
                Err(PubkyClientError::RequestFailed {
                    message: "not the user under test".into(),
                })
            }
        }
    }

    #[test]
    fn test_unbound_backoff_doubles_and_caps_at_ttl() {
        let backoff = UnboundBackoff::default();
        let ttl = Duration::from_secs(300);
        let t0 = Instant::now();
        let ids = || vec!["u".to_string()];

        assert_eq!(backoff.retain_due(ids(), t0), ids(), "never seen: due");

        backoff.record_unbound("u", t0, ttl);
        assert!(backoff
            .retain_due(ids(), t0 + Duration::from_secs(59))
            .is_empty());
        let t1 = t0 + Duration::from_secs(60);
        assert_eq!(backoff.retain_due(ids(), t1), ids(), "due after 60 s");

        backoff.record_unbound("u", t1, ttl);
        assert!(backoff
            .retain_due(ids(), t1 + Duration::from_secs(119))
            .is_empty());
        let t2 = t1 + Duration::from_secs(120);
        assert_eq!(backoff.retain_due(ids(), t2), ids(), "doubled to 120 s");

        backoff.record_unbound("u", t2, ttl);
        let t3 = t2 + Duration::from_secs(240);
        backoff.record_unbound("u", t3, ttl);
        assert!(backoff
            .retain_due(ids(), t3 + Duration::from_secs(299))
            .is_empty());
        assert_eq!(
            backoff.retain_due(ids(), t3 + ttl),
            ids(),
            "capped at the TTL, not 480 s"
        );

        backoff.clear("u");
        backoff.record_unbound("u", t3, ttl);
        assert_eq!(
            backoff.retain_due(ids(), t3 + Duration::from_secs(60)),
            ids(),
            "a resolved lookup resets the wait"
        );
    }

    #[test]
    fn test_unbound_backoff_forgets_users_no_longer_unbound() {
        let backoff = UnboundBackoff::default();
        let t0 = Instant::now();
        backoff.record_unbound("u", t0, Duration::from_secs(300));

        // "u" got bound (or deleted) elsewhere and is no longer selected.
        assert!(backoff.retain_due(vec![], t0).is_empty());
        assert!(backoff.entries.lock().unwrap().is_empty());
    }

    /// A record naming the stored homeserver confirms the binding, refreshes
    /// `resolved_at` and starts or advances the clock.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_confirmed_refreshes_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        // Bound by the ingestor, without a record, long ago.
        set_user_homeserver(&user_id, &stored_hs).await?;
        backdate_resolved_at(&user_id).await?;

        let outcome =
            resolve_user(&MockResolver::naming(&stored_hs, 10), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Confirmed);

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&stored_hs, false, Some(10))
        );
        assert!(
            !get_users_needing_resolution(TTL_MS)
                .await?
                .contains(&user_id),
            "a confirmation refreshes resolved_at"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A record newer than the clock naming another homeserver moves the edge:
    /// the user is read from the new homeserver and no longer from the old one.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_switches_on_newer_record() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let new_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 10, false).await?;

        let outcome = resolve_user(&MockResolver::naming(&new_hs, 20), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: false });

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&new_hs, false, Some(20))
        );
        assert!(!get_user_ids_by_homeserver(&stored_hs)
            .await?
            .contains(&user_id));
        assert!(get_user_ids_by_homeserver(&new_hs)
            .await?
            .contains(&user_id));

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A record naming another homeserver that is not newer than the clock is a
    /// replay or a lagging answer: the binding stands, only `resolved_at` moves.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_ignores_record_not_newer_than_clock() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let old_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 20, false).await?;

        for timestamp in [20, 5] {
            backdate_resolved_at(&user_id).await?;
            let outcome = resolve_user(
                &MockResolver::naming(&old_hs, timestamp),
                &primary,
                &user_pk,
            )
            .await?;
            assert_eq!(outcome, Resolution::IgnoredOlder, "record at {timestamp}");
            assert_eq!(
                get_user_homeserver(&user_id).await?,
                mapping(&stored_hs, false, Some(20)),
                "the binding and the clock stand"
            );
            assert!(
                !get_users_needing_resolution(TTL_MS)
                    .await?
                    .contains(&user_id),
                "an ignored record still refreshes resolved_at"
            );
        }
        assert!(!get_user_ids_by_homeserver(&old_hs)
            .await?
            .contains(&user_id));

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A user without a clock, bound by the ingestor, accepts the first record.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_without_clock_accepts_any_record() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let new_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;

        let outcome = resolve_user(&MockResolver::naming(&new_hs, 1), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: false });
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&new_hs, false, Some(1))
        );

        cleanup_test_user(&user_id).await?;
        Ok(())
    }

    /// A switch to the primary homeserver flags the edge for the per-user
    /// catch-up: the bulk stream has already passed this user's events there.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_switch_to_primary_flags_catch_up() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 10, false).await?;

        let outcome = resolve_user(&MockResolver::naming(&primary, 20), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: true });

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&primary, true, Some(20))
        );
        assert_eq!(
            get_catch_up_user_ids_by_homeserver(&primary).await?,
            vec![user_id.clone()]
        );

        // Confirming the binding later keeps the catch-up on.
        let outcome = resolve_user(&MockResolver::naming(&primary, 30), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Confirmed);
        assert!(get_user_homeserver(&user_id).await?.catch_up);

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// Leaving the primary with the bulk stream covering the user stores the bulk
    /// cursor as the user's per-user floor there, so a return replays only the gap.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_leaving_primary_stores_cursor_floor() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let primary = random_pubky_id();
        let new_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &primary, 10, false).await?;
        Homeserver {
            id: primary.clone(),
            cursor: 42,
        }
        .put_to_index()
        .await?;

        let outcome = resolve_user(&MockResolver::naming(&new_hs, 20), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: false });

        assert_eq!(
            UserHsCursor::read(&[user_id.as_str()], primary.as_ref()).await?,
            vec![42],
            "the bulk cursor becomes the user's floor on the primary"
        );
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&new_hs, false, Some(20))
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// Leaving the primary while still catching up keeps the per-user cursor: the
    /// bulk stream skipped this user, so its cursor says nothing about them.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_leaving_primary_mid_catch_up_keeps_user_cursor(
    ) -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let primary = random_pubky_id();
        let new_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &primary, 10, true).await?;
        UserHsCursor::write(&user_id, primary.as_ref(), 7).await?;
        Homeserver {
            id: primary.clone(),
            cursor: 42,
        }
        .put_to_index()
        .await?;

        let outcome = resolve_user(&MockResolver::naming(&new_hs, 20), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: false });

        assert_eq!(
            UserHsCursor::read(&[user_id.as_str()], primary.as_ref()).await?,
            vec![7],
            "the catch-up's own cursor is the truth while the flag is on"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A record naming no homeserver keeps the binding but advances the clock, so
    /// an older record naming another homeserver cannot follow it while a newer
    /// one still can.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_no_homeserver_record_keeps_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let other_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 10, false).await?;
        backdate_resolved_at(&user_id).await?;

        let outcome = resolve_user(&MockResolver::naming_none(30), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::NoHomeserver);
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&stored_hs, false, Some(30))
        );
        assert!(get_user_ids_by_homeserver(&stored_hs)
            .await?
            .contains(&user_id));
        assert!(
            !get_users_needing_resolution(TTL_MS)
                .await?
                .contains(&user_id),
            "resolved_at is refreshed"
        );

        let outcome =
            resolve_user(&MockResolver::naming(&other_hs, 20), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::IgnoredOlder);

        let outcome =
            resolve_user(&MockResolver::naming(&other_hs, 40), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched { catch_up: false });

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// No record found keeps the binding and the clock, and only refreshes
    /// `resolved_at`: an outage must not move anything.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_missing_record_keeps_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 10, false).await?;
        backdate_resolved_at(&user_id).await?;

        let outcome = resolve_user(&MockResolver::missing(), &primary, &user_pk).await?;
        assert_eq!(outcome, Resolution::Kept);
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&stored_hs, false, Some(10))
        );
        assert!(get_user_ids_by_homeserver(&stored_hs)
            .await?
            .contains(&user_id));
        assert!(
            !get_users_needing_resolution(TTL_MS)
                .await?
                .contains(&user_id),
            "a missing record defers the next lookup by a TTL"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A failed lookup is attributed to the mapping it would have affected and
    /// leaves the stored mapping untouched.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_lookup_failure_leaves_mapping_untouched() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &stored_hs, 10, false).await?;
        backdate_resolved_at(&user_id).await?;

        let outcome = resolve_user(&FailingResolver, &primary, &user_pk).await?;
        assert_eq!(
            outcome,
            Resolution::LookupFailed {
                mapping: MappingState::Active
            }
        );

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            mapping(&stored_hs, false, Some(10))
        );
        assert!(
            get_users_needing_resolution(TTL_MS)
                .await?
                .contains(&user_id),
            "a failed lookup must not refresh resolved_at, or recovery waits a full TTL"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// The mapping counts feeding the gauges include a user catching up.
    ///
    /// Other tests (in this and other test binaries) create mappings and flip
    /// flags concurrently, so only lower bounds on the global counts and the
    /// catch_up <= mapped invariant are stable.
    #[tokio_shared_rt::test(shared)]
    async fn test_count_mappings() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let primary = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver_from_record(&user_id, &primary, 10, true).await?;

        let counts = count_mappings().await?;
        assert!(counts.catch_up >= 1);
        assert!(counts.mapped >= counts.catch_up);

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// Every variant maps to a bounded, intended label set.
    #[test]
    fn test_resolution_labels() {
        use MappingState::{Active, Unbound};

        let cases = [
            (Resolution::Unbound, Outcome::Unresolved, Unbound),
            (Resolution::Bound, Outcome::Resolved, Unbound),
            (Resolution::Confirmed, Outcome::Resolved, Active),
            (
                Resolution::Switched { catch_up: false },
                Outcome::Resolved,
                Active,
            ),
            (
                Resolution::Switched { catch_up: true },
                Outcome::Resolved,
                Active,
            ),
            (Resolution::IgnoredOlder, Outcome::Resolved, Active),
            (Resolution::NoHomeserver, Outcome::Unresolved, Active),
            (Resolution::Kept, Outcome::Unresolved, Active),
            (
                Resolution::LookupFailed { mapping: Active },
                Outcome::Error,
                Active,
            ),
            (
                Resolution::LookupFailed { mapping: Unbound },
                Outcome::Error,
                Unbound,
            ),
        ];

        for (resolution, outcome, mapping) in cases {
            assert_eq!(resolution.outcome(), outcome, "{resolution:?}");
            assert_eq!(resolution.mapping(), mapping, "{resolution:?}");
        }
    }

    /// The exported label strings are part of the alerting contract; pin them.
    #[test]
    fn test_outcome_and_mapping_label_strings() {
        assert_eq!(Outcome::Resolved.as_str(), "resolved");
        assert_eq!(Outcome::Unresolved.as_str(), "unresolved");
        assert_eq!(Outcome::Error.as_str(), "error");

        let label = |mapping| mapping_attribute(mapping).value.to_string();
        assert_eq!(label(Some(MappingState::Unbound)), "unbound");
        assert_eq!(label(Some(MappingState::Active)), "active");
        assert_eq!(label(None), "unknown");
        assert_eq!(mapping_attribute(None).key.as_str(), "mapping");
    }

    /// The `_pubky` target is read from HTTPS and SVCB records the way the SDK
    /// reads it; a packet without one, or with a target that is no key, names none.
    #[test]
    fn test_published_homeserver_reads_the_pubky_target() {
        let user = Keypair::random();
        let homeserver = Keypair::random().public_key();
        let homeserver_id = PubkyId::try_from(homeserver.to_z32().as_str()).expect("valid id");
        let name = || Name::try_from("_pubky").expect("valid name");
        fn svcb(target: &str) -> SVCB<'_> {
            SVCB::new(0, target.try_into().expect("valid target"))
        }

        let https = SignedPacket::builder()
            .https(name(), svcb(&homeserver.to_z32()), 3600)
            .sign(&user)
            .expect("signed");
        assert_eq!(published_homeserver(&https), Some(homeserver_id.clone()));

        let svcb_record = SignedPacket::builder()
            .svcb(name(), svcb(&homeserver.to_z32()), 3600)
            .sign(&user)
            .expect("signed");
        assert_eq!(published_homeserver(&svcb_record), Some(homeserver_id));

        let other_name = SignedPacket::builder()
            .https(
                Name::try_from("_other").expect("valid name"),
                svcb("example.com"),
                3600,
            )
            .sign(&user)
            .expect("signed");
        assert_eq!(published_homeserver(&other_name), None);

        let domain_target = SignedPacket::builder()
            .https(name(), svcb("example.com"), 3600)
            .sign(&user)
            .expect("signed");
        assert_eq!(published_homeserver(&domain_target), None);
    }

    #[test]
    fn test_record_is_newer_than_clock() {
        let no_clock = StoredMapping::default();
        assert!(no_clock.record_is_newer(0));

        let clock = StoredMapping {
            record_ts: Some(10),
            ..StoredMapping::default()
        };
        assert!(clock.record_is_newer(11));
        assert!(!clock.record_is_newer(10));
        assert!(!clock.record_is_newer(9));
        assert!(
            clock.record_is_newer(u64::MAX),
            "saturates instead of wrapping"
        );
    }

    #[test]
    fn test_bisection_order() {
        // Empty and single-element edge cases.
        assert!(bisection_order_user_pks(vec![]).is_empty());
        let lone = random_pk();
        let result = bisection_order_user_pks(vec![lone.clone()]);
        assert_eq!(result[0].as_bytes(), lone.as_bytes());

        // For 8 keys, verify the full BFS-bisection permutation.
        // Sort first to establish the ground-truth lexicographic order.
        let mut sorted: Vec<PublicKey> = (0..8).map(|_| random_pk()).collect();
        sorted.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        // BFS over a sorted array of 8 elements (half-open intervals) emits:
        //   (0,8)→4, (0,4)→2, (5,8)→6, (0,2)→1, (3,4)→3, (5,6)→5, (7,8)→7, (0,1)→0
        let expected_indices: [usize; 8] = [4, 2, 6, 1, 3, 5, 7, 0];

        let result = bisection_order_user_pks(sorted.clone());
        assert_eq!(result.len(), 8);
        for (pos, &idx) in expected_indices.iter().enumerate() {
            assert_eq!(
                result[pos].as_bytes(),
                sorted[idx].as_bytes(),
                "position {pos}: expected sorted[{idx}]"
            );
        }
    }
}
