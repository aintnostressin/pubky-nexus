//! # User Homeserver Resolver
//!
//! Periodic task that resolves each user's homeserver and persists
//! the `(:User)-[:HOSTED_BY]->(:Homeserver)` relationship in Neo4j.

use nexus_common::db::{
    fetch_key_from_graph, queries, GraphResult, PubkyClientResult, PubkyConnector,
};
use nexus_common::models::user::{remove_user_homeserver, set_user_homeserver};
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::{global, KeyValue};
use pubky::pkarr::dns::rdata::RData;
use pubky::pkarr::SignedPacket;
use pubky::PublicKey;
use pubky_app_specs::PubkyId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch::Receiver;
use tracing::{debug, error, info, warn};

static HS_RESOLVER_METRICS: LazyLock<HsResolverMetrics> = LazyLock::new(HsResolverMetrics::new);

/// What a user's PKDNS record says about their homeserver.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkdnsRecord {
    /// No record was found. On pubky 0.9.3 DHT and relay failures surface this
    /// way too, so it says nothing about the user's homeserver.
    Missing,
    /// The user's record, naming their homeserver, or `None` if it names none.
    Published(Option<PubkyId>),
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

/// Production resolver backed by the shared [`PubkyConnector`].
pub struct PubkyConnectorResolver;

#[async_trait::async_trait]
impl PkdnsHomeserverResolver for PubkyConnectorResolver {
    async fn resolve_homeserver(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
        let pubky = PubkyConnector::get()?;
        // The lookup behind `Pubky::get_homeserver_of`, which returns `None` both
        // when no record is found and when the record names no homeserver.
        let record = match pubky.client().pkarr().resolve(user_pk).await {
            Some(packet) => PkdnsRecord::Published(published_homeserver(&packet)),
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
            shutdown_rx,
        }
    }

    pub async fn run(&self) -> Result<(), DynError> {
        let mut shutdown_rx = self.shutdown_rx.clone();
        run(self.resolver.as_ref(), self.ttl_ms, &mut shutdown_rx).await
    }
}

/// Main entry point for one cycle of the periodic task.
///
/// `ttl_ms` controls the minimum time before a user's mapping is re-resolved.
/// Users whose `HOSTED_BY.resolved_at` is newer than `ttl_ms` are skipped.
///
/// `shutdown_rx` cancels the in-flight resolution on shutdown; cancelled users
/// get re-picked-up on the next run via TTL.
pub async fn run(
    resolver: &dyn PkdnsHomeserverResolver,
    ttl_ms: u64,
    shutdown_rx: &mut Receiver<bool>,
) -> Result<(), DynError> {
    let user_ids = get_users_needing_resolution(ttl_ms).await?;
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
            result = resolve_user(resolver, &user_pk) => {
                let user_id = user_pk.z32();
                processed += 1;
                let (outcome, mapping) = match result {
                    // Graph read or write failed: a Neo4j problem, not a resolution one.
                    Err(e) => {
                        warn!(%user_id, error = %e, "Failed to read or update HS mapping");
                        (Outcome::Error, None)
                    }
                    Ok(resolution) => {
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

/// Refreshes the `mapped_users` gauge from the graph.
///
/// A failed count is logged but does not fail the run: the metrics are an
/// observer of the resolver, not part of it.
async fn refresh_mapping_gauges() {
    match count_mapped_users().await {
        Ok(mapped) => HS_RESOLVER_METRICS.record_mapped_users(mapped),
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

/// Fetches user IDs whose homeserver mapping is stale or missing.
///
/// A mapping is considered stale when its `resolved_at` timestamp is older
/// than `ttl_ms` milliseconds ago.
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
    /// No `HOSTED_BY` edge.
    Unbound,
    /// Bound to a homeserver.
    Active,
}

impl MappingState {
    fn of(stored_hs_id: &Option<String>) -> Self {
        match stored_hs_id {
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
    /// No edge and no homeserver published; graph untouched.
    Unbound,
    /// No edge before; bound to the published HS now.
    Bound,
    /// Published HS matches the stored one; `resolved_at` refreshed.
    Confirmed,
    /// Published HS differs from the stored one; edge moved to it.
    Switched,
    /// The record names no homeserver; edge removed.
    Removed,
    /// No record found; edge kept and `resolved_at` refreshed.
    Kept,
    /// The lookup itself failed (pubky 0.10+ surfaces transport errors);
    /// graph untouched.
    LookupFailed { mapping: MappingState },
}

impl Resolution {
    /// `outcome` label: whether PKDNS returned a homeserver.
    fn outcome(self) -> Outcome {
        match self {
            Self::Bound | Self::Confirmed | Self::Switched => Outcome::Resolved,
            Self::Unbound | Self::Removed | Self::Kept => Outcome::Unresolved,
            Self::LookupFailed { .. } => Outcome::Error,
        }
    }

    /// `mapping` label: the stored mapping's state before this resolution.
    fn mapping(self) -> MappingState {
        match self {
            Self::Unbound | Self::Bound => MappingState::Unbound,
            Self::Confirmed | Self::Switched | Self::Removed | Self::Kept => MappingState::Active,
            Self::LookupFailed { mapping } => mapping,
        }
    }
}

/// Resolves a single user's HS and updates the HOSTED_BY relationship to match.
///
/// The relationship only changes when the user's record says so: a record naming
/// another HS moves it, a record naming none removes it. A missing record keeps it.
///
/// A failed lookup is reported as a [`Resolution`] and leaves the graph
/// untouched; only graph errors are returned as `Err`.
async fn resolve_user(
    resolver: &dyn PkdnsHomeserverResolver,
    user_pk: &PublicKey,
) -> Result<Resolution, DynError> {
    let user_id = user_pk.z32();

    // Read the stored mapping first so a failed lookup can still be attributed
    // to the mapping state it would have affected.
    let stored_hs_id = get_user_homeserver(&user_id).await?;

    let record = match resolver.resolve_homeserver(user_pk).await {
        Ok(record) => record,
        Err(e) => {
            warn!(%user_id, error = %e, "PKDNS lookup failed");
            return Ok(Resolution::LookupFailed {
                mapping: MappingState::of(&stored_hs_id),
            });
        }
    };

    let resolution = match (stored_hs_id, record) {
        (None, PkdnsRecord::Missing | PkdnsRecord::Published(None)) => {
            warn!(%user_id, "User has no published homeserver");
            Resolution::Unbound
        }

        (None, PkdnsRecord::Published(Some(resolved_hs_id))) => {
            set_user_homeserver(&user_id, &resolved_hs_id).await?;
            debug!(%user_id, homeserver = %resolved_hs_id, "HS mapping created");
            Resolution::Bound
        }

        // Setting the stored HS again only refreshes `resolved_at`.
        (Some(stored_hs_id), PkdnsRecord::Published(Some(resolved_hs_id)))
            if resolved_hs_id.as_ref() == stored_hs_id =>
        {
            set_user_homeserver(&user_id, &stored_hs_id).await?;
            debug!(%user_id, homeserver = %stored_hs_id, "HS mapping still active");
            Resolution::Confirmed
        }

        (Some(stored_hs_id), PkdnsRecord::Published(Some(resolved_hs_id))) => {
            set_user_homeserver(&user_id, &resolved_hs_id).await?;
            info!(
                %user_id,
                stored_homeserver = %stored_hs_id,
                homeserver = %resolved_hs_id,
                "User published another homeserver; HS mapping switched"
            );
            Resolution::Switched
        }

        (Some(stored_hs_id), PkdnsRecord::Published(None)) => {
            remove_user_homeserver(&user_id).await?;
            info!(
                %user_id,
                stored_homeserver = %stored_hs_id,
                "User published no homeserver; HS mapping removed"
            );
            Resolution::Removed
        }

        // A missing record says nothing about the HS, so the stored one stands.
        // `resolved_at` is still refreshed, deferring the next lookup by a TTL.
        (Some(stored_hs_id), PkdnsRecord::Missing) => {
            set_user_homeserver(&user_id, &stored_hs_id).await?;
            debug!(%user_id, homeserver = %stored_hs_id, "No PKDNS record found; HS mapping kept");
            Resolution::Kept
        }
    };

    Ok(resolution)
}

/// Returns the ID of the homeserver the user is bound to, if any.
async fn get_user_homeserver(user_id: &str) -> GraphResult<Option<String>> {
    let query = queries::get::get_user_homeserver(user_id);
    fetch_key_from_graph(query, "homeserver_id").await
}

/// Counts non-deleted users with a `HOSTED_BY` mapping.
async fn count_mapped_users() -> GraphResult<u64> {
    let query = queries::get::count_user_homeserver_mappings();
    let mapped_users = fetch_key_from_graph(query, "mapped_users").await?;
    Ok(mapped_users.unwrap_or_default())
}

/// Returns all user IDs hosted on a given homeserver.
pub async fn get_user_ids_by_homeserver(hs_id: &str) -> GraphResult<Vec<String>> {
    let query = queries::get::get_active_users_by_homeserver(hs_id);
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
    /// Non-deleted users with a `HOSTED_BY` mapping. Refreshed after every run
    /// that processed users.
    mapped_users: Gauge<u64>,
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
            mapped_users: meter
                .u64_gauge("nexus.task.hs-resolver.mapped_users")
                .with_description("Users with a homeserver mapping")
                .build(),
            heartbeat: meter
                .u64_gauge("nexus.task.hs-resolver.heartbeat_timestamp")
                .with_description("Unix time of the resolver's most recent progress")
                .with_unit("s")
                .build(),
            gauges_populated: AtomicBool::new(false),
        }
    }

    fn record_mapped_users(&self, mapped: u64) {
        self.mapped_users.record(mapped, &[]);
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
    use nexus_common::types::DynError;
    use nexus_common::utils::test_utils::{random_pk, random_pubky_id};
    use nexus_common::{StackConfig, StackManager};

    async fn setup() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await
    }

    /// Resolver stub returning a fixed PKDNS result, so `resolve_user` can be
    /// driven without touching the DHT.
    struct MockResolver {
        result: PkdnsRecord,
    }

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for MockResolver {
        async fn resolve_homeserver(&self, _user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            Ok(self.result.clone())
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

    /// Helper: backdate a user's mapping (2 hours ago) so the user is due for resolution
    async fn backdate_hs_mapping(user_id: &str) -> GraphResult<()> {
        let query = Query::new(
            "backdate_hs_mapping",
            "MATCH (u:User {id: $user_id})-[r:HOSTED_BY]->(:Homeserver)
             SET r.resolved_at = timestamp() - 7200000",
        )
        .param("user_id", user_id);
        exec_single_row(query).await
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_set_user_homeserver_graph_query() -> Result<(), DynError> {
        setup().await?;

        let user_id = "hs_resolver_test_user_001";
        let hs_id_a = "hs_resolver_test_hs_aaa";
        let hs_id_b = "hs_resolver_test_hs_bbb";

        create_test_user(user_id).await?;

        // Set initial homeserver
        let query = queries::put::set_user_homeserver(user_id, hs_id_a);
        exec_single_row(query).await?;

        // Switch to a different homeserver
        let query = queries::put::set_user_homeserver(user_id, hs_id_b);
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
        let query = queries::put::set_user_homeserver(user_id, hs_id);
        exec_single_row(query).await?;

        // Set same homeserver again (should reuse HS, e.g. not create any orphan HS)
        let query = queries::put::set_user_homeserver(user_id, hs_id);
        exec_single_row(query).await?;

        // Cleanup
        cleanup_test_user(user_id).await?;

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

        // Give user_stale a mapping with an old resolved_at (1 hour ago)
        let stale_query = Query::new(
            "set_stale_hs",
            "MATCH (u:User {id: $user_id})
             MERGE (hs:Homeserver {id: $hs_id})
             MERGE (u)-[r:HOSTED_BY]->(hs)
             SET r.resolved_at = timestamp() - 7200000",
        )
        .param("user_id", user_stale)
        .param("hs_id", hs_id);
        exec_single_row(stale_query).await?;

        // user_no_hs has no HOSTED_BY at all

        // With a 1-hour TTL: user_fresh should be skipped, user_stale and user_no_hs returned
        let mut needing = get_users_needing_resolution(3_600_000).await?;
        needing.sort();

        assert!(
            !needing.contains(&user_fresh.to_string()),
            "Recently resolved user should be skipped"
        );
        assert!(
            needing.contains(&user_stale.to_string()),
            "Stale user should need resolution"
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
        let hs_id = random_pk().z32();

        create_test_user(&user_id).await?;

        // No HOSTED_BY edge yet
        assert_eq!(get_user_homeserver(&user_id).await?, None);

        // After assignment the current homeserver is returned
        set_user_homeserver(&user_id, &hs_id).await?;
        assert_eq!(get_user_homeserver(&user_id).await?, Some(hs_id));

        // After removal the user has no homeserver again
        remove_user_homeserver(&user_id).await?;
        assert_eq!(get_user_homeserver(&user_id).await?, None);

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// First-time resolution stores whatever the DHT resolves.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_first_time_sets_homeserver() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let hs_id = random_pubky_id();

        create_test_user(&user_id).await?;

        let resolver = MockResolver {
            result: PkdnsRecord::Published(Some(hs_id.clone())),
        };
        let outcome = resolve_user(&resolver, &user_pk).await?;
        assert_eq!(outcome, Resolution::Bound);

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            Some(hs_id.to_string())
        );
        assert!(get_user_ids_by_homeserver(&hs_id).await?.contains(&user_id));

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// A user with no stored mapping is left alone when PKDNS names no
    /// homeserver, whether no record is found or the record names none.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_first_time_no_homeserver_noop() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();

        create_test_user(&user_id).await?;

        for record in [PkdnsRecord::Missing, PkdnsRecord::Published(None)] {
            let resolver = MockResolver { result: record };
            let outcome = resolve_user(&resolver, &user_pk).await?;
            assert_eq!(outcome, Resolution::Unbound);
            assert_eq!(get_user_homeserver(&user_id).await?, None);
        }
        assert!(
            get_users_needing_resolution(3_600_000)
                .await?
                .contains(&user_id),
            "users with no HS PKDNS mapping found should be retried on every resolver run"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// When the published homeserver changes, the binding moves to it.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_change_switches_homeserver() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();
        let new_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;

        // DHT now points at a different homeserver
        let resolver = MockResolver {
            result: PkdnsRecord::Published(Some(new_hs.clone())),
        };
        let outcome = resolve_user(&resolver, &user_pk).await?;
        assert_eq!(outcome, Resolution::Switched);

        // The user is indexed on the new homeserver only
        assert_eq!(
            get_user_homeserver(&user_id).await?,
            Some(new_hs.to_string())
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

    /// When the user publishes a record naming no homeserver, the binding is removed.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_empty_record_removes_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;

        let resolver = MockResolver {
            result: PkdnsRecord::Published(None),
        };
        let outcome = resolve_user(&resolver, &user_pk).await?;
        assert_eq!(outcome, Resolution::Removed);

        assert_eq!(get_user_homeserver(&user_id).await?, None);
        assert!(!get_user_ids_by_homeserver(&stored_hs)
            .await?
            .contains(&user_id));
        assert!(
            get_users_needing_resolution(3_600_000)
                .await?
                .contains(&user_id),
            "an unbound user should be retried on every resolver run"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// When the published homeserver matches the stored one, the binding stays
    /// and `resolved_at` is refreshed.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_same_homeserver_confirms_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;
        backdate_hs_mapping(&user_id).await?;

        let resolver = MockResolver {
            result: PkdnsRecord::Published(Some(stored_hs.clone())),
        };
        let outcome = resolve_user(&resolver, &user_pk).await?;
        assert_eq!(outcome, Resolution::Confirmed);

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            Some(stored_hs.to_string())
        );
        assert!(
            !get_users_needing_resolution(3_600_000)
                .await?
                .contains(&user_id),
            "a confirmed mapping should not be due again before the TTL passes"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// When no record is found, the binding is kept and the user stays indexed;
    /// `resolved_at` is refreshed so the next lookup waits a TTL.
    #[tokio_shared_rt::test(shared)]
    async fn test_resolve_user_missing_record_keeps_binding() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;
        backdate_hs_mapping(&user_id).await?;

        let resolver = MockResolver {
            result: PkdnsRecord::Missing,
        };
        let outcome = resolve_user(&resolver, &user_pk).await?;
        assert_eq!(outcome, Resolution::Kept);

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            Some(stored_hs.to_string())
        );
        assert!(get_user_ids_by_homeserver(&stored_hs)
            .await?
            .contains(&user_id));
        assert!(
            !get_users_needing_resolution(3_600_000)
                .await?
                .contains(&user_id),
            "a missing record should defer the next lookup by a TTL"
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

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;
        backdate_hs_mapping(&user_id).await?;

        let outcome = resolve_user(&FailingResolver, &user_pk).await?;
        assert_eq!(
            outcome,
            Resolution::LookupFailed {
                mapping: MappingState::Active
            }
        );

        assert_eq!(
            get_user_homeserver(&user_id).await?,
            Some(stored_hs.to_string())
        );
        assert!(
            get_users_needing_resolution(3_600_000)
                .await?
                .contains(&user_id),
            "a failed lookup must not refresh resolved_at, or recovery waits a full TTL"
        );

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// The count feeding the `mapped_users` gauge includes a fresh mapping.
    ///
    /// Other tests (in this and other test binaries) create and remove mappings
    /// concurrently, so only a lower bound on the global count is stable.
    #[tokio_shared_rt::test(shared)]
    async fn test_count_mapped_users() -> Result<(), DynError> {
        setup().await?;

        let user_pk = random_pk();
        let user_id = user_pk.z32();
        let stored_hs = random_pubky_id();

        create_test_user(&user_id).await?;
        set_user_homeserver(&user_id, &stored_hs).await?;

        assert!(count_mapped_users().await? >= 1);

        cleanup_test_user(&user_id).await?;

        Ok(())
    }

    /// Only a `_pubky` record targeting a public key names a homeserver; a
    /// record without one, or with any other target, names none.
    #[test]
    fn test_published_homeserver() {
        use pubky::pkarr::dns::rdata::SVCB;
        use pubky::Keypair;

        let keypair = Keypair::random();
        let packet = |target: Option<&str>| {
            let mut builder = SignedPacket::builder();
            if let Some(target) = target {
                let svcb = SVCB::new(0, target.try_into().expect("valid target"));
                builder = builder.https("_pubky".try_into().expect("valid name"), svcb, 3600);
            }
            builder.sign(&keypair).expect("signed packet")
        };

        let hs_id = random_pubky_id();
        assert_eq!(
            published_homeserver(&packet(Some(hs_id.as_ref()))),
            Some(hs_id)
        );
        assert_eq!(published_homeserver(&packet(None)), None);
        assert_eq!(published_homeserver(&packet(Some("."))), None);
        assert_eq!(published_homeserver(&packet(Some("example.com"))), None);
    }

    /// Every variant maps to a bounded, intended label set.
    #[test]
    fn test_resolution_labels() {
        use MappingState::{Active, Unbound};

        let cases = [
            (Resolution::Unbound, Outcome::Unresolved, Unbound),
            (Resolution::Bound, Outcome::Resolved, Unbound),
            (Resolution::Confirmed, Outcome::Resolved, Active),
            (Resolution::Switched, Outcome::Resolved, Active),
            (Resolution::Removed, Outcome::Unresolved, Active),
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
