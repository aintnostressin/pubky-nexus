//! A user's `HOSTED_BY` binding and how a resolved `_pubky` record is applied
//! to it. Shared by the watcher's periodic resolver and the API's recheck.

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use utoipa::ToSchema;

use super::{PkdnsRecord, StoredRecord};
use crate::db::{exec_single_row, fetch_row_from_graph, queries, GraphResult};

/// What happens to a bound user whose record no longer names the stored
/// homeserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StaleMapping {
    /// Keep the mapping and mark it `stale`: the user is indexed nowhere until
    /// the record names the stored homeserver again. The default.
    #[default]
    Enabled,
    /// Follow the record: a record naming another homeserver moves the mapping
    /// there, a record naming none keeps it. A stale flag left from before is
    /// cleared either way.
    Disabled,
}

/// Why a mapping was marked stale. Both are records the key holder signed; a
/// lookup that finds no record marks nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReason {
    /// The record names no homeserver.
    Unresolved,
    /// The record names a homeserver different from the stored one.
    HsChanged,
}

/// A user's stored mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeserverMapping {
    pub hs_id: String,
    pub stale: bool,
    /// The timestamp of the record the mapping was last applied from, if any.
    pub record_ts: Option<i64>,
}

/// What a resolved record does to the stored mapping: decided by
/// [`Self::decide`], written by [`Self::apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingUpdate {
    /// No mapping and nothing published; graph untouched.
    Unbound,
    /// No mapping before; bound to the published homeserver now.
    Bound { to: String },
    /// The published homeserver is the stored one; the stale flag is cleared.
    Confirmed { was_stale: bool },
    /// The record names no homeserver or another one; the mapping is kept
    /// and marked stale ([`StaleMapping::Enabled`]).
    MarkedStale {
        reason: StaleReason,
        was_stale: bool,
    },
    /// Bound, and no record was found: nothing is known, so the mapping, its
    /// flag and its record stand; only `resolved_at` is refreshed.
    NoRecord { was_stale: bool },
    /// The record is older than the one the mapping was applied from; the
    /// mapping, its flag and its record stand; only `resolved_at` is refreshed.
    IgnoredOlder {
        was_stale: bool,
        /// Whether the ignored record named a homeserver.
        names_homeserver: bool,
    },
    /// The mapping moved to the published homeserver ([`StaleMapping::Disabled`]).
    Switched {
        from: String,
        to: String,
        was_stale: bool,
    },
    /// The record names no homeserver; the mapping is kept and the stale flag
    /// cleared ([`StaleMapping::Disabled`]).
    Kept { was_stale: bool },
}

impl MappingUpdate {
    /// What the resolved record does to the stored mapping.
    ///
    /// A record older than the one the mapping was applied from, by its
    /// timestamp against the mapping's `record_ts`, is ignored: it is a replay
    /// or a lagging node's answer and changes nothing, whatever it names. No
    /// record at all says nothing either: the mapping, its flag and its record
    /// stand.
    pub fn decide(
        stored: Option<&HomeserverMapping>,
        record: &PkdnsRecord,
        stale_mapping: StaleMapping,
    ) -> Self {
        let resolved = record.homeserver();
        let Some(stored) = stored else {
            return match (resolved, record.stored()) {
                (Some(hs_id), Some(_)) => Self::Bound {
                    to: hs_id.to_string(),
                },
                _ => Self::Unbound,
            };
        };
        let was_stale = stored.stale;

        if matches!(record, PkdnsRecord::Missing) {
            return Self::NoRecord { was_stale };
        }
        let older = record
            .stored()
            .zip(stored.record_ts)
            .is_some_and(|(record, clock)| record.timestamp < clock);
        if older {
            return Self::IgnoredOlder {
                was_stale,
                names_homeserver: resolved.is_some(),
            };
        }

        match (resolved, stale_mapping) {
            (Some(hs_id), _) if hs_id.as_ref() == stored.hs_id => Self::Confirmed { was_stale },
            (resolved, StaleMapping::Enabled) => Self::MarkedStale {
                reason: match resolved {
                    None => StaleReason::Unresolved,
                    Some(_) => StaleReason::HsChanged,
                },
                was_stale,
            },
            (Some(hs_id), StaleMapping::Disabled) => Self::Switched {
                from: stored.hs_id.clone(),
                to: hs_id.to_string(),
                was_stale,
            },
            (None, StaleMapping::Disabled) => Self::Kept { was_stale },
        }
    }

    /// Writes the update to `user_id`'s mapping, keeping `record` on the edge
    /// as what the key holder published, except where the update stands for no
    /// usable record. The user node must exist. Every variant but
    /// [`Self::Unbound`] refreshes `resolved_at`.
    pub async fn apply(&self, user_id: &str, record: &PkdnsRecord) -> GraphResult<()> {
        let stored_record = record.stored();
        match self {
            Self::Unbound => Ok(()),
            Self::Bound { to } => {
                debug!(%user_id, homeserver = %to, "HS mapping created");
                match &stored_record {
                    // `decide` only binds from a storable record.
                    Some(stored_record) => {
                        set_user_homeserver_from_record(user_id, to, stored_record).await
                    }
                    None => set_user_homeserver(user_id, to).await,
                }
            }
            Self::Confirmed { .. } => {
                debug!(%user_id, "HS mapping still active");
                set_user_homeserver_stale(user_id, false, stored_record.as_ref()).await
            }
            Self::MarkedStale { reason, .. } => {
                warn!(%user_id, ?reason, "User homeserver changed or was removed; switching disabled, mapping marked stale");
                set_user_homeserver_stale(user_id, true, stored_record.as_ref()).await
            }
            Self::NoRecord { was_stale } => {
                debug!(%user_id, "No PKDNS record found; HS mapping left as it is");
                set_user_homeserver_stale(user_id, *was_stale, None).await
            }
            Self::IgnoredOlder { was_stale, .. } => {
                warn!(%user_id, "PKDNS record is older than the one applied; ignored");
                set_user_homeserver_stale(user_id, *was_stale, None).await
            }
            Self::Switched { from, to, .. } => {
                info!(%user_id, %from, %to, "User published another homeserver; HS mapping switched");
                match &stored_record {
                    // `decide` only switches on a storable record.
                    Some(stored_record) => {
                        set_user_homeserver_from_record(user_id, to, stored_record).await
                    }
                    None => set_user_homeserver(user_id, to).await,
                }
            }
            Self::Kept { .. } => {
                debug!(%user_id, "Record names no homeserver; HS mapping kept");
                set_user_homeserver_stale(user_id, false, stored_record.as_ref()).await
            }
        }
    }

    /// The homeserver the user is bound to once the update is applied to
    /// `stored`, if any.
    pub fn homeserver_after(&self, stored: Option<&HomeserverMapping>) -> Option<String> {
        match self {
            Self::Bound { to } | Self::Switched { to, .. } => Some(to.clone()),
            Self::Unbound => None,
            Self::Confirmed { .. }
            | Self::MarkedStale { .. }
            | Self::NoRecord { .. }
            | Self::IgnoredOlder { .. }
            | Self::Kept { .. } => stored.map(|mapping| mapping.hs_id.clone()),
        }
    }

    /// The homeserver the update binds the user to, when it binds one.
    pub fn bound_to(&self) -> Option<&str> {
        match self {
            Self::Bound { to } | Self::Switched { to, .. } => Some(to),
            Self::Unbound
            | Self::Confirmed { .. }
            | Self::MarkedStale { .. }
            | Self::NoRecord { .. }
            | Self::IgnoredOlder { .. }
            | Self::Kept { .. } => None,
        }
    }
}

/// Returns the user's stored mapping, if any.
pub async fn get_user_homeserver(user_id: &str) -> GraphResult<Option<HomeserverMapping>> {
    let query = queries::get::get_user_homeserver(user_id);
    let Some(row) = fetch_row_from_graph(query).await? else {
        return Ok(None);
    };
    Ok(Some(HomeserverMapping {
        hs_id: row.get("homeserver_id")?,
        stale: row.get("stale")?,
        record_ts: row.get("record_ts")?,
    }))
}

/// Binds a user to a homeserver without a PKDNS record, as tests do, recording
/// the `HOSTED_BY` relationship and `resolved_at`; the resolver and the ingestor
/// bind from the record they resolved, see [`set_user_homeserver_from_record`].
pub async fn set_user_homeserver(user_id: &str, homeserver_id: &str) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver(
        user_id,
        homeserver_id,
        None,
    ))
    .await
}

/// Binds a user to the homeserver their PKDNS record names, keeping the record
/// on the `HOSTED_BY` relationship.
pub async fn set_user_homeserver_from_record(
    user_id: &str,
    homeserver_id: &str,
    record: &StoredRecord,
) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver(
        user_id,
        homeserver_id,
        Some(record),
    ))
    .await
}

/// Toggles the stale flag on a user's existing homeserver mapping, keeping the
/// PKDNS record the flag was applied from on the relationship when there is one.
pub async fn set_user_homeserver_stale(
    user_id: &str,
    stale: bool,
    record: Option<&StoredRecord>,
) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver_stale(
        user_id, stale, record,
    ))
    .await
}

/// The result of re-checking a user's homeserver against the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HomeserverRecheck {
    /// The homeserver the user is bound to after the check, if any.
    pub homeserver: Option<String>,
    pub outcome: RecheckOutcome,
}

/// What a recheck did to the user's mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecheckOutcome {
    /// Nothing is published and the user has no mapping.
    Unbound,
    /// The user had no mapping and is now bound to the published homeserver.
    Bound,
    /// The published homeserver is the one the user was bound to.
    Confirmed,
    /// The mapping moved to the published homeserver.
    Switched,
    /// The record said nothing new; the mapping was kept.
    Kept,
}

/// A recheck follows the record ([`StaleMapping::Disabled`]), so it never
/// marks a mapping stale; should one arrive anyway, it reads as kept.
impl From<&MappingUpdate> for RecheckOutcome {
    fn from(update: &MappingUpdate) -> Self {
        match update {
            MappingUpdate::Unbound => Self::Unbound,
            MappingUpdate::Bound { .. } => Self::Bound,
            MappingUpdate::Confirmed { .. } => Self::Confirmed,
            MappingUpdate::Switched { .. } => Self::Switched,
            MappingUpdate::Kept { .. }
            | MappingUpdate::NoRecord { .. }
            | MappingUpdate::IgnoredOlder { .. }
            | MappingUpdate::MarkedStale { .. } => Self::Kept,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::{random_pubky_id, signed_packet_naming};
    use pubky::Keypair;
    use pubky_app_specs::PubkyId;

    fn naming(user: &Keypair, homeserver: Option<&PubkyId>, timestamp: u64) -> PkdnsRecord {
        PkdnsRecord::Published(signed_packet_naming(user, homeserver, timestamp))
    }

    /// Every combination of stored mapping, resolved record and policy maps to
    /// one update, and the policy only matters when the record and the stored
    /// mapping disagree.
    #[test]
    fn decide_covers_every_case() {
        use MappingUpdate::*;
        use StaleMapping::{Disabled, Enabled};
        use StaleReason::{HsChanged, Unresolved};

        let user = Keypair::random();
        let (a, b) = (random_pubky_id(), random_pubky_id());
        let on_a = |stale| HomeserverMapping {
            hs_id: a.to_string(),
            stale,
            record_ts: Some(10),
        };
        let switched = |was_stale| Switched {
            from: a.to_string(),
            to: b.to_string(),
            was_stale,
        };
        let cases = [
            (None, PkdnsRecord::Missing, Enabled, Unbound),
            (None, PkdnsRecord::Missing, Disabled, Unbound),
            (None, naming(&user, None, 10), Enabled, Unbound),
            (
                None,
                naming(&user, Some(&a), 10),
                Enabled,
                Bound { to: a.to_string() },
            ),
            (
                None,
                naming(&user, Some(&a), 10),
                Disabled,
                Bound { to: a.to_string() },
            ),
            (
                Some(on_a(false)),
                naming(&user, Some(&a), 10),
                Enabled,
                Confirmed { was_stale: false },
            ),
            (
                Some(on_a(true)),
                naming(&user, Some(&a), 10),
                Disabled,
                Confirmed { was_stale: true },
            ),
            (
                Some(on_a(false)),
                naming(&user, Some(&b), 10),
                Enabled,
                MarkedStale {
                    reason: HsChanged,
                    was_stale: false,
                },
            ),
            (
                Some(on_a(true)),
                naming(&user, None, 10),
                Enabled,
                MarkedStale {
                    reason: Unresolved,
                    was_stale: true,
                },
            ),
            (
                Some(on_a(false)),
                PkdnsRecord::Missing,
                Enabled,
                NoRecord { was_stale: false },
            ),
            (
                Some(on_a(true)),
                PkdnsRecord::Missing,
                Disabled,
                NoRecord { was_stale: true },
            ),
            (
                Some(on_a(false)),
                naming(&user, Some(&b), 9),
                Enabled,
                IgnoredOlder {
                    was_stale: false,
                    names_homeserver: true,
                },
            ),
            (
                Some(on_a(false)),
                naming(&user, None, 9),
                Disabled,
                IgnoredOlder {
                    was_stale: false,
                    names_homeserver: false,
                },
            ),
            (
                Some(on_a(false)),
                naming(&user, Some(&b), 10),
                Disabled,
                switched(false),
            ),
            (
                Some(on_a(true)),
                naming(&user, Some(&b), 11),
                Disabled,
                switched(true),
            ),
            (
                Some(on_a(false)),
                naming(&user, None, 10),
                Disabled,
                Kept { was_stale: false },
            ),
            (
                Some(on_a(true)),
                naming(&user, None, 10),
                Disabled,
                Kept { was_stale: true },
            ),
        ];
        for (stored, record, policy, expected) in cases {
            assert_eq!(
                MappingUpdate::decide(stored.as_ref(), &record, policy),
                expected,
                "stored {stored:?}, record {record:?}, {policy:?}"
            );
        }
    }

    /// After an update the user is bound to the homeserver it bound, or to
    /// the stored one when it kept it, or to none.
    #[test]
    fn homeserver_after_follows_the_update() {
        use MappingUpdate::*;

        let (a, b) = (random_pubky_id(), random_pubky_id());
        let on_a = Some(HomeserverMapping {
            hs_id: a.to_string(),
            stale: false,
            record_ts: None,
        });
        let cases = [
            (Unbound, None, None),
            (Bound { to: b.to_string() }, None, Some(b.to_string())),
            (
                Confirmed { was_stale: false },
                on_a.clone(),
                Some(a.to_string()),
            ),
            (
                MarkedStale {
                    reason: StaleReason::Unresolved,
                    was_stale: true,
                },
                on_a.clone(),
                Some(a.to_string()),
            ),
            (
                NoRecord { was_stale: false },
                on_a.clone(),
                Some(a.to_string()),
            ),
            (
                IgnoredOlder {
                    was_stale: false,
                    names_homeserver: true,
                },
                on_a.clone(),
                Some(a.to_string()),
            ),
            (
                Switched {
                    from: a.to_string(),
                    to: b.to_string(),
                    was_stale: false,
                },
                on_a.clone(),
                Some(b.to_string()),
            ),
            (Kept { was_stale: true }, on_a.clone(), Some(a.to_string())),
        ];
        for (update, stored, expected) in cases {
            assert_eq!(
                update.homeserver_after(stored.as_ref()),
                expected,
                "{update:?}"
            );
        }
    }
}
