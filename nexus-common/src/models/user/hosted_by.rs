//! A user's `HOSTED_BY` binding and how a resolved `_pubky` record is applied
//! to it: decided by [`MappingUpdate::decide`], written by [`MappingUpdate::apply`].
//! Shared by the watcher's periodic sweep and the API's recheck.
//!
//! The record a mapping was last applied from is kept on the edge, and a
//! record older than it is a replay or a lagging answer: it changes nothing,
//! whatever it names and whatever the policy. The same record seen again is
//! applied again, so a policy that follows records can act on a record the
//! other policy only marked stale. No record at all says nothing and changes
//! nothing; a record naming no homeserver is a change the key holder signed,
//! and marks the mapping stale under both policies.

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use utoipa::ToSchema;

use crate::db::{exec_single_row, fetch_row_from_graph, queries, GraphResult};
use crate::models::user::{PkdnsRecord, StoredRecord};

/// What happens to a bound user whose record no longer names the stored
/// homeserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StaleMapping {
    /// Keep the mapping and mark it `stale`: the user is indexed nowhere until
    /// the record names the stored homeserver again. The default.
    #[default]
    Enabled,
    /// Follow the record: a record naming another homeserver moves the mapping
    /// there, clearing a stale flag left from before.
    Disabled,
}

/// Why a mapping was marked stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReason {
    /// PKDNS returned no homeserver.
    Unresolved,
    /// PKDNS returned another homeserver.
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

/// What a resolved record does to the stored mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingUpdate {
    /// No mapping and nothing published; graph untouched.
    Unbound,
    /// No mapping before; bound to the published homeserver now.
    Bound { to: String },
    /// The published homeserver is the stored one; the stale flag is cleared.
    Confirmed { was_stale: bool },
    /// The record names no homeserver, or another one under
    /// [`StaleMapping::Enabled`]; the mapping is kept and marked stale.
    MarkedStale {
        reason: StaleReason,
        was_stale: bool,
    },
    /// The mapping moved to the published homeserver ([`StaleMapping::Disabled`]).
    Switched {
        from: String,
        to: String,
        was_stale: bool,
    },
    /// No record was found: nothing is known, so the mapping, its flag and its
    /// record stand; only `resolved_at` is refreshed.
    NoRecord { was_stale: bool },
    /// The record is older than the one the mapping was applied from; the
    /// mapping, its flag and its record stand.
    IgnoredOlder {
        was_stale: bool,
        /// Whether the ignored record named a homeserver.
        names_homeserver: bool,
    },
}

impl MappingUpdate {
    /// What the record does to the stored mapping under `stale_mapping`.
    pub fn decide(
        stored: Option<&HomeserverMapping>,
        record: &PkdnsRecord,
        stale_mapping: StaleMapping,
    ) -> Self {
        let resolved = record.homeserver();
        let Some(stored) = stored else {
            return match resolved {
                None => Self::Unbound,
                Some(hs_id) => Self::Bound {
                    to: hs_id.to_string(),
                },
            };
        };
        let was_stale = stored.stale;
        let Some(timestamp) = record.timestamp() else {
            return Self::NoRecord { was_stale };
        };
        if stored.record_ts.is_some_and(|clock| timestamp < clock) {
            return Self::IgnoredOlder {
                was_stale,
                names_homeserver: resolved.is_some(),
            };
        }
        match (resolved, stale_mapping) {
            (Some(hs_id), _) if hs_id.as_ref() == stored.hs_id => Self::Confirmed { was_stale },
            // A signed statement that the user has no homeserver now.
            (None, _) => Self::MarkedStale {
                reason: StaleReason::Unresolved,
                was_stale,
            },
            (Some(_), StaleMapping::Enabled) => Self::MarkedStale {
                reason: StaleReason::HsChanged,
                was_stale,
            },
            (Some(hs_id), StaleMapping::Disabled) => Self::Switched {
                from: stored.hs_id.clone(),
                to: hs_id.to_string(),
                was_stale,
            },
        }
    }

    /// Writes the update to `user_id`'s mapping, keeping `record`, the record
    /// it was decided from, on the edge when the update applied it. The user
    /// node must exist. Every variant but [`Self::Unbound`] refreshes
    /// `resolved_at`.
    pub async fn apply(&self, user_id: &str, record: Option<&StoredRecord>) -> GraphResult<()> {
        match self {
            Self::Unbound => Ok(()),
            Self::Bound { to } => {
                debug!(%user_id, homeserver = %to, "HS mapping created");
                exec_single_row(queries::put::set_user_homeserver(user_id, to, record)).await
            }
            Self::Confirmed { .. } => {
                debug!(%user_id, "HS mapping still active");
                set_user_homeserver_stale(user_id, false, record).await
            }
            Self::MarkedStale { reason, .. } => {
                warn!(%user_id, ?reason, "User's record no longer names the stored homeserver; mapping marked stale");
                set_user_homeserver_stale(user_id, true, record).await
            }
            Self::Switched { from, to, .. } => {
                info!(%user_id, %from, %to, "User published another homeserver; HS mapping switched");
                exec_single_row(queries::put::set_user_homeserver(user_id, to, record)).await
            }
            Self::NoRecord { was_stale } => {
                debug!(%user_id, "No PKDNS record found; HS mapping left as it is");
                set_user_homeserver_stale(user_id, *was_stale, None).await
            }
            Self::IgnoredOlder { was_stale, .. } => {
                warn!(%user_id, "PKDNS record is older than the one applied; ignored");
                set_user_homeserver_stale(user_id, *was_stale, None).await
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
            | Self::IgnoredOlder { .. } => stored.map(|mapping| mapping.hs_id.clone()),
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
            | Self::IgnoredOlder { .. } => None,
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
    /// The record names no homeserver; the mapping was kept and marked stale,
    /// so the user is indexed nowhere until a record names one.
    Unpublished,
    /// No record was found; the mapping was kept.
    Kept,
    /// The record found is older than the one the mapping was applied from;
    /// the mapping was kept.
    Ignored,
}

/// A recheck follows the record ([`StaleMapping::Disabled`]), so the only
/// update that marks a mapping stale is a record naming no homeserver.
impl From<&MappingUpdate> for RecheckOutcome {
    fn from(update: &MappingUpdate) -> Self {
        match update {
            MappingUpdate::Unbound => Self::Unbound,
            MappingUpdate::Bound { .. } => Self::Bound,
            MappingUpdate::Confirmed { .. } => Self::Confirmed,
            MappingUpdate::Switched { .. } => Self::Switched,
            MappingUpdate::MarkedStale { .. } => Self::Unpublished,
            MappingUpdate::NoRecord { .. } => Self::Kept,
            MappingUpdate::IgnoredOlder { .. } => Self::Ignored,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::{random_pubky_id, signed_packet_naming};
    use pubky::Keypair;

    /// Every combination of stored mapping, record and policy maps to one
    /// update, and the policy only matters when the record and the stored
    /// mapping disagree.
    #[test]
    fn decide_covers_every_case() {
        use MappingUpdate::*;
        use StaleMapping::{Disabled, Enabled};
        use StaleReason::{HsChanged, Unresolved};

        let (a, b) = (random_pubky_id(), random_pubky_id());
        let user = Keypair::random();
        let naming = |hs| PkdnsRecord::Published(signed_packet_naming(&user, Some(hs), 10));
        let naming_none = || PkdnsRecord::Published(signed_packet_naming(&user, None, 10));
        let on_a = |stale| HomeserverMapping {
            hs_id: a.to_string(),
            stale,
            record_ts: None,
        };
        let marked_stale = |reason, was_stale| MarkedStale { reason, was_stale };
        let switched = |was_stale| Switched {
            from: a.to_string(),
            to: b.to_string(),
            was_stale,
        };
        let cases = [
            (None, PkdnsRecord::Missing, Enabled, Unbound),
            (None, naming_none(), Disabled, Unbound),
            (None, naming(&a), Enabled, Bound { to: a.to_string() }),
            (None, naming(&a), Disabled, Bound { to: a.to_string() }),
            (
                Some(on_a(false)),
                naming(&a),
                Enabled,
                Confirmed { was_stale: false },
            ),
            (
                Some(on_a(true)),
                naming(&a),
                Disabled,
                Confirmed { was_stale: true },
            ),
            (
                Some(on_a(false)),
                naming(&b),
                Enabled,
                marked_stale(HsChanged, false),
            ),
            (
                Some(on_a(true)),
                naming_none(),
                Enabled,
                marked_stale(Unresolved, true),
            ),
            (
                Some(on_a(false)),
                PkdnsRecord::Missing,
                Enabled,
                NoRecord { was_stale: false },
            ),
            (Some(on_a(false)), naming(&b), Disabled, switched(false)),
            (Some(on_a(true)), naming(&b), Disabled, switched(true)),
            (
                Some(on_a(false)),
                naming_none(),
                Disabled,
                marked_stale(Unresolved, false),
            ),
            (
                Some(on_a(true)),
                PkdnsRecord::Missing,
                Disabled,
                NoRecord { was_stale: true },
            ),
        ];
        for (stored, record, policy, expected) in cases {
            assert_eq!(
                MappingUpdate::decide(stored.as_ref(), &record, policy),
                expected,
                "stored {stored:?}, record names {:?}, {policy:?}",
                record.homeserver()
            );
        }
    }

    /// A record older than the one the mapping was applied from is ignored
    /// under both policies, whatever it names; the same record again, or a
    /// newer one, is applied. A mapping without a clock takes any record.
    #[test]
    fn decide_ignores_records_older_than_the_applied_one() {
        use MappingUpdate::*;
        use StaleMapping::{Disabled, Enabled};

        let (a, b) = (random_pubky_id(), random_pubky_id());
        let user = Keypair::random();
        let naming =
            |hs: Option<&_>, ts| PkdnsRecord::Published(signed_packet_naming(&user, hs, ts));
        let on_a = |stale, record_ts| HomeserverMapping {
            hs_id: a.to_string(),
            stale,
            record_ts,
        };
        let ignored = |was_stale, names_homeserver| IgnoredOlder {
            was_stale,
            names_homeserver,
        };

        for policy in [Enabled, Disabled] {
            assert_eq!(
                MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(Some(&b), 10), policy),
                ignored(false, true),
                "{policy:?}: an older record naming another homeserver"
            );
            assert_eq!(
                MappingUpdate::decide(Some(&on_a(true, Some(20))), &naming(Some(&a), 10), policy),
                ignored(true, true),
                "{policy:?}: an older record naming the stored homeserver does not clear the flag"
            );
            assert_eq!(
                MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(None, 10), policy),
                ignored(false, false),
                "{policy:?}: an older record naming none"
            );
            assert_eq!(
                MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(Some(&a), 20), policy),
                Confirmed { was_stale: false },
                "{policy:?}: the applied record again"
            );
            assert_eq!(
                MappingUpdate::decide(Some(&on_a(false, None)), &naming(Some(&a), 1), policy),
                Confirmed { was_stale: false },
                "{policy:?}: no clock takes any record"
            );
        }
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(true, Some(20))), &naming(Some(&b), 20), Disabled),
            Switched {
                from: a.to_string(),
                to: b.to_string(),
                was_stale: true,
            },
            "a record the other policy only marked stale is followed"
        );
        assert_eq!(
            MappingUpdate::decide(
                Some(&on_a(false, Some(20))),
                &naming(Some(&b), 30),
                Disabled
            ),
            Switched {
                from: a.to_string(),
                to: b.to_string(),
                was_stale: false,
            },
            "a newer record naming another homeserver"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(true, Some(20))), &PkdnsRecord::Missing, Disabled),
            NoRecord { was_stale: true },
            "a missing record cannot be dated and changes nothing"
        );
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
            record_ts: Some(1),
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
                Switched {
                    from: a.to_string(),
                    to: b.to_string(),
                    was_stale: false,
                },
                on_a.clone(),
                Some(b.to_string()),
            ),
            (
                NoRecord { was_stale: true },
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
        ];
        for (update, stored, expected) in cases {
            assert_eq!(
                update.homeserver_after(stored.as_ref()),
                expected,
                "{update:?}"
            );
            assert_eq!(
                update.bound_to().map(str::to_string),
                match update {
                    Bound { .. } | Switched { .. } => expected.clone(),
                    _ => None,
                },
                "{update:?}"
            );
        }
    }
}
