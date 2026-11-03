//! A user's `HOSTED_BY` binding and how a resolved `_pubky` record is applied
//! to it: decided by [`MappingUpdate::decide`], written by [`MappingUpdate::apply`].
//!
//! The record a mapping was last applied from is kept on the edge, and a
//! record older than it is a replay or a lagging answer: it changes nothing,
//! whatever it names. No record at all says nothing and changes nothing; a
//! record naming no homeserver is a change the key holder signed, and marks
//! the mapping stale.

use tracing::{debug, warn};

use crate::db::{exec_single_row, fetch_row_from_graph, queries, GraphResult};
use crate::models::user::{PkdnsRecord, StoredRecord};

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
    /// The record names no homeserver, or another one; the mapping is kept and
    /// marked stale.
    MarkedStale {
        reason: StaleReason,
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
    /// What the record does to the stored mapping.
    pub fn decide(stored: Option<&HomeserverMapping>, record: &PkdnsRecord) -> Self {
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
        match resolved {
            Some(hs_id) if hs_id.as_ref() == stored.hs_id => Self::Confirmed { was_stale },
            // A signed statement that the user has no homeserver now.
            None => Self::MarkedStale {
                reason: StaleReason::Unresolved,
                was_stale,
            },
            Some(_) => Self::MarkedStale {
                reason: StaleReason::HsChanged,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::{random_pubky_id, signed_packet_naming};
    use pubky::Keypair;

    /// Every combination of stored mapping and record maps to one update.
    #[test]
    fn decide_covers_every_case() {
        use MappingUpdate::*;
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
        let cases = [
            (None, PkdnsRecord::Missing, Unbound),
            (None, naming_none(), Unbound),
            (None, naming(&a), Bound { to: a.to_string() }),
            (
                Some(on_a(false)),
                naming(&a),
                Confirmed { was_stale: false },
            ),
            (Some(on_a(true)), naming(&a), Confirmed { was_stale: true }),
            (
                Some(on_a(false)),
                naming(&b),
                marked_stale(HsChanged, false),
            ),
            (Some(on_a(true)), naming(&b), marked_stale(HsChanged, true)),
            (
                Some(on_a(false)),
                naming_none(),
                marked_stale(Unresolved, false),
            ),
            (
                Some(on_a(true)),
                naming_none(),
                marked_stale(Unresolved, true),
            ),
            (
                Some(on_a(false)),
                PkdnsRecord::Missing,
                NoRecord { was_stale: false },
            ),
            (
                Some(on_a(true)),
                PkdnsRecord::Missing,
                NoRecord { was_stale: true },
            ),
        ];
        for (stored, record, expected) in cases {
            assert_eq!(
                MappingUpdate::decide(stored.as_ref(), &record),
                expected,
                "stored {stored:?}, record names {:?}",
                record.homeserver()
            );
        }
    }

    /// A record older than the one the mapping was applied from is ignored,
    /// whatever it names; the same record again, or a newer one, is applied.
    /// A mapping without a clock takes any record.
    #[test]
    fn decide_ignores_records_older_than_the_applied_one() {
        use MappingUpdate::*;

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

        assert_eq!(
            MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(Some(&b), 10)),
            ignored(false, true),
            "an older record naming another homeserver"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(true, Some(20))), &naming(Some(&a), 10)),
            ignored(true, true),
            "an older record naming the stored homeserver does not clear the flag"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(None, 10)),
            ignored(false, false),
            "an older record naming none"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(Some(&a), 20)),
            Confirmed { was_stale: false },
            "the applied record again"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(false, None)), &naming(Some(&a), 1)),
            Confirmed { was_stale: false },
            "no clock takes any record"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(false, Some(20))), &naming(Some(&b), 30)),
            MarkedStale {
                reason: StaleReason::HsChanged,
                was_stale: false,
            },
            "a newer record naming another homeserver"
        );
        assert_eq!(
            MappingUpdate::decide(Some(&on_a(true, Some(20))), &PkdnsRecord::Missing),
            NoRecord { was_stale: true },
            "a missing record cannot be dated and changes nothing"
        );
    }
}
