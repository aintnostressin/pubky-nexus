//! A user's `HOSTED_BY` binding and how a resolved `_pubky` record is applied
//! to it: decided by [`MappingUpdate::decide`], written by [`MappingUpdate::apply`].

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
    /// The published homeserver is missing or another one; the mapping is kept
    /// and marked stale.
    MarkedStale {
        reason: StaleReason,
        was_stale: bool,
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
        match resolved {
            Some(hs_id) if hs_id.as_ref() == stored.hs_id => Self::Confirmed { was_stale },
            resolved => Self::MarkedStale {
                reason: match resolved {
                    None => StaleReason::Unresolved,
                    Some(_) => StaleReason::HsChanged,
                },
                was_stale,
            },
        }
    }

    /// Writes the update to `user_id`'s mapping, keeping `record`, the record
    /// it was decided from, on the edge when there is one. The user node must
    /// exist. Every variant but [`Self::Unbound`] refreshes `resolved_at`.
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
                warn!(%user_id, ?reason, "User homeserver changed or was removed; switching unsupported, mapping marked stale");
                set_user_homeserver_stale(user_id, true, record).await
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
        let naming = |hs| PkdnsRecord::Published(signed_packet_naming(&user, Some(hs), 1));
        let naming_none = || PkdnsRecord::Published(signed_packet_naming(&user, None, 1));
        let on_a = |stale| HomeserverMapping {
            hs_id: a.to_string(),
            stale,
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
                Some(on_a(false)),
                PkdnsRecord::Missing,
                marked_stale(Unresolved, false),
            ),
            (
                Some(on_a(true)),
                PkdnsRecord::Missing,
                marked_stale(Unresolved, true),
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
}
