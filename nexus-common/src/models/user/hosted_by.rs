//! A user's binding to the homeserver nexus indexes them from: the
//! `(:User)-[:HOSTED_BY]->(:Homeserver)` edge, with the record clock
//! `hs_record_ts` kept on the `User` node beside it.

use crate::db::{exec_single_row, queries, GraphResult};

/// Binds a user to their homeserver without a PKDNS record in hand, as the
/// ingestor does, or refreshes `resolved_at` on an existing binding. The record
/// clock and the `catch_up` flag are left as they are.
pub async fn set_user_homeserver(user_id: &str, homeserver_id: &str) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver(
        user_id,
        homeserver_id,
        None,
        false,
    ))
    .await
}

/// Binds a user to the homeserver their PKDNS record names, advancing the record
/// clock to the record's timestamp (microseconds) when it is newer. `catch_up`
/// flags the edge for the per-user catch-up reader: set on a switch to the
/// primary homeserver, whose bulk stream has already passed the user's events.
pub async fn set_user_homeserver_from_record(
    user_id: &str,
    homeserver_id: &str,
    record_ts: i64,
    catch_up: bool,
) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver(
        user_id,
        homeserver_id,
        Some(record_ts),
        catch_up,
    ))
    .await
}

/// Refreshes `resolved_at` on a user's binding without changing it. `record_ts`
/// advances the record clock when newer; `None` leaves it alone.
pub async fn refresh_user_homeserver(user_id: &str, record_ts: Option<i64>) -> GraphResult<()> {
    exec_single_row(queries::put::refresh_user_homeserver(user_id, record_ts)).await
}

/// Sets or clears the `catch_up` flag on a user's binding to `homeserver_id`.
pub async fn set_user_homeserver_catch_up(
    user_id: &str,
    homeserver_id: &str,
    catch_up: bool,
) -> GraphResult<()> {
    exec_single_row(queries::put::set_user_homeserver_catch_up(
        user_id,
        homeserver_id,
        catch_up,
    ))
    .await
}
