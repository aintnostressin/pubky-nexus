use async_trait::async_trait;
use futures::StreamExt;

use crate::migrations::{manager::Migration, utils::delete_keys_by_pattern};
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_common::types::DynError;
use tracing::info;

/// Migrate from the `[DELETED]` name sentinel to a boolean `deleted` property.
///
/// # What this does
/// - Sets `u.deleted = true` on every `:User` node whose `name` is exactly `'[DELETED]'`.
/// - Sets `u.deleted = false` on every other `:User` node so the property is present
///   everywhere (enables dropping `coalesce()` and adding an index later).
///
/// # Redis cache strategy
/// We do NOT proactively invalidate cached `UserDetails` JSON in Redis. The
/// deserializer for `deleted` already tolerates a missing key (deserializes
/// through `Option<bool>` → `false`). Stale cache entries without the field
/// will read as `deleted: false` and be naturally evicted on the next profile
/// write. The worst case is a briefly-visible deleted user until the cache
/// refreshes, which is acceptable for a one-shot migration.
///
/// # Idempotency
/// Safe to re-run. The queries are SET operations that overwrite the same
/// values on each pass.
pub struct UserDeletedFlag1780617600;

#[async_trait]
impl Migration for UserDeletedFlag1780617600 {
    fn id(&self) -> &'static str {
        "UserDeletedFlag1780617600"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let graph = get_neo4j_graph()?;

        // 1. Mark sentinel tombstones as deleted: true
        let mut total_marked: i64 = 0;
        loop {
            let query = Query::new(
                "user_deleted_flag_sentinel",
                "MATCH (u:User) WHERE u.name = '[DELETED]' SET u.deleted = true WITH u LIMIT 10000 RETURN count(u) AS processed",
            );
            let mut result = graph.execute(query).await?;

            let processed: i64 = match result.next().await {
                Some(Ok(row)) => row.get::<i64>("processed").unwrap_or(0),
                Some(Err(e)) => return Err(e.into()),
                None => 0,
            };

            total_marked += processed;

            if processed == 0 {
                break;
            }

            info!(
                "UserDeletedFlag migration: marked {} sentinel users as deleted ({} total)",
                processed, total_marked
            );
        }

        info!(
            "UserDeletedFlag migration: marked {} users with name '[DELETED]' as deleted: true",
            total_marked
        );

        // 2. Set deleted: false on every other user so the property is present everywhere
        let mut total_cleared: i64 = 0;
        loop {
            let query = Query::new(
                "user_deleted_flag_clear",
                "MATCH (u:User) WHERE u.name <> '[DELETED]' SET u.deleted = false WITH u LIMIT 10000 RETURN count(u) AS processed",
            );
            let mut result = graph.execute(query).await?;

            let processed: i64 = match result.next().await {
                Some(Ok(row)) => row.get::<i64>("processed").unwrap_or(0),
                Some(Err(e)) => return Err(e.into()),
                None => 0,
            };

            total_cleared += processed;

            if processed == 0 {
                break;
            }

            info!(
                "UserDeletedFlag migration: cleared {} users ({} total)",
                processed, total_cleared
            );
        }

        info!(
            "UserDeletedFlag migration: set deleted: false on {} users",
            total_cleared
        );

        // 3. Invalidate UserDetails cache entries in Redis to force a graph re-read.
        // This avoids the brief window where stale cache reads deleted: false for
        // migrated tombstones. We scan all cached UserDetails keys and delete them.
        // The cache will be repopulated from the graph on next access.
        let deleted = delete_keys_by_pattern("Users:Details:*", 100).await?;
        info!(
            "UserDeletedFlag migration: invalidated {} UserDetails cache entries",
            deleted
        );

        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
