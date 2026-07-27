use async_trait::async_trait;
use crate::migrations::manager::Migration;
use futures::StreamExt;
use nexus_common::db::graph::Query;
use nexus_common::db::{get_neo4j_graph, RedisOps};
use nexus_common::models::user::UserDetails;
use nexus_common::types::DynError;
use tracing::info;

/// Migrate from the `[DELETED]` name sentinel to a boolean `deleted` property.
///
/// # What this does
/// - Sets `u.deleted = true` on every `:User` node whose `name` is exactly `'[DELETED]'`.
/// - Sets `u.deleted = false` on every other `:User` node so the property is present
///   everywhere (enables dropping `coalesce()` and adding an index later).
/// - Performs targeted per-batch Redis cache invalidation for tombstones only, via the
///   typed `UserDetails::remove_from_index_multiple_json` API. Live users whose cached
///   entry predates the migration lack the `deleted` key and deserialize to `false`
///   through `Option<bool>`, which is already correct.
///
/// # Idempotency
/// Safe to re-run. The `WHERE u.deleted IS NULL` predicate drains on each pass (the `SET`
/// writes `deleted`, so the predicate becomes false). A post-rollout re-run additionally
/// re-catches users tombstoned by pre-cutover code during the deploy window
/// (old `create_user` left `deleted` at whatever the migration wrote while still setting
/// the sentinel name).
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

        let mut total_processed: i64 = 0;
        let mut total_tombstoned: usize = 0;

        loop {
            // Drain predicate: every node this query touches stops matching it, because
            // the SET below always writes `deleted`. The second disjunct additionally
            // re-catches users tombstoned by pre-cutover code during the deploy window
            // (old `create_user` left `deleted` at whatever the migration wrote while
            // still setting the sentinel name), which makes a post-rollout re-run useful.
            //
            // WITH ... LIMIT must precede SET, otherwise SET applies to the whole match
            // and the batch size is meaningless.
            let query = Query::new(
                "user_deleted_flag_backfill",
                "MATCH (u:User)
                 WHERE u.deleted IS NULL OR (u.name = '[DELETED]' AND u.deleted = false)
                 WITH u LIMIT 10000
                 SET u.deleted = (u.name = '[DELETED]')
                 RETURN count(u) AS processed,
                        collect(CASE WHEN u.name = '[DELETED]' THEN u.id ELSE null END) AS tombstoned",
            );

            let mut result = graph.execute(query).await?;

            let (processed, tombstoned): (i64, Vec<String>) = match result.next().await {
                Some(Ok(row)) => (
                    row.get::<i64>("processed").unwrap_or(0),
                    row.get::<Vec<String>>("tombstoned").unwrap_or_default(),
                ),
                Some(Err(e)) => return Err(e.into()),
                None => (0, Vec::new()),
            };

            if processed == 0 {
                break;
            }

            // Invalidate cached UserDetails JSON for tombstones only. Live users whose
            // cache entry predates the migration deserialize the missing key as false,
            // which is already correct.
            if !tombstoned.is_empty() {
                let owned: Vec<Vec<&str>> = tombstoned.iter().map(|id| vec![id.as_str()]).collect();
                let key_parts_list: Vec<&[&str]> = owned.iter().map(|k| k.as_slice()).collect();
                UserDetails::remove_from_index_multiple_json(&key_parts_list).await?;
                total_tombstoned += tombstoned.len();
            }

            total_processed += processed;
            info!(
                "UserDeletedFlag migration: processed {} users this batch ({} total, {} tombstones invalidated)",
                processed, total_processed, total_tombstoned
            );
        }

        info!(
            "UserDeletedFlag migration: complete — {} users backfilled, {} tombstones marked and cache-invalidated",
            total_processed, total_tombstoned
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
