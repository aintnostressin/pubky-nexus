use async_trait::async_trait;
use tracing::info;

use crate::migrations::{manager::Migration, utils::delete_keys_by_pattern};
use nexus_common::types::DynError;

/// The read-through cache of post counts: `Post:Counts:{author_id}:{post_id}`.
const RESET_PATTERN: &str = "Post:Counts:*";
const SCAN_COUNT: usize = 1000;

/// Deletes every cached post counts key, so each is refilled with its TTL.
///
/// Keys written before the read-through cache carried a TTL never expire, and
/// the watcher no longer invalidates any, so without this they would serve
/// their counts forever. The next read of a post recomputes its counts from
/// the graph and caches them with `POST_COUNTS_TTL_SECS`. Run it after this
/// change's watcher and API are deployed. Safe to re-run.
pub struct PostCountsKeysReset1792650906;

#[async_trait]
impl Migration for PostCountsKeysReset1792650906 {
    fn id(&self) -> &'static str {
        "PostCountsKeysReset1792650906"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let deleted = delete_keys_by_pattern(RESET_PATTERN, SCAN_COUNT).await?;
        info!(
            deleted,
            "PostCountsKeysReset: deleted cached post counts keys from Redis"
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
