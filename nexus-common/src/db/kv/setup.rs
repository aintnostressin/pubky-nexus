use crate::db::kv::RedisResult;
use crate::models::post::create_post_content_index;
use tracing::info;

/// Ensure Redis has the required RediSearch indexes.
///
/// This is the Redis counterpart to [`crate::db::setup::setup_graph`]: the single
/// declaration of the search schema that must exist in every environment, and the
/// place where future RediSearch indexes get added.
///
/// # There is deliberately no `OnceCell` here
///
/// `setup_graph` may run once per process because Neo4j's
/// `MATCH (n) DETACH DELETE n` wipes data but preserves constraints and indexes.
/// Redis has no such separation: `FLUSHDB` destroys the FT index along with the
/// keys, so this function must stay re-runnable and be called again after every
/// flush (see `crate::db::kv::clear_redis`). Wrapping it in a `OnceCell` would
/// reintroduce the bug where `nexus-webapi/benches/reindex.rs` flushes the cache
/// on each criterion iteration and never gets the index back.
///
/// Safe to call during connector init: `PostDetails::prefix()` is derived from the
/// type name and needs no live connection.
pub async fn setup_cache() -> RedisResult<()> {
    create_post_content_index().await?;

    info!("RediSearch indexes have been applied successfully");

    Ok(())
}
