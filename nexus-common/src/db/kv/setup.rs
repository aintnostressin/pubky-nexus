use crate::db::kv::RedisResult;
use crate::models::post::create_post_content_index;

/// Ensure Redis has the required RediSearch indexes.
///
/// Redis's equivalent of [`crate::db::graph::setup::setup_graph`]: the single
/// declaration of the indexes that must exist in every environment. Future
/// RediSearch indexes get added here.
///
/// Unlike `setup_graph`, this is deliberately NOT wrapped in a `OnceCell`.
/// Neo4j's `MATCH (n) DETACH DELETE n` preserves indexes and constraints, so
/// the graph DDL can safely run once per process. `FLUSHDB`, in contrast,
/// destroys the FT index along with the keys, so `setup_cache` must be
/// re-runnable after every flush. A `OnceCell` here would reintroduce the
/// bench bug where a `MockDb::drop_cache()` in a criterion loop left the
/// environment silently without full-text search.
///
/// `PostDetails::prefix()` is derived from the type name and needs no live
/// connection, so this is safe to call during connector init.
pub async fn setup_cache() -> RedisResult<()> {
    create_post_content_index().await
}
