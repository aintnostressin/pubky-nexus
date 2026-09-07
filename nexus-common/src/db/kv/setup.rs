use crate::db::kv::RedisResult;
use crate::models::post::create_post_content_index;

/// Bootstrap Redis with the required RediSearch indexes.
///
/// This bootstraps fresh environments and any environment that has just been
/// flushed. `FLUSHDB` destroys the FT index along with the keys, so `setup_cache`
/// must be re-runnable after every flush.
///
/// Unlike `setup_graph`, this is deliberately NOT wrapped in a `OnceCell`.
/// Neo4j's `MATCH (n) DETACH DELETE n` preserves indexes and constraints, so
/// the graph DDL can safely run once per process. `FLUSHDB`, in contrast,
/// destroys the FT index along with the keys, so `setup_cache` must be
/// re-runnable after every flush.
///
/// `FT.CREATE` does not reconcile an existing index: if `postContentIdx` is
/// already present, the command is swallowed and the on-disk schema is left
/// untouched. Editing this file therefore has no effect on environments that
/// already have the index; schema changes must be delivered through a migration.
/// Index migrations should be additive (`FT.ALTER SCHEMA ADD`) wherever the
/// change allows it. A drop+create migration registered after this bootstrap
/// will overwrite what it creates on a fresh environment, so any such migration
/// must be the last writer for that index and must match the declaration here.
pub async fn setup_cache() -> RedisResult<()> {
    create_post_content_index().await
}
