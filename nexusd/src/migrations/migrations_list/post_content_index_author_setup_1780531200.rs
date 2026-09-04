/// Adds $.author TAG CASESENSITIVE + $.kind TAG CASESENSITIVE to the post content full-text index.
///
/// # Deployment expectation: stop → migrate → start
///
/// Between `drop_post_content_index_v2` and `create_post_content_index_v2` the
/// index is absent, so global content search returns empty results. After
/// FT.CREATE the PREFIX clause triggers a background scan that indexes all
/// existing PostDetails JSON documents against the new schema — both `author`
/// and `kind` fields are already present in every document, so no re-persistence
/// is needed. For zero-downtime deployments, schedule this migration during a
/// maintenance window or accept a brief gap where search is unavailable.
use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::{db::get_redis_conn, db::RedisOps, models::post::PostDetails, types::DynError};
use tracing::info;

pub struct PostContentIndexAuthorSetup1780531200;

const POST_CONTENT_INDEX: &str = "postContentIdx";

/// Drops the existing post content index without deleting the underlying documents.
/// Idempotent: no-ops if the index is already absent.
async fn drop_post_content_index_v2() -> Result<(), DynError> {
    let mut conn = get_redis_conn().await?;

    let result = redis::cmd("FT.DROPINDEX")
        .arg(POST_CONTENT_INDEX)
        .query_async::<()>(&mut conn)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().to_lowercase().contains("unknown index name") => {
            info!("RediSearch index '{POST_CONTENT_INDEX}' already absent");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Creates the v2 post content index: $.content TEXT + $.author TAG CASESENSITIVE + $.kind TAG CASESENSITIVE.
/// Includes NOOFFSETS, NOHL. The FT.CREATE arg list is frozen in this migration so it keeps
/// producing the exact v2 schema it shipped with, even if the live `setup_cache` schema evolves.
/// Idempotent: no-ops if the index already exists.
async fn create_post_content_index_v2() -> Result<(), DynError> {
    let prefix = format!("{}:", PostDetails::prefix().await);
    let mut conn = get_redis_conn().await?;

    let result = redis::cmd("FT.CREATE")
        .arg(POST_CONTENT_INDEX)
        .arg("ON")
        .arg("JSON")
        .arg("PREFIX")
        .arg("1")
        .arg(&prefix)
        .arg("NOOFFSETS")
        .arg("NOHL")
        .arg("SCHEMA")
        .arg("$.content")
        .arg("AS")
        .arg("content")
        .arg("TEXT")
        .arg("$.author")
        .arg("AS")
        .arg("author")
        .arg("TAG")
        .arg("CASESENSITIVE")
        .arg("$.kind")
        .arg("AS")
        .arg("kind")
        .arg("TAG")
        .arg("CASESENSITIVE")
        .query_async::<()>(&mut conn)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("already exists") => {
            info!("RediSearch index '{POST_CONTENT_INDEX}' already exists");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[async_trait]
impl Migration for PostContentIndexAuthorSetup1780531200 {
    fn id(&self) -> &'static str {
        "PostContentIndexAuthorSetup1780531200"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        // Drop existing index (idempotent — no-ops if already absent).
        drop_post_content_index_v2().await?;
        info!("Dropped post content index for schema upgrade");

        // Recreate with the new schema ($.content TEXT + $.author TAG + $.kind TAG).
        // FT.CREATE with PREFIX triggers a background scan that indexes all existing
        // PostDetails JSON documents against the new schema — both `author` and `kind`
        // fields are already present in every document, so no re-persistence is needed.
        create_post_content_index_v2().await?;
        info!("Recreated post content index with author and kind fields");

        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
