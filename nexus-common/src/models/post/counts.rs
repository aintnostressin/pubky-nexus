use crate::db::kv::RedisResult;
use crate::db::{fetch_row_from_graph, queries, GraphResult, RedisOps};
use crate::models::error::ModelResult;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::PostStream;

/// Lifetime of the read-through cache: the most a read can trail the graph.
pub const POST_COUNTS_TTL_SECS: i64 = 600;

/// Represents total counts of relationships of a user.
#[derive(Serialize, Deserialize, ToSchema, Default, Debug)]
pub struct PostCounts {
    // how many times was pointed the post with a tag
    pub tags: u32,
    // Distinct tags where the post was referenced
    pub unique_tags: u32,
    pub replies: u32,
    pub reposts: u32,
    // Defaulted so counts cached before this field existed still deserialize.
    #[serde(default)]
    pub collections: u32,
}

impl RedisOps for PostCounts {}

impl PostCounts {
    /// Retrieves counts by user ID, first trying to get from Redis, then from Neo4j if not found.
    pub async fn get_by_id(author_id: &str, post_id: &str) -> ModelResult<Option<PostCounts>> {
        match Self::get_from_index(author_id, post_id).await? {
            Some(counts) => Ok(Some(counts)),
            None => {
                let graph_response = Self::get_from_graph(author_id, post_id).await?;
                if let Some((post_counts, _is_reply)) = graph_response {
                    // Cache miss: populate from the graph via cache_json (JSON only).
                    post_counts.cache_json(author_id, post_id).await?;
                    return Ok(Some(post_counts));
                }
                Ok(None)
            }
        }
    }

    pub async fn get_from_index(author_id: &str, post_id: &str) -> RedisResult<Option<PostCounts>> {
        Self::try_from_index_json(&[author_id, post_id], None).await
    }

    /// Retrieves the counts from Neo4j.
    pub async fn get_from_graph(
        author_id: &str,
        post_id: &str,
    ) -> GraphResult<Option<(PostCounts, bool)>> {
        let query = queries::get::post_counts(author_id, post_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        if let Some(row) = maybe_row {
            let post_exists: bool = row.get("exists").unwrap_or(false);
            if post_exists {
                let counts: PostCounts = row.get("counts")?;
                let is_reply: bool = row.get("is_reply").unwrap_or(false);

                return Ok(Some((counts, is_reply)));
            }
        }
        Ok(None)
    }

    /// Read-through cache write. Nothing invalidates it, so a cached entry may
    /// trail the graph by up to [`POST_COUNTS_TTL_SECS`].
    pub async fn cache_json(&self, author_id: &str, post_id: &str) -> RedisResult<()> {
        self.put_index_json(&[author_id, post_id], None, Some(POST_COUNTS_TTL_SECS))
            .await
    }

    /// Re-seeds a root post's global engagement score from the graph. Replies
    /// are tracked via POST_REPLIES sets instead.
    pub async fn reindex_engagement(author_id: &str, post_id: &str) -> ModelResult<()> {
        match Self::get_from_graph(author_id, post_id).await? {
            Some((counts, false)) => {
                PostStream::add_to_engagement_sorted_set(&counts, author_id, post_id).await?
            }
            Some((_, true)) => {}
            None => tracing::error!(
                "{}:{} Could not found post counts in the graph",
                author_id,
                post_id
            ),
        }
        Ok(())
    }
}
