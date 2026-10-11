use crate::db::kv::RedisResult;
use crate::db::{fetch_row_from_graph, queries, GraphResult, RedisOps};
use crate::models::error::ModelResult;
use pubky_social_specs::PubkyId;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The posts a post's graph write links it to, each as `(author_id, post_id)`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PostLinks {
    /// The post this one replies to.
    pub replied: Option<(PubkyId, String)>,
    /// The post this one reposts.
    pub reposted: Option<(PubkyId, String)>,
    /// For a collection, the posts it curates, in curator order; `None` for any other kind.
    pub collected: Option<Vec<(PubkyId, String)>>,
}

#[derive(Serialize, Deserialize, ToSchema, Default, Debug)]
pub struct PostRelationships {
    /// If set, URI of the post this is a reply to
    pub replied: Option<String>,

    /// If set, URI of the post this post is reposting
    pub reposted: Option<String>,

    /// List of user IDs mentioned in this post
    pub mentioned: Vec<PubkyId>,
}

impl RedisOps for PostRelationships {}

impl PostRelationships {
    /// Retrieves post relationships by user ID, first trying to get from Redis, then from Neo4j if not found.
    pub async fn get_by_id(
        author_id: &str,
        post_id: &str,
    ) -> ModelResult<Option<PostRelationships>> {
        match Self::get_from_index(author_id, post_id).await? {
            Some(counts) => Ok(Some(counts)),
            None => {
                let graph_response = Self::get_from_graph(author_id, post_id).await?;
                if let Some(post_relationships) = graph_response {
                    post_relationships.put_to_index(author_id, post_id).await?;
                    return Ok(Some(post_relationships));
                }
                Ok(None)
            }
        }
    }

    pub async fn get_from_index(
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<Option<PostRelationships>> {
        Self::try_from_index_json(&[author_id, post_id], None).await
    }

    /// Retrieves the counts from Neo4j.
    pub async fn get_from_graph(
        author_id: &str,
        post_id: &str,
    ) -> GraphResult<Option<PostRelationships>> {
        let query = queries::get::post_relationships(author_id, post_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        let Some(row) = maybe_row else {
            return Ok(None);
        };

        let replied = row.get::<Option<String>>("replied_uri").unwrap_or(None);
        let reposted = row.get::<Option<String>>("reposted_uri").unwrap_or(None);
        let mentioned: Vec<PubkyId> = row.get("mentioned_user_ids").unwrap_or(Vec::new());

        Ok(Some(Self {
            replied,
            reposted,
            mentioned,
        }))
    }

    pub async fn put_to_index(&self, author_id: &str, post_id: &str) -> RedisResult<()> {
        self.put_index_json(&[author_id, post_id], None, None).await
    }

    pub async fn delete(author_id: &str, post_id: &str) -> RedisResult<()> {
        Self::remove_from_index_multiple_json(&[&[author_id, post_id]]).await
    }

    pub async fn reindex(author_id: &str, post_id: &str) -> ModelResult<()> {
        match Self::get_from_graph(author_id, post_id).await? {
            Some(relationships) => relationships.put_to_index(author_id, post_id).await?,
            None => tracing::error!(
                "{}:{} Could not found post relationships in the graph",
                author_id,
                post_id
            ),
        }
        Ok(())
    }
}
