use crate::db::graph::exec::execute_graph_operation;
use crate::db::kv::RedisResult;
use crate::db::{queries, GraphResult, OperationOutcome, RedisOps};
use crate::models::error::ModelResult;
use crate::models::tag::traits::collection::MAX_TAG_PAGE;
use crate::models::tag::traits::{fetch_tag_details, TaggersCollection};
use crate::models::tag::TagDetails;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
pub struct TagResource(pub Vec<String>);

impl AsRef<[String]> for TagResource {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

#[async_trait]
impl RedisOps for TagResource {
    async fn prefix() -> String {
        String::from("Resource:Taggers")
    }
}

impl TaggersCollection for TagResource {}

impl TagResource {
    /// Tags on a resource, read from the graph on every call: unlike user and
    /// post tags, there is no `TagCollection` index to serve stale or rebuild.
    /// Returns `None` only when the resource does not exist.
    /// `skip_tags`/`limit_tags`/`limit_taggers` default to 0 / 5 / 5 and are
    /// capped at `MAX_TAG_PAGE`, as for user and post tags.
    pub async fn get_by_id(
        resource_id: &str,
        skip_tags: Option<usize>,
        limit_tags: Option<usize>,
        limit_taggers: Option<usize>,
        viewer_id: Option<&str>,
    ) -> ModelResult<Option<Vec<TagDetails>>> {
        let query = queries::get::resource_tags(
            resource_id,
            viewer_id,
            skip_tags.unwrap_or(0),
            limit_tags.unwrap_or(5).min(MAX_TAG_PAGE),
            limit_taggers.unwrap_or(5).min(MAX_TAG_PAGE),
        );
        Ok(fetch_tag_details(query).await?)
    }

    /// Adds `tagger_id` to the label's tagger set, scoped to `app` when given:
    /// `Resource:Taggers:{resource_id}[:{app}]:{label}`.
    pub async fn add_tagger_to_index(
        resource_id: &str,
        app: Option<&str>,
        tagger_id: &str,
        label: &str,
    ) -> RedisResult<()> {
        let key = Self::create_label_index(resource_id, app, label, false);
        Self::put_index_set(&key, &[tagger_id], None, None).await
    }

    /// Creates or merges a TAGGED relationship between a user and a Resource node.
    /// Unlike Post/User tags which use MATCH (target must exist), this uses MERGE
    /// for the Resource (first tag creates it).
    #[allow(clippy::too_many_arguments)]
    pub async fn put_to_graph_resource(
        tagger_id: &str,
        resource_id: &str,
        uri: &str,
        scheme: &str,
        app: &str,
        tag_id: &str,
        label: &str,
        indexed_at: i64,
    ) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_resource_tag(
            tagger_id,
            resource_id,
            uri,
            scheme,
            app,
            tag_id,
            label,
            indexed_at,
        );
        execute_graph_operation(query).await
    }
}
