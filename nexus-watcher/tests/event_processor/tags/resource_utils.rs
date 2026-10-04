use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::graph::Query;
use nexus_common::db::{fetch_key_from_graph, get_redis_conn, RedisOps};
use nexus_common::models::resource::stream::ResourceStream;
use nexus_common::models::resource::tag::RESOURCE_TAGS_KEY_PARTS;
use serde::{Deserialize, Serialize};

/// Graph query result for a Resource tag
#[derive(Serialize, Deserialize, Debug)]
pub struct ResourceTagResult {
    pub label: String,
    pub app: Option<String>,
    pub tagger: String,
    pub uri: String,
    pub scheme: String,
}

/// Find a Resource tag in the graph database
pub async fn find_resource_tag(
    resource_id: &str,
    label: &str,
) -> Result<Option<ResourceTagResult>> {
    let query = resource_tag_query(resource_id, label);
    let result = fetch_key_from_graph(query, "details").await.unwrap();
    Ok(result)
}

/// Check if a Resource node exists in the graph
pub async fn resource_exists_in_graph(resource_id: &str) -> Result<bool> {
    let query = Query::new(
        "resource_exists",
        "
        OPTIONAL MATCH (r:Resource {id: $resource_id})
        RETURN r IS NOT NULL AS exists
        ",
    )
    .param("resource_id", resource_id);
    let result: Option<bool> = fetch_key_from_graph(query, "exists").await.unwrap();
    Ok(result.unwrap_or(false))
}

/// Count TAGGED relationships pointing to a Resource
pub async fn count_resource_tags(resource_id: &str) -> Result<i64> {
    let query = Query::new(
        "count_resource_tags",
        "
        MATCH (:User)-[t:TAGGED]->(:Resource {id: $resource_id})
        RETURN count(t) AS tag_count
        ",
    )
    .param("resource_id", resource_id);
    let result: Option<i64> = fetch_key_from_graph(query, "tag_count").await.unwrap();
    Ok(result.unwrap_or(0))
}

/// Check if a resource_id is a member of a Redis sorted set
pub async fn check_resource_in_sorted_set(
    key_parts: &[&str],
    resource_id: &str,
) -> Result<Option<isize>> {
    let score = ResourceStream::check_sorted_set_member(None, key_parts, &[resource_id])
        .await
        .unwrap();
    Ok(score)
}

/// Compute the resource_id for a given URI (for test assertions)
pub fn compute_resource_id(uri: &str) -> String {
    let (normalized, _) = nexus_common::universal_tag::normalize::normalize_uri(uri).unwrap();
    nexus_common::universal_tag::normalize::resource_id(&normalized)
}

/// Whether the retired label-score sorted set of a resource,
/// `Sorted:Resources:Tag:{resource_id}`, exists in Redis
pub async fn resource_label_scores_exist(resource_id: &str) -> Result<bool> {
    let key = format!("Sorted:{}:{resource_id}", RESOURCE_TAGS_KEY_PARTS.join(":"));
    let mut redis_conn = get_redis_conn().await?;
    Ok(redis_conn.exists(key).await?)
}

fn resource_tag_query(resource_id: &str, label: &str) -> Query {
    Query::new(
        "resource_tag_query",
        "
        MATCH (tagger:User)-[t:TAGGED {label: $label}]->(r:Resource {id: $resource_id})
        RETURN {
            label: t.label,
            app: t.app,
            tagger: tagger.id,
            uri: r.uri,
            scheme: r.scheme
        } AS details
        ",
    )
    .param("resource_id", resource_id)
    .param("label", label)
}
