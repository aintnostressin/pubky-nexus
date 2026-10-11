use async_trait::async_trait;
use std::time::Instant;
use tracing::{info, warn};

use crate::migrations::manager::Migration;
use nexus_common::{
    db::{exec_single_row, fetch_all_rows_from_graph, queries},
    models::post::{PostDetails, PostKind},
    types::DynError,
};
use nexus_watcher::events::collection_item_keys;

const PROGRESS_LOG_EVERY: u64 = 500;

/// Stores `item_keys` on every collection indexed before the watcher stored them.
///
/// # What this does
/// The collection stream reads a collection's items from `item_keys`: the keys of the posts its
/// envelope lists, `author_id:post_id` in curator order, which the watcher resolves when it
/// indexes the collection. A collection indexed before has none and reads as empty. This resolves
/// each stored envelope the way the watcher does and stores the keys. An envelope that is not one
/// is skipped, so that collection keeps reading as empty, as it did.
///
/// # Idempotency
/// Each write is guarded on the content its keys came from, so a collection edited meanwhile keeps
/// the newer keys the watcher wrote. A rerun writes the same keys again; `is_multi_staged()` is
/// `false`.
///
/// # Deploy ordering
/// Stop every instance, run `nexusd db migration run`, then start the new binaries: the new stream
/// serves an empty item list for a collection without keys.
pub struct CollectionItemKeysBackfill1791676800;

#[async_trait]
impl Migration for CollectionItemKeysBackfill1791676800 {
    fn id(&self) -> &'static str {
        "CollectionItemKeysBackfill1791676800"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        // Only the keys are snapshotted; each envelope is read from the graph right before its
        // write, which the content guard drops if the collection moved on.
        let rows = fetch_all_rows_from_graph(queries::get::get_collection_posts()).await?;

        let started = Instant::now();
        let mut processed: u64 = 0;
        for row in rows {
            let author_id: String = row.get("author_id")?;
            let post_id: String = row.get("post_id")?;
            let fresh = PostDetails::get_from_graph(&author_id, &post_id)
                .await?
                .filter(|(details, _)| details.kind == PostKind::Collection);
            let Some((details, _)) = fresh else {
                continue;
            };
            let keys = match collection_item_keys(&details.content) {
                Ok(keys) => keys,
                Err(e) => {
                    warn!("Collection {author_id}:{post_id} envelope malformed, skipped: {e}");
                    continue;
                }
            };
            exec_single_row(queries::put::set_collection_items(
                &author_id,
                &post_id,
                &details.content,
                &keys,
            ))
            .await?;

            processed += 1;
            if processed.is_multiple_of(PROGRESS_LOG_EVERY) {
                info!(
                    processed,
                    elapsed_secs = format!("{:.1}", started.elapsed().as_secs_f64()),
                    "CollectionItemKeysBackfill progress"
                );
            }
        }

        info!(
            processed,
            elapsed_secs = format!("{:.1}", started.elapsed().as_secs_f64()),
            "CollectionItemKeysBackfill completed"
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

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::db::fetch_key_from_graph;
    use nexus_common::db::graph::Query;
    use nexus_common::{StackConfig, StackManager};

    const AUTHOR_ID: &str = "MigrationTest:CollectionItemKeys:Author";
    const COLLECTION_ID: &str = "MigrationTest:CollectionItemKeys:Collection";
    const MALFORMED_ID: &str = "MigrationTest:CollectionItemKeys:Malformed";
    const ITEM_AUTHOR: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";

    async fn delete_fixture() -> Result<(), DynError> {
        exec_single_row(
            Query::new(
                "collection_item_keys_test_cleanup",
                "MATCH (n) WHERE (n:User AND n.id = $author) OR (n:Post AND n.id IN $posts)
                 DETACH DELETE n",
            )
            .param("author", AUTHOR_ID)
            .param("posts", vec![COLLECTION_ID, MALFORMED_ID]),
        )
        .await?;
        Ok(())
    }

    async fn item_keys(post_id: &str) -> Result<Option<Vec<String>>, DynError> {
        let query = Query::new(
            "collection_item_keys_test_read",
            "MATCH (c:Post {id: $post_id}) RETURN c.item_keys AS item_keys",
        )
        .param("post_id", post_id);
        Ok(
            fetch_key_from_graph::<Option<Vec<String>>>(query, "item_keys")
                .await?
                .flatten(),
        )
    }

    /// A collection indexed before `item_keys` gets the keys of its post items in curator order,
    /// other items dropped; a malformed envelope gets none. A second run changes nothing.
    #[tokio_shared_rt::test(shared)]
    async fn test_backfill_stores_item_keys_in_curator_order() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        delete_fixture().await?;

        let post = |id: &str| format!("pubky://{ITEM_AUTHOR}/pub/pubky.app/posts/{id}");
        let items = [
            post("0032SSN7Q4EVG"),
            format!("pubky://{ITEM_AUTHOR}/pub/pubky.app/profile.json"),
            "https://example.com/not-a-post".to_string(),
            post("0032SSN7Q4EVA"),
        ];
        let items: Vec<String> = items.iter().map(|item| format!("\"{item}\"")).collect();
        let content = format!(
            r#"{{"name":"migration test","items":[{}]}}"#,
            items.join(",")
        );
        exec_single_row(
            Query::new(
                "collection_item_keys_test_seed",
                "CREATE (u:User {id: $author, name: 'migration test'})
                 CREATE (u)-[:AUTHORED]->(:Post {id: $collection, uri: $collection, content: $content, kind: 'collection', indexed_at: 1})
                 CREATE (u)-[:AUTHORED]->(:Post {id: $malformed, uri: $malformed, content: 'not an envelope', kind: 'collection', indexed_at: 1})",
            )
            .param("author", AUTHOR_ID)
            .param("collection", COLLECTION_ID)
            .param("malformed", MALFORMED_ID)
            .param("content", content),
        )
        .await?;

        for run in 1..=2 {
            CollectionItemKeysBackfill1791676800.backfill().await?;

            assert_eq!(
                item_keys(COLLECTION_ID).await?,
                Some(vec![
                    format!("{ITEM_AUTHOR}:0032SSN7Q4EVG"),
                    format!("{ITEM_AUTHOR}:0032SSN7Q4EVA"),
                ]),
                "run {run}"
            );
            assert_eq!(item_keys(MALFORMED_ID).await?, None, "run {run}");
        }

        delete_fixture().await?;
        Ok(())
    }
}
