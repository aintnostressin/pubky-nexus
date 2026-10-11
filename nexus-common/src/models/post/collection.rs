use super::{PostCounts, PostDetails};
use crate::db::{fetch_key_from_graph, queries};
use crate::models::error::ModelResult;
use pubky_social_specs::PubkyId;

/// A collection item as `item_keys` stores it: `author_id:post_id`.
pub(crate) fn item_key((author_id, post_id): &(PubkyId, String)) -> String {
    format!("{author_id}:{post_id}")
}

/// The posts a collection curates, in curator order, as its graph write stored them. Empty for a
/// post that is not a collection or is not indexed.
pub async fn get_collection_items(
    author_id: &str,
    post_id: &str,
) -> ModelResult<Vec<(PubkyId, String)>> {
    let query = queries::get::collection_item_keys(author_id, post_id);
    let keys = fetch_key_from_graph::<Vec<String>>(query, "item_keys").await?;
    Ok(keys
        .unwrap_or_default()
        .iter()
        .filter_map(|key| {
            let (item_author_id, item_post_id) = key.split_once(':')?;
            let item_author_id = PubkyId::try_from(item_author_id).ok()?;
            Some((item_author_id, item_post_id.to_string()))
        })
        .collect())
}

/// Reconciles the COLLECTED edges of `author_id:post_id` with `items` and
/// invalidates the counts of every item the graph reports as touched.
/// Idempotent, so any retry path may call it freely. `derived_from` is the post
/// state `items` came from; see [`queries::put::sync_collection_items`].
pub async fn sync_collected_edges(
    author_id: &str,
    post_id: &str,
    items: &[(PubkyId, String)],
    derived_from: Option<&PostDetails>,
) -> ModelResult<()> {
    let query = queries::put::sync_collection_items(author_id, post_id, items, derived_from);
    // No row: the post is not in the graph, or it moved on since `items` were
    // resolved, so there is nothing to reconcile.
    let Some(touched) = fetch_key_from_graph::<Vec<Vec<String>>>(query, "touched").await? else {
        return Ok(());
    };
    let keys: Vec<Vec<&str>> = touched
        .iter()
        .map(|key| key.iter().map(String::as_str).collect())
        .collect();
    let keys: Vec<&[&str]> = keys.iter().map(Vec::as_slice).collect();
    PostCounts::invalidate_many(&keys).await?;
    Ok(())
}
