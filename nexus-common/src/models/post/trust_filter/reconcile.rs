//! Bringing the shared sets in line with a published ranking.

use std::collections::{BTreeMap, BTreeSet};

use deadpool_redis::Connection;
use neo4rs::Row;
use redis::{AsyncCommands, Script, ScriptInvocation};

use super::scripts::{add_call, remove_call, ADD, REMOVE};
use super::{applied_key, is_enabled, sorted_key, APPLIED_KEY_PARTS, BATCH};
use crate::db::kv::{RedisError, RedisResult};
use crate::db::{
    fetch_all_rows_from_graph, fetch_key_from_graph, get_redis_conn, queries, GraphResult, RedisOps,
};
use crate::models::error::ModelResult;
use crate::models::post::search::PostsByTagSearch;
use crate::models::post::PostStream;
use crate::models::user::SocialGraphStatus;

/// Posts read from the graph, and written, per page of an author's entries.
const POSTS_PAGE: usize = 1_000;

/// Brings the shared sets in line with the published ranking, then records it
/// as applied. With the filter off the ranking counts as absent, which writes
/// every hidden author back. Takes no lock: [`REMOVE`] spares an author the
/// live ranking admits and [`ADD`] skips one it doesn't, so overlapping runs
/// converge.
pub(crate) async fn reconcile() -> ModelResult<()> {
    let mut conn = get_redis_conn().await?;
    let ranking = match is_enabled() {
        true => members(&mut conn, &SocialGraphStatus::ranking_key()).await?,
        false => None,
    };
    let applied = members(&mut conn, &applied_key()).await?;
    if applied == ranking {
        return Ok(());
    }

    let (hide, show) = rank_changes(applied.as_ref(), ranking.as_ref()).await?;
    tracing::info!(
        hide = hide.len(),
        show = show.len(),
        "Applying a rank change to the shared post sets"
    );
    let hides = hide.iter().map(|author| (author, Write::Hide));
    let shows = show.iter().map(|author| (author, Write::Show));
    for (author, write) in hides.chain(shows) {
        write_author_pages(&mut conn, author, write, POSTS_PAGE)
            .await
            .inspect_err(|error| {
                tracing::error!(author, ?write, %error, "Failed to update an author's shared post entries")
            })?;
    }
    // Last, so a run that fails keeps the old copy and the next one redoes the diff.
    record_applied(ranking.as_ref()).await?;
    tracing::info!("Rank change applied to the shared post sets");
    Ok(())
}

/// The authors to hide and to show when the shared sets go from the `applied`
/// ranking to `ranking`. No ranking takes every author.
async fn rank_changes(
    applied: Option<&BTreeSet<String>>,
    ranking: Option<&BTreeSet<String>>,
) -> ModelResult<(Vec<String>, Vec<String>)> {
    Ok(match (applied, ranking) {
        (None, None) => (Vec::new(), Vec::new()),
        // The first ranking: until now the sets took everyone.
        (None, Some(ranking)) => (authors_outside(ranking).await?, Vec::new()),
        (Some(applied), Some(ranking)) => (
            applied.difference(ranking).cloned().collect(),
            ranking.difference(applied).cloned().collect(),
        ),
        // The ranking is gone: the sets take everyone again.
        (Some(applied), None) => (Vec::new(), authors_outside(applied).await?),
    })
}

/// Records `ranking` as the one the shared sets reflect. `None` deletes the
/// copy, so nothing reads as filtered.
async fn record_applied(ranking: Option<&BTreeSet<String>>) -> RedisResult<()> {
    let applied: Vec<(f64, &str)> = ranking
        .into_iter()
        .flatten()
        .map(|id| (0.0, id.as_str()))
        .collect();
    PostStream::replace_index_sorted_set(&APPLIED_KEY_PARTS, &applied, None, None).await
}

/// The members of the sorted set `key`, or `None` when it doesn't exist.
async fn members(conn: &mut Connection, key: &str) -> RedisResult<Option<BTreeSet<String>>> {
    let members: BTreeSet<String> = conn.zrange(key, 0, -1).await?;
    Ok((!members.is_empty()).then_some(members))
}

/// Every user who wrote a post, ids in `set` aside.
async fn authors_outside(set: &BTreeSet<String>) -> ModelResult<Vec<String>> {
    let query = queries::get::post_author_ids();
    let mut authors: Vec<String> = fetch_key_from_graph(query, "user_ids")
        .await?
        .unwrap_or_default();
    authors.retain(|author| !set.contains(author));
    Ok(authors)
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Write {
    /// Remove the author's entries, unless the live ranking admits them.
    Hide,
    /// Write them back where the live ranking admits them, or everywhere
    /// with the filter off.
    Show,
}

impl Write {
    fn script(self) -> &'static Script {
        match self {
            Write::Hide => &REMOVE,
            Write::Show => &ADD,
        }
    }

    /// The script call that writes, or removes, `batch` in the shared set `key`.
    fn call(self, key: &str, batch: &[(f64, String)]) -> ScriptInvocation<'static> {
        let entries = batch
            .iter()
            .map(|(score, member)| (*score, member.as_str()));
        match self {
            Write::Hide => remove_call(key, entries.map(|(_, member)| member)),
            Write::Show => add_call(key, !is_enabled(), entries),
        }
    }
}

/// Applies `write` to `author`'s entries `page_size` posts at a time, oldest
/// first, so a prolific author needs no single huge query or pipeline.
pub(super) async fn write_author_pages(
    conn: &mut Connection,
    author: &str,
    write: Write,
    page_size: usize,
) -> ModelResult<()> {
    let mut after = (i64::MIN, String::new());
    loop {
        let query = queries::get::author_post_entries(author, (after.0, &after.1), page_size);
        let rows = fetch_all_rows_from_graph(query).await?;
        let posts: Vec<AuthorPost> = rows
            .iter()
            .map(AuthorPost::from_row)
            .collect::<Result<_, _>>()?;
        write_entries(conn, write, &shared_entries(author, &posts)).await?;
        match posts.iter().map(|post| (post.indexed_at, &post.id)).max() {
            Some((indexed_at, id)) if posts.len() >= page_size => after = (indexed_at, id.clone()),
            _ => return Ok(()),
        }
    }
}

/// Applies `write` to `entries` in one pipeline of script calls.
async fn write_entries(
    conn: &mut Connection,
    write: Write,
    entries: &BTreeMap<String, Vec<(f64, String)>>,
) -> ModelResult<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    pipe.load_script(write.script()).ignore();
    for (key, entries) in entries {
        for batch in entries.chunks(BATCH) {
            pipe.invoke_script(&write.call(key, batch)).ignore();
        }
    }
    let _: () = pipe.query_async(conn).await.map_err(RedisError::from)?;
    Ok(())
}

/// One of an author's posts, with what the shared sets index it by.
#[derive(Debug)]
pub(crate) struct AuthorPost {
    pub id: String,
    pub indexed_at: i64,
    /// `(author, post)` of the post it replies to.
    pub parent: Option<(String, String)>,
    /// The labels it is tagged with.
    pub labels: Vec<String>,
    /// Tags, replies and reposts, summed as `PostCounts` sums them.
    pub engagement: i64,
    /// Users it mentions.
    pub mentions: i64,
}

impl AuthorPost {
    fn from_row(row: &Row) -> GraphResult<Self> {
        let parent_author: Option<String> = row.get("parent_author_id")?;
        let parent_post: Option<String> = row.get("parent_post_id")?;
        Ok(AuthorPost {
            id: row.get("post_id")?,
            indexed_at: row.get("indexed_at")?,
            parent: parent_author.zip(parent_post),
            labels: row.get("labels")?,
            engagement: row.get("engagement")?,
            mentions: row.get("mentions")?,
        })
    }
}

/// Every entry `author`'s posts have in the shared sets, by key, as the models
/// that own those sets place them.
pub(super) fn shared_entries(
    author: &str,
    posts: &[AuthorPost],
) -> BTreeMap<String, Vec<(f64, String)>> {
    let mut entries: BTreeMap<String, Vec<(f64, String)>> = BTreeMap::new();
    for post in posts {
        let member = format!("{author}:{}", post.id);
        let sets = PostStream::shared_set_entries(author, post)
            .into_iter()
            .chain(PostsByTagSearch::shared_set_entries(post));
        for (key_parts, score) in sets {
            let key = sorted_key(&[&key_parts]);
            entries
                .entry(key)
                .or_default()
                .push((score, member.clone()));
        }
    }
    entries
}
