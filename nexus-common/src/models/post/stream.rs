use std::sync::Arc;

use super::{collection_item_keys, Bookmark, PostCounts, PostDetails, PostView};
use crate::db::kv::{RedisResult, ScoreAction, SortOrder};
use crate::db::{get_neo4j_graph, queries, GraphError, GraphResult, RedisOps};
use crate::models::error::ModelError;
use crate::models::error::ModelResult;
use crate::models::follow::{Followers, Following, Friends, UserFollows};
use crate::models::post::search::PostsByTagSearch;
use crate::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};
use crate::types::{DomainTrust, Pagination, StreamSorting, WotDepth};
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use pubky_app_specs::PubkyAppPostKind;
use serde::{Deserialize, Serialize};
use tokio::task::spawn;
use tokio::time::{timeout, Duration};
use tracing::warn;
use utoipa::ToSchema;

pub const POST_TIMELINE_KEY_PARTS: [&str; 3] = ["Posts", "Global", "Timeline"];
pub const POST_TOTAL_ENGAGEMENT_KEY_PARTS: [&str; 3] = ["Posts", "Global", "TotalEngagement"];
/// The global sets restricted to posts whose author the trust ranking holds,
/// at the same scores. They serve `source=all` without tags or kind filter:
/// the Redis sets carry no trust information, and the engagement score is
/// computed from three relationship counts per post, so no graph index can
/// order it. Maintained alongside the global sets on every write, and synced
/// to the ranking whenever it is published (see
/// [`PostStream::sync_ranked_sets`]); absent while no ranking exists.
///
/// They filter on ranking membership as of the last ranking publish, while
/// the Cypher path (tagged and kind-filtered streams) filters on the graph's
/// live `trust` scores; the two can differ inside a ranking run.
pub const POST_RANKED_TIMELINE_KEY_PARTS: [&str; 4] = ["Posts", "Global", "Timeline", "Ranked"];
pub const POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS: [&str; 4] =
    ["Posts", "Global", "TotalEngagement", "Ranked"];

/// A global `author:post` sorted set and its ranked twin, with the scratch
/// keys a full build stages the twin in and moves the old one aside to.
struct MirroredSets {
    global: &'static [&'static str],
    ranked: &'static [&'static str],
    staged: &'static [&'static str],
    aside: &'static [&'static str],
}

const TIMELINE_SETS: MirroredSets = MirroredSets {
    global: &POST_TIMELINE_KEY_PARTS,
    ranked: &POST_RANKED_TIMELINE_KEY_PARTS,
    staged: &["Posts", "Global", "Timeline", "Ranked", "Staged"],
    aside: &["Posts", "Global", "Timeline", "Ranked", "Aside"],
};
const ENGAGEMENT_SETS: MirroredSets = MirroredSets {
    global: &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
    ranked: &POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS,
    staged: &["Posts", "Global", "TotalEngagement", "Ranked", "Staged"],
    aside: &["Posts", "Global", "TotalEngagement", "Ranked", "Aside"],
};

/// Every key a sync of the ranked sets reads or writes.
struct RankedSyncKeys {
    /// The published ranking.
    ranking: &'static [&'static str],
    /// Prefix of the per-author root post sets (`…:<author_id>`).
    author_posts: &'static [&'static str],
    timeline: &'static MirroredSets,
    engagement: &'static MirroredSets,
    /// The ranking the twins were last synced to. A sync diffs the published
    /// ranking against it, and only replaces it once the twins match.
    synced: &'static [&'static str],
    /// The ranking this sync applies, copied once so a concurrent publish
    /// cannot change it mid-sync.
    target: &'static [&'static str],
    /// Scratch key `synced` is moved to while `target` replaces it.
    aside: &'static [&'static str],
}

const RANKED_SYNC_KEYS: RankedSyncKeys = RankedSyncKeys {
    ranking: &USER_SOCIAL_GRAPH_KEY_PARTS,
    author_posts: &POST_PER_USER_KEY_PARTS,
    timeline: &TIMELINE_SETS,
    engagement: &ENGAGEMENT_SETS,
    synced: &["Posts", "Global", "Ranked", "Synced"],
    target: &["Posts", "Global", "Ranked", "Target"],
    aside: &["Posts", "Global", "Ranked", "Aside"],
};

/// Members read or written per Redis call while syncing the twins: every
/// command stays short however large the sets grow.
const RANKED_SYNC_CHUNK: usize = 1000;

/// A full build re-mirrors global timeline entries indexed from this long
/// before it started, covering writes the swap would drop and clock skew
/// between the watcher and the build.
const RANKED_BUILD_CATCH_UP_MS: i64 = 60_000;

impl MirroredSets {
    fn for_sorting(sorting: StreamSorting) -> &'static Self {
        match sorting {
            StreamSorting::Timeline => &TIMELINE_SETS,
            StreamSorting::TotalEngagement => &ENGAGEMENT_SETS,
        }
    }
}
pub const POST_PER_USER_KEY_PARTS: [&str; 2] = ["Posts", "AuthorParents"];
pub const POST_REPLIES_PER_USER_KEY_PARTS: [&str; 2] = ["Posts", "AuthorReplies"];
pub const POST_REPLIES_PER_POST_KEY_PARTS: [&str; 2] = ["Posts", "PostReplies"];
const BOOKMARKS_USER_KEY_PARTS: [&str; 2] = ["Bookmarks", "User"];

#[derive(ToSchema, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum StreamSource {
    PostReplies {
        post_id: String,
        author_id: String,
    },
    Following {
        observer_id: String,
    },
    Followers {
        observer_id: String,
    },
    Friends {
        observer_id: String,
    },
    Bookmarks {
        observer_id: String,
    },
    Author {
        author_id: String,
    },
    AuthorReplies {
        author_id: String,
    },
    Collection {
        author_id: String,
        post_id: String,
    },
    /// Collection posts that contain the post `author_id:post_id` as an item.
    PostCollections {
        author_id: String,
        post_id: String,
    },
    /// Posts authored by users in the observer's Web of Trust (transitive FOLLOWS, 1..=depth).
    Wot {
        observer_id: String,
        depth: WotDepth,
    },
    /// Posts by users whom the observer's Web of Trust has tagged with a `domain_tags` label.
    /// `trust = Me` restricts the taggers to the observer alone (depth-0 self set).
    /// Includes the observer's own posts when they themselves carry a matching label.
    WotDomain {
        observer_id: String,
        trust: DomainTrust,
        domain_tags: Vec<String>,
    },
    #[default]
    All,
}

impl StreamSource {
    /// Low-cardinality source value and optional WoT depth for telemetry.
    pub(crate) fn telemetry_dimensions(&self) -> (&'static str, Option<u8>) {
        match self {
            StreamSource::PostReplies { .. } => ("post_replies", None),
            StreamSource::Following { .. } => ("following", None),
            StreamSource::Followers { .. } => ("followers", None),
            StreamSource::Friends { .. } => ("friends", None),
            StreamSource::Bookmarks { .. } => ("bookmarks", None),
            StreamSource::Author { .. } => ("author", None),
            StreamSource::AuthorReplies { .. } => ("author_replies", None),
            StreamSource::Collection { .. } => ("collection", None),
            StreamSource::PostCollections { .. } => ("post_collections", None),
            StreamSource::Wot { depth, .. } => ("wot", Some(depth.get())),
            StreamSource::WotDomain { trust, .. } => (
                "wot_domain",
                Some(match trust {
                    DomainTrust::Me => 0,
                    DomainTrust::Network(depth) => depth.get(),
                }),
            ),
            StreamSource::All => ("all", None),
        }
    }

    pub fn get_observer(&self) -> Option<&str> {
        match self {
            StreamSource::Followers { observer_id }
            | StreamSource::Following { observer_id }
            | StreamSource::Friends { observer_id }
            | StreamSource::Bookmarks { observer_id }
            | StreamSource::Wot { observer_id, .. }
            | StreamSource::WotDomain { observer_id, .. } => Some(observer_id),
            _ => None,
        }
    }

    /// Domain-trust tag labels carried by `WotDomain`; `None` for other sources.
    pub fn get_domain_tags(&self) -> Option<&[String]> {
        match self {
            StreamSource::WotDomain { domain_tags, .. } => Some(domain_tags),
            _ => None,
        }
    }

    /// Author whose posts are streamed. Collection and PostCollections return
    /// `None`: their `author_id` names the anchoring post, not the streamed
    /// authors, and `post_stream` filters on `author.id` whenever this is `Some`.
    pub fn get_author(&self) -> Option<&str> {
        match self {
            StreamSource::PostReplies {
                author_id,
                post_id: _,
            } => Some(author_id),
            StreamSource::Author { author_id } => Some(author_id),
            StreamSource::AuthorReplies { author_id } => Some(author_id),
            _ => None,
        }
    }

    /// Post the stream is anchored on; `None` unless the source is PostCollections.
    pub fn get_anchor_post(&self) -> Option<(&str, &str)> {
        match self {
            StreamSource::PostCollections { author_id, post_id } => Some((author_id, post_id)),
            _ => None,
        }
    }
}

/// Post-kind filter for streams. Any filter routes the query to the Cypher
/// path: the Redis sorted-set indexes carry no kind information.
#[derive(Debug, Clone, PartialEq)]
pub enum KindFilter {
    /// Only posts of exactly this kind.
    Kind(PubkyAppPostKind),
    /// Posts of any kind except the listed ones. Posts with a missing (NULL)
    /// or unrecognized ("unknown") kind are never excluded.
    Exclude(Vec<PubkyAppPostKind>),
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PostKeyStream {
    pub post_keys: Vec<String>,
    pub last_post_score: Option<u64>,
}

impl PostKeyStream {
    pub fn new(post_keys: Vec<String>, last_post_score: Option<u64>) -> Self {
        Self {
            post_keys,
            last_post_score,
        }
    }

    // Iterate over tuples of (post_key, score) to extract the post keys and capture the last score
    pub fn from_scored_entries(entries: Vec<(String, f64)>) -> Self {
        let last_post_score = entries.last().map(|(_, score)| score.round() as u64);
        let post_keys = entries.into_iter().map(|(key, _)| key).collect();
        Self::new(post_keys, last_post_score)
    }

    pub fn is_empty(&self) -> bool {
        self.post_keys.is_empty()
    }
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct PostStream(pub Vec<PostView>);

impl RedisOps for PostStream {}

impl PostStream {
    pub fn extend(&mut self, post_stream: PostStream) {
        self.0.extend(post_stream.0);
    }
    pub async fn get_posts(
        source: StreamSource,
        pagination: Pagination,
        order: SortOrder,
        sorting: StreamSorting,
        viewer_id: Option<&str>,
        tags: Option<Vec<String>>,
        kind: Option<KindFilter>,
    ) -> ModelResult<Option<Self>> {
        let post_key_stream =
            Self::collect_post_keys(source, pagination, order, sorting, tags, kind).await?;

        if post_key_stream.is_empty() {
            return Ok(None);
        }

        Self::from_listed_post_ids(viewer_id, &post_key_stream.post_keys).await
    }

    pub async fn get_post_keys(
        source: StreamSource,
        pagination: Pagination,
        order: SortOrder,
        sorting: StreamSorting,
        tags: Option<Vec<String>>,
        kind: Option<KindFilter>,
    ) -> ModelResult<Option<PostKeyStream>> {
        let post_key_stream =
            Self::collect_post_keys(source, pagination, order, sorting, tags, kind).await?;

        if post_key_stream.is_empty() {
            return Ok(None);
        }

        Ok(Some(post_key_stream))
    }

    async fn collect_post_keys(
        source: StreamSource,
        pagination: Pagination,
        order: SortOrder,
        sorting: StreamSorting,
        tags: Option<Vec<String>>,
        kind: Option<KindFilter>,
    ) -> ModelResult<PostKeyStream> {
        // Collection has its own envelope-driven resolution path (neither
        // sorted-set index nor Cypher).
        if let StreamSource::Collection { author_id, post_id } = &source {
            return Self::get_collection_items_post_keys(
                author_id,
                post_id,
                pagination.skip,
                pagination.limit,
            )
            .await;
        }

        // WoT sources emit observability metrics (spec v3.1). Capture the source
        // label and depth before `source` is consumed by the query below.
        let wot = match &source {
            StreamSource::Wot { depth, .. } => Some(("wot", depth.get())),
            // depth-0 is the "Me" self trust set; 1..=3 is the follow-network reach.
            StreamSource::WotDomain { trust, .. } => Some((
                "wot_domain",
                match trust {
                    DomainTrust::Me => 0,
                    DomainTrust::Network(depth) => depth.get(),
                },
            )),
            _ => None,
        };
        if let Some((source, depth)) = wot {
            super::metrics::record_wot_request(source, depth);
        }

        // Decide whether to use index or fallback to graph query
        let use_index = Self::can_use_index(&sorting, &source, &tags, &kind);

        let started = std::time::Instant::now();
        let result: ModelResult<PostKeyStream> = match use_index {
            true => Self::get_from_index(source, sorting, order, &tags, pagination).await,
            false => Self::get_from_graph(source, sorting, order, &tags, pagination, kind)
                .await
                .map_err(Into::into),
        };

        // Record duration on both success and error paths, so timeouts / DB errors
        // are not silently dropped from the latency histogram.
        if let Some((source, depth)) = wot {
            super::metrics::record_wot_result(
                source,
                depth,
                started.elapsed(),
                result.as_ref().ok().map(|keys| keys.post_keys.len()),
            );
        }

        result
    }

    // Determine if we have a quick access sorted set for this combination
    fn can_use_index(
        sorting: &StreamSorting,
        source: &StreamSource,
        tags: &Option<Vec<String>>,
        kind: &Option<KindFilter>,
    ) -> bool {
        if kind.is_some() {
            return false;
        }
        match (sorting, source, tags) {
            // We have a sorted set for posts by a specific author
            (StreamSorting::Timeline, StreamSource::Author { .. }, None) => true,
            // Both global sorts have a trust-filtered sorted set. A single tag
            // has a per-tag set, read only while no ranking exists (see
            // `get_from_index`); several tags query the graph.
            (_, StreamSource::All, None) => true,
            (_, StreamSource::All, Some(tags)) if tags.len() == 1 => true,
            (_, StreamSource::All, Some(_)) => false,
            // We can use sorted set for posts by source only for timeline
            (StreamSorting::Timeline, StreamSource::Following { .. }, None) => true,
            (StreamSorting::Timeline, StreamSource::Followers { .. }, None) => true,
            (StreamSorting::Timeline, StreamSource::Friends { .. }, None) => true,
            // We have a sorted set for bookmarks only for timeline
            (StreamSorting::Timeline, StreamSource::Bookmarks { .. }, None) => true,
            // We can use sorted set of post replies
            (_, StreamSource::PostReplies { .. }, _) => true,
            // We can use sorted set of author replies
            (_, StreamSource::AuthorReplies { .. }, _) => true,
            // Other combinations require querying the graph
            _ => false,
        }
    }

    // Fetch posts from index
    async fn get_from_index(
        source: StreamSource,
        sorting: StreamSorting,
        order: SortOrder,
        tags: &Option<Vec<String>>,
        pagination: Pagination,
    ) -> ModelResult<PostKeyStream> {
        let start = pagination.start;
        let end = pagination.end;
        let skip = pagination.skip;
        let limit = pagination.limit;

        let result = match (source, tags) {
            // Bookmark streams
            (StreamSource::Bookmarks { observer_id }, None) => {
                Self::get_bookmarked_posts(&observer_id, order, start, end, skip, limit).await?
            }
            // Stream of replies to specific a post
            (StreamSource::PostReplies { author_id, post_id }, None) => {
                Self::get_post_replies(&author_id, &post_id, order, start, end, skip, limit).await?
            }
            // Stream of parent post from a given author
            (StreamSource::Author { author_id }, None) => {
                Self::get_author_posts(&author_id, order, start, end, skip, limit, false).await?
            }
            // Streams of replies from a given author
            (StreamSource::AuthorReplies { author_id }, None) => {
                Self::get_author_posts(&author_id, order, start, end, skip, limit, true).await?
            }
            // Global streams, unranked authors hidden. A ranking with no
            // ranked set behind it yet (first deploy, a rebuild that failed
            // after the ranking was written) is served by the Cypher path,
            // which filters on the graph's trust scores.
            (StreamSource::All, None) => {
                match Self::get_global_posts_keys(
                    sorting.clone(),
                    order.clone(),
                    start,
                    end,
                    skip,
                    limit,
                )
                .await?
                {
                    Some(page) => page,
                    None => {
                        Self::get_from_graph(
                            StreamSource::All,
                            sorting,
                            order,
                            &None,
                            pagination,
                            None,
                        )
                        .await?
                    }
                }
            }
            // Single-tag global stream. The per-tag set carries no trust
            // information, so with a ranking the request takes the Cypher
            // path, where the trust filter is a predicate.
            (StreamSource::All, Some(labels)) if labels.len() == 1 => {
                if Self::trust_filter_active().await {
                    Self::get_from_graph(StreamSource::All, sorting, order, tags, pagination, None)
                        .await?
                } else {
                    Self::get_posts_keys_by_tag(&labels[0], sorting, start, end, skip, limit)
                        .await?
                }
            }
            // Streams by simple source/reach: Following, Followers, Friends
            (source, None) => {
                Self::get_posts_by_source(source, order, start, end, skip, limit).await?
            }
            _ => PostKeyStream::default(),
        };
        Ok(result)
    }

    async fn get_collection_items_post_keys(
        author_id: &str,
        post_id: &str,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<PostKeyStream> {
        let Some(details) = PostDetails::get_by_id(author_id, post_id).await? else {
            return Ok(PostKeyStream::default());
        };
        if !matches!(details.kind, PubkyAppPostKind::Collection) {
            return Ok(PostKeyStream::default());
        }
        let items = match collection_item_keys(&details.content) {
            Ok(items) => items,
            Err(e) => {
                warn!("Collection {author_id}:{post_id} envelope malformed: {e}");
                return Ok(PostKeyStream::default());
            }
        };

        let skip = skip.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        // Dead refs were already dropped, so slicing cannot shorten a page.
        let post_keys: Vec<String> = items
            .into_iter()
            .map(|(item_author_id, item_post_id)| format!("{item_author_id}:{item_post_id}"))
            .skip(skip)
            .take(limit)
            .collect();

        Ok(PostKeyStream::new(post_keys, None))
    }

    // Fetch posts from index
    async fn get_from_graph(
        source: StreamSource,
        sorting: StreamSorting,
        order: SortOrder,
        tags: &Option<Vec<String>>,
        pagination: Pagination,
        kind: Option<KindFilter>,
    ) -> GraphResult<PostKeyStream> {
        let graph = get_neo4j_graph()?;
        let ranked_only = matches!(source, StreamSource::All) && Self::trust_filter_active().await;
        let query =
            queries::get::post_stream(source, sorting, order, tags, pagination, kind, ranked_only)?;

        // The 10-second budget covers execution AND row streaming: execute()
        // only submits the query and the heavy work (ORDER BY materializes at
        // the first pull) happens while streaming, so a timeout on execute
        // alone lets a slow query run until the HTTP layer's 408.
        timeout(Duration::from_secs(10), async {
            let mut result = graph.execute(query).await?;

            let mut post_keys = Vec::new();
            // Last row's sorting score (timestamp for timeline, engagement otherwise),
            // used as the pagination cursor.
            let mut last_post_score: Option<i64> = None;

            while let Some(row) = result.try_next().await? {
                let author_id: String = row.get("author_id")?;
                let post_id: String = row.get("post_id")?;
                let score: i64 = row.get("score")?;
                last_post_score = Some(score);
                post_keys.push(format!("{author_id}:{post_id}"));
            }

            Ok(PostKeyStream::new(
                post_keys,
                last_post_score.map(|s| s as u64),
            ))
        })
        .await
        .map_err(|_| GraphError::QueryTimeout)?
    }

    /// Whether `source=all` hides unranked authors: a ranking exists. A Redis
    /// error serves the stream unfiltered.
    async fn trust_filter_active() -> bool {
        SocialGraphStatus::is_built().await.unwrap_or_else(|e| {
            warn!("Trust ranking unavailable, serving source=all unfiltered: {e}");
            false
        })
    }

    /// One page of a global stream. Reads the ranked set while a ranking
    /// exists, the unfiltered global set otherwise: both carry the same
    /// scores, so a cursor from one resumes on the other. `None` when the
    /// ranking exists but its ranked set does not.
    pub async fn get_global_posts_keys(
        sorting: StreamSorting,
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> RedisResult<Option<PostKeyStream>> {
        let sets = MirroredSets::for_sorting(sorting);
        if !Self::trust_filter_active().await {
            let entries =
                Self::try_from_index_sorted_set(sets.global, start, end, skip, limit, order, None)
                    .await?;
            return Ok(Some(PostKeyStream::from_scored_entries(
                entries.unwrap_or_default(),
            )));
        }
        Ok(
            Self::try_from_index_sorted_set(sets.ranked, start, end, skip, limit, order, None)
                .await?
                .map(PostKeyStream::from_scored_entries),
        )
    }

    pub async fn get_posts_keys_by_tag(
        label: &str,
        sorting: StreamSorting,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> RedisResult<PostKeyStream> {
        let skip = skip.unwrap_or(0);
        let limit = limit.unwrap_or(10);

        let pag = Pagination {
            start,
            end,
            skip: Some(skip),
            limit: Some(limit),
        };

        let post_search_result = PostsByTagSearch::get_by_label(label, Some(sorting), pag).await?;

        let stream = match post_search_result {
            Some(post_keys) => {
                // Iterate over PostsByTagSearch structs to extract post keys and capture the last score
                let last_post_score = post_keys.last().map(|entry| entry.score as u64);
                let post_keys = post_keys
                    .into_iter()
                    .map(|post_score| post_score.post_key)
                    .collect();
                PostKeyStream::new(post_keys, last_post_score)
            }
            None => PostKeyStream::default(),
        };

        Ok(stream)
    }

    /// Brings both ranked sets in line with the published ranking. Run by the
    /// trust job and by a full reindex right after the ranking is published.
    ///
    /// Normally applies only the difference since the last sync: posts of
    /// authors who entered the ranking are added from their per-author sets,
    /// posts of authors who left are removed. Without a previous sync, or
    /// with a twin missing, both twins are built from the global timeline.
    /// With no ranking the twins are dropped and readers use the global sets.
    /// Every Redis call handles at most [`RANKED_SYNC_CHUNK`] members.
    pub async fn sync_ranked_sets() -> RedisResult<()> {
        Self::sync_ranked_sets_with(&RANKED_SYNC_KEYS).await
    }

    async fn sync_ranked_sets_with(keys: &RankedSyncKeys) -> RedisResult<()> {
        // An empty sorted set does not exist in Redis: absent means no ranking.
        if !Self::index_sorted_set_exists(keys.ranking).await? {
            return Self::drop_ranked_sets(keys).await;
        }
        Self::unlink_index_sorted_sets(&[keys.target]).await?;
        Self::copy_in_pages(keys.ranking, keys.target).await?;

        let incremental = Self::index_sorted_set_exists(keys.synced).await?
            && Self::index_sorted_set_exists(keys.timeline.ranked).await?
            && Self::index_sorted_set_exists(keys.engagement.ranked).await?;
        match incremental {
            true => Self::apply_ranking_diff(keys).await?,
            false => Self::build_ranked_sets(keys).await?,
        }

        // Only now do the twins match the target: record it as synced.
        Self::swap_in_index_sorted_set(keys.target, keys.synced, keys.aside).await?;
        Ok(())
    }

    /// Copies a sorted set page by page, so no single command grows with it.
    async fn copy_in_pages(source: &[&str], destination: &[&str]) -> RedisResult<()> {
        let mut from = 0;
        loop {
            let page = Self::try_from_index_sorted_set_by_rank(
                source,
                from,
                from + RANKED_SYNC_CHUNK - 1,
                None,
            )
            .await?;
            let elements: Vec<(f64, &str)> = page.iter().map(|(m, s)| (*s, m.as_str())).collect();
            Self::put_index_sorted_set(destination, &elements, None, None).await?;
            if page.len() < RANKED_SYNC_CHUNK {
                return Ok(());
            }
            from += RANKED_SYNC_CHUNK;
        }
    }

    /// Adds the posts of authors who entered the ranking since the last sync
    /// and removes those of authors who left. Cost follows ranking churn:
    /// each ranking is paged once and checked against the other, and only
    /// changed authors touch their posts.
    async fn apply_ranking_diff(keys: &RankedSyncKeys) -> RedisResult<()> {
        for (ranking, previous, entered) in [
            (keys.target, keys.synced, true),
            (keys.synced, keys.target, false),
        ] {
            let mut from = 0;
            loop {
                let page = Self::try_from_index_sorted_set_by_rank(
                    ranking,
                    from,
                    from + RANKED_SYNC_CHUNK - 1,
                    None,
                )
                .await?;
                let authors: Vec<&str> = page.iter().map(|(id, _)| id.as_str()).collect();
                let in_previous = Self::index_sorted_set_scores(previous, &authors).await?;
                for (author_id, rank) in authors.iter().zip(&in_previous) {
                    if rank.is_none() {
                        Self::sync_author_posts(keys, author_id, entered).await?;
                    }
                }
                if page.len() < RANKED_SYNC_CHUNK {
                    break;
                }
                from += RANKED_SYNC_CHUNK;
            }
        }
        Ok(())
    }

    /// Adds (`entered`) or removes one author's root posts in both twins,
    /// paging their per-author set. Added posts take their global scores; a
    /// post missing from a global set is skipped for that set, so the twins
    /// never hold more than the global sets.
    async fn sync_author_posts(
        keys: &RankedSyncKeys,
        author_id: &str,
        entered: bool,
    ) -> RedisResult<()> {
        let author_posts = [keys.author_posts, &[author_id]].concat();
        let mut from = 0;
        loop {
            let page = Self::try_from_index_sorted_set_by_rank(
                &author_posts,
                from,
                from + RANKED_SYNC_CHUNK - 1,
                None,
            )
            .await?;
            let post_keys: Vec<String> = page
                .iter()
                .map(|(post_id, _)| format!("{author_id}:{post_id}"))
                .collect();
            let post_keys: Vec<&str> = post_keys.iter().map(String::as_str).collect();

            for sets in [keys.timeline, keys.engagement] {
                match entered {
                    true => {
                        let scores = Self::index_sorted_set_scores(sets.global, &post_keys).await?;
                        let elements = Self::scored(&post_keys, &scores);
                        Self::put_index_sorted_set(sets.ranked, &elements, None, None).await?;
                    }
                    false => {
                        Self::remove_from_index_sorted_set(None, sets.ranked, &post_keys).await?;
                    }
                }
            }

            if page.len() < RANKED_SYNC_CHUNK {
                return Ok(());
            }
            from += RANKED_SYNC_CHUNK;
        }
    }

    /// Builds both twins from scratch: stages them from the global timeline,
    /// swaps them in, then re-mirrors the entries indexed while it ran, which
    /// the swap would otherwise drop.
    ///
    /// The catch-up goes by timeline score, so it does not cover engagement
    /// bumps: a reply, repost or tag that lands on an older post after the
    /// walk has passed it is mirrored into the old twin, which the swap
    /// discards, and the new twin keeps the post's earlier score until its
    /// next bump.
    async fn build_ranked_sets(keys: &RankedSyncKeys) -> RedisResult<()> {
        let started_at = chrono::Utc::now().timestamp_millis();
        Self::unlink_index_sorted_sets(&[keys.timeline.staged, keys.engagement.staged]).await?;

        Self::copy_ranked_entries(keys, None, |sets| sets.staged).await?;
        for sets in [keys.timeline, keys.engagement] {
            Self::swap_in_index_sorted_set(sets.staged, sets.ranked, sets.aside).await?;
        }

        let catch_up_from = (started_at - RANKED_BUILD_CATCH_UP_MS) as f64;
        Self::copy_ranked_entries(keys, Some(catch_up_from), |sets| sets.ranked).await
    }

    /// Walks the global timeline in ascending score order from `from_score`
    /// and writes every entry by a target-ranked author into the set that
    /// `destination` picks, for both the timeline and the engagement twin.
    /// Engagement scores come from the global engagement set.
    ///
    /// The timeline is walked because its scores only grow: entries indexed
    /// during the walk land after the cursor instead of shifting pages, which
    /// an engagement walk could not promise. The score cursor skips the
    /// entries already read at the cursor score, and timeline ties are rare,
    /// so the skip stays small.
    async fn copy_ranked_entries(
        keys: &RankedSyncKeys,
        from_score: Option<f64>,
        destination: fn(&MirroredSets) -> &'static [&'static str],
    ) -> RedisResult<()> {
        let mut cursor = from_score;
        let mut seen_at_cursor = 0usize;
        loop {
            let page = Self::try_from_index_sorted_set(
                keys.timeline.global,
                None,
                cursor,
                Some(seen_at_cursor),
                Some(RANKED_SYNC_CHUNK),
                SortOrder::Ascending,
                None,
            )
            .await?
            .unwrap_or_default();

            let authors: Vec<&str> = page
                .iter()
                .map(|(key, _)| {
                    key.split_once(':')
                        .map_or(key.as_str(), |(author, _)| author)
                })
                .collect();
            let ranks = Self::index_sorted_set_scores(keys.target, &authors).await?;
            let timeline: Vec<(f64, &str)> = page
                .iter()
                .zip(&ranks)
                .filter(|(_, rank)| rank.is_some())
                .map(|((key, score), _)| (*score, key.as_str()))
                .collect();
            let post_keys: Vec<&str> = timeline.iter().map(|(_, key)| *key).collect();
            let engagement_scores =
                Self::index_sorted_set_scores(keys.engagement.global, &post_keys).await?;
            let engagement = Self::scored(&post_keys, &engagement_scores);

            Self::put_index_sorted_set(destination(keys.timeline), &timeline, None, None).await?;
            Self::put_index_sorted_set(destination(keys.engagement), &engagement, None, None)
                .await?;

            let Some((_, last)) = page.last() else {
                return Ok(());
            };
            let ties = page
                .iter()
                .rev()
                .take_while(|(_, score)| score == last)
                .count();
            seen_at_cursor = match cursor {
                Some(previous) if previous == *last => seen_at_cursor + ties,
                _ => ties,
            };
            cursor = Some(*last);
            if page.len() < RANKED_SYNC_CHUNK {
                return Ok(());
            }
        }
    }

    /// Pairs members with their scores, dropping the members that have none.
    fn scored<'a>(members: &[&'a str], scores: &[Option<f64>]) -> Vec<(f64, &'a str)> {
        members
            .iter()
            .zip(scores)
            .filter_map(|(member, score)| score.map(|score| (score, *member)))
            .collect()
    }

    /// Removes the twins and every sync key: with no ranking there is nothing
    /// to hide, and the next ranking starts with a full build.
    async fn drop_ranked_sets(keys: &RankedSyncKeys) -> RedisResult<()> {
        Self::unlink_index_sorted_sets(&[
            keys.timeline.ranked,
            keys.timeline.staged,
            keys.timeline.aside,
            keys.engagement.ranked,
            keys.engagement.staged,
            keys.engagement.aside,
            keys.synced,
            keys.target,
            keys.aside,
        ])
        .await
    }

    /// Writes the post at `score` into the ranked set when its author is
    /// ranked. The absolute score is written rather than the increment
    /// mirrored: ZINCRBY would create an absent member. Unranked authors are
    /// left alone; the rebuild on the next ranking pass evicts posts whose
    /// author dropped out.
    async fn mirror_ranked(
        sets: &MirroredSets,
        author_id: &str,
        post_id: &str,
        score: f64,
    ) -> RedisResult<()> {
        if !SocialGraphStatus::is_ranked(author_id).await? {
            return Ok(());
        }
        let post_key = format!("{author_id}:{post_id}");
        Self::put_index_sorted_set(sets.ranked, &[(score, post_key.as_str())], None, None).await
    }

    /// Removes the post from a global set and its ranked twin.
    async fn remove_from_sets(
        sets: &MirroredSets,
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        let post_key = format!("{author_id}:{post_id}");
        Self::remove_from_index_sorted_set(None, sets.global, &[&post_key]).await?;
        Self::remove_from_index_sorted_set(None, sets.ranked, &[&post_key]).await
    }

    pub async fn get_author_posts(
        user_id: &str,
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
        replies: bool,
    ) -> RedisResult<PostKeyStream> {
        // Retrieve only parents or only reply posts written by the author from index
        let key_parts = match replies {
            true => POST_REPLIES_PER_USER_KEY_PARTS,
            false => POST_PER_USER_KEY_PARTS,
        };

        let key_parts = [&key_parts[..], &[user_id]].concat();
        let post_ids =
            Self::try_from_index_sorted_set(&key_parts, start, end, skip, limit, order, None)
                .await?;

        if let Some(post_ids) = post_ids {
            let post_keys = post_ids
                .into_iter()
                .map(|(post_id, score)| (format!("{user_id}:{post_id}"), score))
                .collect();
            Ok(PostKeyStream::from_scored_entries(post_keys))
        } else {
            Ok(PostKeyStream::default())
        }
    }

    pub async fn get_posts_by_source(
        source: StreamSource,
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<PostKeyStream> {
        let custom_limit = Some(200);
        let observer_id = source.get_observer();
        let user_ids = match &source {
            StreamSource::Following { observer_id } => {
                Following::get_by_id(observer_id, None, custom_limit)
                    .await?
                    .unwrap_or_default()
                    .0
            }
            StreamSource::Followers { observer_id } => {
                Followers::get_by_id(observer_id, None, custom_limit)
                    .await?
                    .unwrap_or_default()
                    .0
            }
            StreamSource::Friends { observer_id } => {
                Friends::get_by_id(observer_id, None, custom_limit)
                    .await?
                    .unwrap_or_default()
                    .0
            }
            _ => vec![],
        }
        .into_iter()
        .filter(|user_id| Some(user_id.as_str()) != observer_id)
        .collect::<Vec<_>>();

        if !user_ids.is_empty() {
            let post_keys = Self::get_posts_for_user_ids(
                &user_ids.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
                order,
                start,
                end,
                skip,
                limit,
            )
            .await?;
            Ok(PostKeyStream::from_scored_entries(post_keys))
        } else {
            Ok(PostKeyStream::default())
        }
    }

    pub async fn get_bookmarked_posts(
        user_id: &str,
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> RedisResult<PostKeyStream> {
        let key_parts = [&BOOKMARKS_USER_KEY_PARTS[..], &[user_id]].concat();
        let post_keys =
            Self::try_from_index_sorted_set(&key_parts, start, end, skip, limit, order, None)
                .await?;

        Ok(PostKeyStream::from_scored_entries(
            post_keys.unwrap_or_default(),
        ))
    }

    pub async fn get_post_replies(
        author_id: &str,
        post_id: &str,
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> RedisResult<PostKeyStream> {
        let key_parts = [&POST_REPLIES_PER_POST_KEY_PARTS[..], &[author_id, post_id]].concat();
        let post_replies =
            Self::try_from_index_sorted_set(&key_parts, start, end, skip, limit, order, None)
                .await?;
        Ok(PostKeyStream::from_scored_entries(
            post_replies.unwrap_or_default(),
        ))
    }

    // Streams for followers / followings / friends are expensive.
    // We are truncating to the first 200 user_ids. We could also random draw 200.
    // TODO rethink, we could also fallback to graph
    async fn get_posts_for_user_ids(
        user_ids: &[&str],
        order: SortOrder,
        start: Option<f64>,
        end: Option<f64>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<Vec<(String, f64)>> {
        // Limit the number of user IDs to process to the first 200
        let max_user_ids = 200;
        let truncated_user_ids: Vec<String> = user_ids
            .iter()
            .take(max_user_ids)
            .map(|s| s.to_string())
            .collect();

        // Bounded to protect the pool; `buffered` keeps equal-score ties in input
        // order through the stable re-sort below; items owned so the future stays `Send`.
        let mut post_keys: Vec<(f64, String)> =
            stream::iter(truncated_user_ids.into_iter().map(|user_id| {
                let order = order.clone();
                async move {
                    let key_parts = [&POST_PER_USER_KEY_PARTS[..], &[user_id.as_str()]].concat();
                    let post_ids = Self::try_from_index_sorted_set(
                        &key_parts, start, end,
                        None, // We do not apply skip and limit here, as we need the full sorted set
                        None, order, None,
                    )
                    .await?;
                    Ok::<_, ModelError>(
                        post_ids
                            .map(|ids| {
                                ids.into_iter()
                                    .map(|(post_id, score)| (score, format!("{user_id}:{post_id}")))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default(),
                    )
                }
            }))
            .buffered(8)
            .try_collect::<Vec<Vec<_>>>()
            .await?
            .into_iter()
            .flatten()
            .collect();

        // The selected user_ids does not have any post
        if post_keys.is_empty() {
            return Ok(Vec::new());
        }

        // Sort all the collected posts globally by their score (descending)
        post_keys.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Apply global skip and limit after sorting
        let start_index = skip.unwrap_or(0).clamp(0, post_keys.len());
        let end_index = limit
            .map(|l| (start_index + l).min(post_keys.len()))
            .unwrap_or(post_keys.len());

        // Ensure valid slice range
        if start_index >= end_index {
            return Ok(Vec::new());
        }

        let selected_post_keys = post_keys[start_index..end_index]
            .iter()
            .map(|(score, post_key)| (post_key.clone(), *score))
            .collect();

        Ok(selected_post_keys)
    }

    pub async fn from_listed_post_ids(
        viewer_id: Option<&str>,
        post_keys: &[String],
    ) -> ModelResult<Option<Self>> {
        let viewer_id = viewer_id.map(Arc::from);
        let mut handles = Vec::with_capacity(post_keys.len());

        for post_key in post_keys {
            let Some((author_id, post_id)) = post_key.split_once(':') else {
                warn!("Invalid post_key format (missing ':'): {post_key}");
                continue;
            };
            let author_id = author_id.to_string();
            let viewer_id = viewer_id.clone();
            let post_id = post_id.to_string();
            let handle = spawn(async move {
                PostView::get_by_id(&author_id, &post_id, viewer_id.as_deref(), None, None).await
            });
            handles.push(handle);
        }

        let mut post_views = Vec::with_capacity(post_keys.len());

        for handle in handles {
            if let Some(post_view) = handle.await.map_err(ModelError::from_generic)?? {
                post_views.push(post_view);
            }
        }

        Ok(Some(Self(post_views)))
    }

    /// Adds the post to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn add_to_timeline_sorted_set(details: &PostDetails) -> RedisResult<()> {
        let element = format!("{}:{}", details.author, details.id);
        let score = details.indexed_at as f64;
        Self::put_index_sorted_set(
            &POST_TIMELINE_KEY_PARTS,
            &[(score, element.as_str())],
            None,
            None,
        )
        .await?;
        Self::mirror_ranked(&TIMELINE_SETS, &details.author, &details.id, score).await
    }

    /// Removes the post from the global timeline set and its ranked twin.
    pub async fn remove_from_timeline_sorted_set(
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        Self::remove_from_sets(&TIMELINE_SETS, author_id, post_id).await
    }

    /// Adds the post to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn add_to_per_user_sorted_set(details: &PostDetails) -> RedisResult<()> {
        let key_parts = [&POST_PER_USER_KEY_PARTS[..], &[details.author.as_str()]].concat();
        let score = details.indexed_at as f64;
        Self::put_index_sorted_set(&key_parts, &[(score, details.id.as_str())], None, None).await
    }

    /// Adds the post to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn remove_from_per_user_sorted_set(
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&POST_PER_USER_KEY_PARTS[..], &[author_id]].concat();
        Self::remove_from_index_sorted_set(None, &key_parts, &[post_id]).await
    }

    /// Adds the post response to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn add_to_post_reply_sorted_set(
        // parent_user_id: &str,
        // parent_post_id: &str,
        parent_post_key_parts: &[&str; 2],
        author_id: &str,
        reply_id: &str,
        indexed_at: i64,
    ) -> RedisResult<()> {
        let key_parts = [&POST_REPLIES_PER_POST_KEY_PARTS[..], parent_post_key_parts].concat();
        let score = indexed_at as f64;
        let element = format!("{author_id}:{reply_id}");
        Self::put_index_sorted_set(&key_parts, &[(score, element.as_str())], None, None).await
    }

    /// Adds the post response to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn remove_from_post_reply_sorted_set(
        parent_post_key_parts: &[&str; 2],
        author_id: &str,
        reply_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&POST_REPLIES_PER_POST_KEY_PARTS[..], parent_post_key_parts].concat();
        let element = format!("{author_id}:{reply_id}");
        Self::remove_from_index_sorted_set(None, &key_parts, &[element.as_str()]).await
    }

    /// Adds the post to a Redis sorted set of replies per author using the `indexed_at` timestamp as the score.
    pub async fn add_to_replies_per_user_sorted_set(details: &PostDetails) -> RedisResult<()> {
        let key_parts = [
            &POST_REPLIES_PER_USER_KEY_PARTS[..],
            &[details.author.as_str()],
        ]
        .concat();
        let score = details.indexed_at as f64;
        Self::put_index_sorted_set(&key_parts, &[(score, details.id.as_str())], None, None).await
    }

    /// Adds the post to a Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn remove_from_replies_per_user_sorted_set(
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&POST_REPLIES_PER_USER_KEY_PARTS[..], &[author_id]].concat();
        Self::remove_from_index_sorted_set(None, &key_parts, &[post_id]).await
    }

    /// Adds a bookmark to Redis sorted set using the `indexed_at` timestamp as the score.
    pub async fn add_to_bookmarks_sorted_set(
        bookmark: &Bookmark,
        bookmarker_id: &str,
        post_id: &str,
        author_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&BOOKMARKS_USER_KEY_PARTS[..], &[bookmarker_id]].concat();
        let post_key = format!("{author_id}:{post_id}");
        let score = bookmark.indexed_at as f64;
        Self::put_index_sorted_set(&key_parts, &[(score, post_key.as_str())], None, None).await
    }

    /// Remove a bookmark from Redis sorted
    pub async fn remove_from_bookmarks_sorted_set(
        bookmarker_id: &str,
        post_id: &str,
        author_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&BOOKMARKS_USER_KEY_PARTS[..], &[bookmarker_id]].concat();
        let post_key = format!("{author_id}:{post_id}");
        Self::remove_from_index_sorted_set(None, &key_parts, &[&post_key]).await
    }

    /// Adds the post to a Redis sorted set using the total engagement as the score.
    pub async fn add_to_engagement_sorted_set(
        counts: &PostCounts,
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        let element = format!("{author_id}:{post_id}");
        let score = counts.tags + counts.replies + counts.reposts;
        let score = score as f64;

        Self::put_index_sorted_set(
            &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
            &[(score, element.as_str())],
            None,
            None,
        )
        .await?;
        Self::mirror_ranked(&ENGAGEMENT_SETS, author_id, post_id, score).await
    }

    pub async fn delete_from_engagement_sorted_set(
        author_id: &str,
        post_id: &str,
    ) -> RedisResult<()> {
        Self::remove_from_sets(&ENGAGEMENT_SETS, author_id, post_id).await
    }

    /// Moves the post's engagement score by `score_action` in the global set,
    /// and mirrors it into the ranked set. Callers gate on the post being a
    /// root post already in the index: ZINCRBY creates an absent member.
    pub async fn update_index_score(
        author_id: &str,
        post_id: &str,
        score_action: ScoreAction,
    ) -> RedisResult<()> {
        let score = Self::incr_score_index_sorted_set(
            &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
            &[author_id, post_id],
            score_action,
        )
        .await?;
        Self::mirror_ranked(&ENGAGEMENT_SETS, author_id, post_id, score).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PostCollections is served from the graph only; no sorted set backs it.
    #[test]
    fn test_can_use_index_is_false_for_post_collections() {
        let source = StreamSource::PostCollections {
            author_id: "author".to_string(),
            post_id: "post".to_string(),
        };
        for sorting in [StreamSorting::Timeline, StreamSorting::TotalEngagement] {
            assert!(!PostStream::can_use_index(&sorting, &source, &None, &None));
        }
    }

    /// `can_use_index` short-circuits to the Cypher path whenever a kind filter
    /// is set, regardless of which kind — kind-filtered queries route via Cypher
    /// where the kind-specific filter actually applies.
    ///
    /// We parametrize across the source/sorting combinations that *would*
    /// otherwise be index-eligible (per the match arms below the early-return)
    /// to lock in that the kind short-circuit wins over the index path.
    #[test]
    fn test_can_use_index_returns_false_for_any_kind_filter() {
        let kinds_to_test = [
            PubkyAppPostKind::Short,
            PubkyAppPostKind::Long,
            PubkyAppPostKind::Image,
            PubkyAppPostKind::Video,
            PubkyAppPostKind::Link,
            PubkyAppPostKind::File,
            PubkyAppPostKind::Collection,
        ];

        // Combinations that would normally return `true` when kind is None.
        let index_eligible_combos = [
            (
                StreamSorting::Timeline,
                StreamSource::Following {
                    observer_id: "observer".to_string(),
                },
            ),
            (
                StreamSorting::Timeline,
                StreamSource::Author {
                    author_id: "author".to_string(),
                },
            ),
            (
                StreamSorting::Timeline,
                StreamSource::Bookmarks {
                    observer_id: "observer".to_string(),
                },
            ),
        ];

        for kind in &kinds_to_test {
            for (sorting, source) in &index_eligible_combos {
                for filter in [
                    KindFilter::Kind(kind.clone()),
                    KindFilter::Exclude(vec![kind.clone()]),
                ] {
                    assert!(
                        !PostStream::can_use_index(sorting, source, &None, &Some(filter.clone())),
                        "can_use_index({:?}, {:?}, None, Some({:?})) must return false",
                        sorting,
                        source,
                        filter
                    );
                }
            }
        }
    }

    /// Sanity counterpart: when no kind is set, `can_use_index` returns true
    /// for the index-eligible combinations. Locks in that the test above
    /// isn't passing because of a different bug elsewhere in the function.
    #[test]
    fn test_can_use_index_returns_true_for_no_kind_filter() {
        assert!(PostStream::can_use_index(
            &StreamSorting::Timeline,
            &StreamSource::Author {
                author_id: "author".to_string(),
            },
            &None,
            &None,
        ));
    }

    /// A private key namespace per sync test, so the tests neither see the
    /// fixture sets nor each other.
    macro_rules! sync_test_keys {
        ($name:ident, $ns:literal) => {
            const $name: RankedSyncKeys = RankedSyncKeys {
                ranking: &[$ns, "Ranking"],
                author_posts: &[$ns, "AuthorParents"],
                timeline: &MirroredSets {
                    global: &[$ns, "Timeline"],
                    ranked: &[$ns, "Timeline", "Ranked"],
                    staged: &[$ns, "Timeline", "Staged"],
                    aside: &[$ns, "Timeline", "Aside"],
                },
                engagement: &MirroredSets {
                    global: &[$ns, "Engagement"],
                    ranked: &[$ns, "Engagement", "Ranked"],
                    staged: &[$ns, "Engagement", "Staged"],
                    aside: &[$ns, "Engagement", "Aside"],
                },
                synced: &[$ns, "Synced"],
                target: &[$ns, "Target"],
                aside: &[$ns, "Aside"],
            };
        };
    }

    /// A root post in a sync test: author, post id, timeline and engagement score.
    type SeedPost = (String, String, f64, f64);
    /// Both twins as `(member, score)` lists sorted by member.
    type Twins = (Vec<(String, f64)>, Vec<(String, f64)>);

    fn posts_of(author: &str, count: usize, timestamp: impl Fn(usize) -> f64) -> Vec<SeedPost> {
        (0..count)
            .map(|i| {
                let ts = timestamp(i);
                (author.to_string(), format!("P{i:06}"), ts, (i % 7) as f64)
            })
            .collect()
    }

    async fn read_all(key: &[&str]) -> Result<Vec<(String, f64)>, crate::types::DynError> {
        Ok(PostStream::try_from_index_sorted_set_by_rank(key, 0, 1_000_000, None).await?)
    }

    async fn clear(keys: &RankedSyncKeys, authors: &[&str]) -> Result<(), crate::types::DynError> {
        PostStream::drop_ranked_sets(keys).await?;
        let mut all: Vec<Vec<&str>> = vec![
            keys.ranking.to_vec(),
            keys.timeline.global.to_vec(),
            keys.engagement.global.to_vec(),
        ];
        for author in authors {
            all.push([keys.author_posts, &[*author]].concat());
        }
        let all: Vec<&[&str]> = all.iter().map(Vec::as_slice).collect();
        Ok(PostStream::unlink_index_sorted_sets(&all).await?)
    }

    async fn set_ranking(
        keys: &RankedSyncKeys,
        ranked: &[&str],
    ) -> Result<(), crate::types::DynError> {
        let ranks: Vec<(f64, &str)> = ranked
            .iter()
            .enumerate()
            .map(|(i, id)| ((i + 1) as f64, *id))
            .collect();
        Ok(PostStream::replace_index_sorted_set(keys.ranking, &ranks, None, None).await?)
    }

    async fn add_posts(
        keys: &RankedSyncKeys,
        posts: &[SeedPost],
    ) -> Result<(), crate::types::DynError> {
        for chunk in posts.chunks(500) {
            let members: Vec<String> = chunk
                .iter()
                .map(|(a, p, _, _)| format!("{a}:{p}"))
                .collect();
            let timeline: Vec<(f64, &str)> = chunk
                .iter()
                .zip(&members)
                .map(|(post, m)| (post.2, m.as_str()))
                .collect();
            let engagement: Vec<(f64, &str)> = chunk
                .iter()
                .zip(&members)
                .map(|(post, m)| (post.3, m.as_str()))
                .collect();
            PostStream::put_index_sorted_set(keys.timeline.global, &timeline, None, None).await?;
            PostStream::put_index_sorted_set(keys.engagement.global, &engagement, None, None)
                .await?;
            for (author, post_id, ts, _) in chunk {
                let author_posts = [keys.author_posts, &[author.as_str()]].concat();
                PostStream::put_index_sorted_set(
                    &author_posts,
                    &[(*ts, post_id.as_str())],
                    None,
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// The twins a correct sync must produce: every post by a ranked author,
    /// at its global scores, as sorted `(member, score)` lists.
    fn expected_twins(posts: &[SeedPost], ranked: &[&str]) -> Twins {
        let mut timeline = Vec::new();
        let mut engagement = Vec::new();
        for (author, post_id, ts, eng) in posts {
            if ranked.contains(&author.as_str()) {
                timeline.push((format!("{author}:{post_id}"), *ts));
                engagement.push((format!("{author}:{post_id}"), *eng));
            }
        }
        timeline.sort_by(|a, b| a.0.cmp(&b.0));
        engagement.sort_by(|a, b| a.0.cmp(&b.0));
        (timeline, engagement)
    }

    async fn actual_twins(keys: &RankedSyncKeys) -> Result<Twins, crate::types::DynError> {
        let mut timeline = read_all(keys.timeline.ranked).await?;
        let mut engagement = read_all(keys.engagement.ranked).await?;
        timeline.sort_by(|a, b| a.0.cmp(&b.0));
        engagement.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((timeline, engagement))
    }

    async fn assert_no_scratch_keys(keys: &RankedSyncKeys) -> Result<(), crate::types::DynError> {
        for key in [
            keys.timeline.staged,
            keys.timeline.aside,
            keys.engagement.staged,
            keys.engagement.aside,
            keys.target,
            keys.aside,
        ] {
            assert!(
                !PostStream::index_sorted_set_exists(key).await?,
                "scratch key {key:?} left behind"
            );
        }
        Ok(())
    }

    /// A full build keeps exactly the ranked authors' posts at their global
    /// scores, across pages and across a run of equal timestamps longer than
    /// a page, and records the ranking it applied.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sync_full_build_filters_by_ranking() -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};
        sync_test_keys!(KEYS, "Test:RankedSync:Full");
        StackManager::setup(&StackConfig::default()).await?;
        let authors = ["ranked-a", "ranked-b", "unranked-c"];
        clear(&KEYS, &authors).await?;

        // 1,500 posts share one timestamp, so a page boundary falls inside the tie.
        let mut posts = posts_of("ranked-a", 1_500, |_| 1_000.0);
        posts.extend(posts_of("ranked-b", 700, |i| 2_000.0 + i as f64));
        posts.extend(posts_of("unranked-c", 900, |i| 500.0 + i as f64));
        add_posts(&KEYS, &posts).await?;
        set_ranking(&KEYS, &["ranked-a", "ranked-b"]).await?;

        PostStream::sync_ranked_sets_with(&KEYS).await?;

        assert_eq!(
            actual_twins(&KEYS).await?,
            expected_twins(&posts, &["ranked-a", "ranked-b"])
        );
        assert_eq!(read_all(KEYS.synced).await?, read_all(KEYS.ranking).await?);
        assert_no_scratch_keys(&KEYS).await?;
        clear(&KEYS, &authors).await
    }

    /// After a sync, a ranking change is applied as a diff: the entering
    /// author's posts are added (paging a per-author set longer than a page),
    /// the leaving author's are removed, and nothing else is touched.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sync_applies_the_ranking_diff() -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};
        sync_test_keys!(KEYS, "Test:RankedSync:Diff");
        StackManager::setup(&StackConfig::default()).await?;
        let authors = ["stays", "leaves", "enters"];
        clear(&KEYS, &authors).await?;

        let mut posts = posts_of("stays", 50, |i| 100.0 + i as f64);
        posts.extend(posts_of("leaves", 60, |i| 300.0 + i as f64));
        posts.extend(posts_of("enters", 1_200, |i| 500.0 + i as f64));
        add_posts(&KEYS, &posts).await?;
        set_ranking(&KEYS, &["stays", "leaves"]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;

        // A member only a full build would drop proves the diff path ran.
        PostStream::put_index_sorted_set(KEYS.timeline.ranked, &[(1.0, "stray:X")], None, None)
            .await?;
        set_ranking(&KEYS, &["stays", "enters"]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;

        let (mut timeline, engagement) = expected_twins(&posts, &["stays", "enters"]);
        timeline.push(("stray:X".to_string(), 1.0));
        timeline.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(actual_twins(&KEYS).await?, (timeline, engagement));
        assert_eq!(read_all(KEYS.synced).await?, read_all(KEYS.ranking).await?);
        assert_no_scratch_keys(&KEYS).await?;
        clear(&KEYS, &authors).await
    }

    /// A sync that stopped before recording its ranking is fully re-applied
    /// by the next one, and a missing sync record forces a full build.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sync_recovers_an_interrupted_sync() -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};
        sync_test_keys!(KEYS, "Test:RankedSync:Recover");
        StackManager::setup(&StackConfig::default()).await?;
        let authors = ["stays", "leaves", "enters"];
        clear(&KEYS, &authors).await?;

        let mut posts = posts_of("stays", 10, |i| 100.0 + i as f64);
        posts.extend(posts_of("leaves", 10, |i| 200.0 + i as f64));
        posts.extend(posts_of("enters", 10, |i| 300.0 + i as f64));
        add_posts(&KEYS, &posts).await?;
        set_ranking(&KEYS, &["stays", "leaves"]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;

        // Interrupted: half of the entering author's posts written, nothing
        // removed, the sync record still at the old ranking.
        set_ranking(&KEYS, &["stays", "enters"]).await?;
        let (half, _) = expected_twins(&posts[20..25], &["enters"]);
        let half: Vec<(f64, &str)> = half.iter().map(|(m, s)| (*s, m.as_str())).collect();
        PostStream::put_index_sorted_set(KEYS.timeline.ranked, &half, None, None).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;
        assert_eq!(
            actual_twins(&KEYS).await?,
            expected_twins(&posts, &["stays", "enters"])
        );

        // No sync record: a full build, which drops anything unexpected.
        PostStream::put_index_sorted_set(KEYS.timeline.ranked, &[(1.0, "stray:X")], None, None)
            .await?;
        PostStream::unlink_index_sorted_sets(&[KEYS.synced]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;
        assert_eq!(
            actual_twins(&KEYS).await?,
            expected_twins(&posts, &["stays", "enters"])
        );
        assert_no_scratch_keys(&KEYS).await?;
        clear(&KEYS, &authors).await
    }

    /// With no ranking the twins and the sync record are dropped.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sync_drops_the_twins_without_a_ranking() -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};
        sync_test_keys!(KEYS, "Test:RankedSync:NoRanking");
        StackManager::setup(&StackConfig::default()).await?;
        let authors = ["ranked"];
        clear(&KEYS, &authors).await?;

        add_posts(&KEYS, &posts_of("ranked", 5, |i| 10.0 + i as f64)).await?;
        set_ranking(&KEYS, &["ranked"]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;
        assert!(PostStream::index_sorted_set_exists(KEYS.timeline.ranked).await?);

        PostStream::unlink_index_sorted_sets(&[KEYS.ranking]).await?;
        PostStream::sync_ranked_sets_with(&KEYS).await?;
        for key in [KEYS.timeline.ranked, KEYS.engagement.ranked, KEYS.synced] {
            assert!(
                !PostStream::index_sorted_set_exists(key).await?,
                "{key:?} kept"
            );
        }
        assert_no_scratch_keys(&KEYS).await?;
        clear(&KEYS, &authors).await
    }

    /// The catch-up after a full build copies only the timeline entries from
    /// its start score onward, into the live twins.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sync_catch_up_copies_entries_from_its_start(
    ) -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};
        sync_test_keys!(KEYS, "Test:RankedSync:CatchUp");
        StackManager::setup(&StackConfig::default()).await?;
        let authors = ["ranked", "unranked"];
        clear(&KEYS, &authors).await?;

        let mut posts = posts_of("ranked", 20, |i| 1_000.0 + i as f64);
        posts.extend(posts_of("unranked", 20, |i| 1_000.0 + i as f64));
        add_posts(&KEYS, &posts).await?;
        set_ranking(&KEYS, &["ranked"]).await?;
        PostStream::copy_in_pages(KEYS.ranking, KEYS.target).await?;

        PostStream::copy_ranked_entries(&KEYS, Some(1_010.0), |sets| sets.ranked).await?;

        let late: Vec<SeedPost> = posts.iter().filter(|p| p.2 >= 1_010.0).cloned().collect();
        assert_eq!(
            actual_twins(&KEYS).await?,
            expected_twins(&late, &["ranked"])
        );
        clear(&KEYS, &authors).await
    }

    /// The per-write mirror: a root post by a ranked author enters both ranked
    /// sets at its global score and leaves them on delete; one by an unranked
    /// author never enters them. Needs the docker stack with a ranking loaded.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_sets_mirror_writes_by_ranked_authors() -> Result<(), crate::types::DynError> {
        use crate::{StackConfig, StackManager};

        StackManager::setup(&StackConfig::default()).await?;
        let ranked_author = PostStream::try_from_index_sorted_set(
            &USER_SOCIAL_GRAPH_KEY_PARTS,
            None,
            None,
            None,
            Some(1),
            SortOrder::Ascending,
            None,
        )
        .await?
        .and_then(|members| members.into_iter().next())
        .map(|(id, _)| id)
        .expect("a trust ranking is loaded");
        const UNRANKED_AUTHOR: &str = "mirror-test-unranked-author";
        const POST_ID: &str = "MIRRORTEST001";

        for (author, mirrored) in [(ranked_author.as_str(), true), (UNRANKED_AUTHOR, false)] {
            let member = [author, POST_ID];
            let in_ranked = |sets: &'static MirroredSets| async move {
                PostStream::check_sorted_set_member(None, sets.ranked, &member).await
            };

            PostStream::mirror_ranked(&TIMELINE_SETS, author, POST_ID, 1_650_000_000_000.0).await?;
            assert_eq!(
                in_ranked(&TIMELINE_SETS).await?,
                mirrored.then_some(1_650_000_000_000),
                "timeline mirror for {author}"
            );

            let counts = PostCounts {
                tags: 2,
                ..Default::default()
            };
            PostStream::add_to_engagement_sorted_set(&counts, author, POST_ID).await?;
            assert_eq!(
                in_ranked(&ENGAGEMENT_SETS).await?,
                mirrored.then_some(2),
                "engagement mirror for {author}"
            );
            PostStream::update_index_score(author, POST_ID, ScoreAction::Increment(1.0)).await?;
            assert_eq!(
                in_ranked(&ENGAGEMENT_SETS).await?,
                mirrored.then_some(3),
                "engagement mirror follows the global score for {author}"
            );

            PostStream::remove_from_timeline_sorted_set(author, POST_ID).await?;
            PostStream::delete_from_engagement_sorted_set(author, POST_ID).await?;
            for sets in [&TIMELINE_SETS, &ENGAGEMENT_SETS] {
                assert_eq!(in_ranked(sets).await?, None, "delete clears the twin");
                assert_eq!(
                    PostStream::check_sorted_set_member(None, sets.global, &member).await?,
                    None,
                    "delete clears the global set"
                );
            }
        }
        Ok(())
    }

    /// Untagged and single-tag `All` streams are index-eligible (the index
    /// reader decides between the ranked sets, the per-tag set and Cypher);
    /// several tags always query the graph.
    #[test]
    fn test_can_use_index_for_all_depends_on_tags() {
        for sorting in [StreamSorting::Timeline, StreamSorting::TotalEngagement] {
            assert!(PostStream::can_use_index(
                &sorting,
                &StreamSource::All,
                &None,
                &None
            ));
            assert!(PostStream::can_use_index(
                &sorting,
                &StreamSource::All,
                &Some(vec!["tag".to_string()]),
                &None,
            ));
            assert!(!PostStream::can_use_index(
                &sorting,
                &StreamSource::All,
                &Some(vec!["a".to_string(), "b".to_string()]),
                &None,
            ));
        }
    }
}
