use crate::db::kv::{strings, RedisResult, SortOrder};
use crate::db::{fetch_key_from_graph, queries, RedisOps};
use crate::models::error::ModelResult;
use crate::models::user::SocialGraphStatus;
use crate::types::routes::HotTagsInputDTO;
use crate::types::{StreamReach, Timeframe};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use tracing::{debug, warn};
use utoipa::ToSchema;

use super::global::{CachedTaggers, HotTagsTaggers, Taggers};
use super::TaggedType;

pub const HOT_TAGS_CACHE_PREFIX: &str = "Cache";
pub const POST_HOT_TAGS: [&str; 3] = ["Tags", "Post", "Hot"];
/// Snapshot size per timeframe. A skip past this cannot be filled from cache.
pub const GLOBAL_HOT_TAGS_CACHE_SIZE: usize = 100;
const GLOBAL_HOT_TAGS_TAGGERS_LIMIT: usize = 20;
/// Key segment of the cache variant that counts only ranked taggers.
pub const RANKED_HOT_TAGS: &str = "Ranked";
/// Key segment of the marker a scan that found no tags leaves for its variant.
const EMPTY_HOT_TAGS: &str = "Empty";
/// Seconds an empty scan is remembered, which is how long tags appearing in an
/// empty window can stay unserved.
const EMPTY_HOT_TAGS_TTL: u64 = 60;

/// Whether hot tags count only taggers with a positive trust score: a trust
/// ranking (`Sorted:Users:SocialGraph`, built by the `trust-recompute` job)
/// exists. Without one the recompute never ran and nobody carries trust, so
/// the filter would empty every response. A Redis error serves the tags
/// unfiltered.
pub(crate) async fn ranked_taggers_only() -> bool {
    ranked_only_from(SocialGraphStatus::is_built().await)
}

/// The decision behind [`ranked_taggers_only`]. A failed read fails open: the
/// filter is off.
fn ranked_only_from(built: RedisResult<bool>) -> bool {
    built.unwrap_or_else(|e| {
        warn!("Trust ranking unavailable, serving hot tags unfiltered: {e}");
        false
    })
}

/// Cache key parts for a hot-tags index: `Tags:Post:Hot[:Ranked]:<rest>`.
///
/// The ranked variant lives under its own keys so that a ranking appearing (or
/// being dropped) switches the served variant on the next request instead of
/// waiting out the TTL of an entry counted the other way.
pub fn hot_tags_key_parts<'a>(ranked_only: bool, rest: &[&'a str]) -> Vec<&'a str> {
    let variant: &[&str] = if ranked_only { &[RANKED_HOT_TAGS] } else { &[] };
    [&POST_HOT_TAGS[..], variant, rest].concat()
}

/// The decision behind [`HotTags::read_global_cache_or_fill`]: a hit is served
/// as read, a miss runs `fill` once and is read again.
async fn read_or_fill<T, R, ReadFut, F, FillFut>(read: R, fill: F) -> ModelResult<Option<T>>
where
    R: Fn() -> ReadFut,
    ReadFut: Future<Output = RedisResult<Option<T>>>,
    F: FnOnce() -> FillFut,
    FillFut: Future<Output = ModelResult<()>>,
{
    if let Some(cached) = read().await? {
        return Ok(Some(cached));
    }
    fill().await?;
    read().await.map_err(Into::into)
}

/// Key of the empty marker of one variant and timeframe, under its prefix.
fn empty_marker_key(timeframe: &Timeframe, ranked_only: bool) -> String {
    let timeframe = timeframe.to_string();
    hot_tags_key_parts(ranked_only, &[EMPTY_HOT_TAGS, &timeframe]).join(":")
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct HotTag {
    pub label: String,
    pub taggers_id: Taggers,
    pub tagged_count: u64,
    pub taggers_count: usize,
}

// Define a newtype wrapper
#[derive(Serialize, Deserialize, Debug, ToSchema, Default, Clone)]
pub struct HotTags(pub Vec<HotTag>);

impl RedisOps for HotTags {}

// Implement Deref so TagList can be used like Vec<String>
impl Deref for HotTags {
    type Target = Vec<HotTag>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Create a HotTags instance directly from an iterator of HotTag items
// Need it in collect()
impl FromIterator<HotTag> for HotTags {
    fn from_iter<I: IntoIterator<Item = HotTag>>(iter: I) -> Self {
        HotTags(iter.into_iter().collect())
    }
}

impl HotTags {
    /// It dynamically determines whether to fetch **global hot tags** or **user-specific hot tags**
    /// based on the provided `user_id` and `reach` parameters
    ///
    /// # Arguments
    /// * `user_id` - An optional user ID
    /// * `reach` - An optional `TagStreamReach` value specifying the scope of tag retrieval
    /// * `hot_tags_input` - The input parameters received from the API endpoint
    pub async fn get_hot_tags(
        user_id: Option<String>,
        reach: Option<StreamReach>,
        hot_tags_input: &HotTagsInputDTO,
    ) -> ModelResult<Option<HotTags>> {
        let ranked_only = ranked_taggers_only().await;
        match user_id {
            Some(user_id) => {
                HotTags::get_hot_tags_by_reach(
                    user_id,
                    reach.unwrap_or(StreamReach::Following),
                    hot_tags_input,
                    ranked_only,
                )
                .await
            }
            None => HotTags::get_global_hot_tags(hot_tags_input, ranked_only).await,
        }
    }

    /// Retrieves hot tags based on the user's reach criteria
    /// Queries the graph database to fetch hot tags relevant to a given user,
    /// filtered by their reach and additional criteria defined in `hot_tags_input`
    ///
    /// # Arguments
    /// * `user_id` - The ID of the user whose reach is used for filtering hot tags
    /// * `reach` - The `TagStreamReach` parameter that defines the scope of tag retrieval
    /// * `hot_tags_input` - The input parameters received from the API endpoint
    /// * `ranked_only` - Count only taggers with a positive trust score
    async fn get_hot_tags_by_reach(
        user_id: String,
        reach: StreamReach,
        hot_tags_input: &HotTagsInputDTO,
        ranked_only: bool,
    ) -> ModelResult<Option<HotTags>> {
        let query = queries::get::get_hot_tags_by_reach(
            user_id.as_str(),
            reach,
            hot_tags_input,
            ranked_only,
        );
        fetch_key_from_graph::<HotTags>(query, "hot_tags")
            .await
            .map_err(Into::into)
    }

    /// Cache hit, including an empty page. A missing key is filled first, so
    /// `skip`/`limit`/`taggers_limit` apply to the snapshot, not the graph result.
    ///
    /// * `ranked_only` - Count only taggers with a positive trust score; selects the cache variant
    async fn get_global_hot_tags(
        hot_tags_input: &HotTagsInputDTO,
        ranked_only: bool,
    ) -> ModelResult<Option<HotTags>> {
        // A skip past the snapshot is an empty page no matter what the graph holds,
        // so filling for it would only let a caller walk `skip` to force scans.
        if hot_tags_input.skip >= GLOBAL_HOT_TAGS_CACHE_SIZE {
            return Ok(Some(HotTags::default()));
        }

        HotTags::read_global_cache_or_fill(&hot_tags_input.timeframe, ranked_only, || {
            HotTags::get_from_global_cache(hot_tags_input, HOT_TAGS_CACHE_PREFIX, ranked_only)
        })
        .await
    }

    /// The read-through every global reader shares: `read` the selected cache
    /// variant and, on a miss, scan the graph into it and `read` again. A scan
    /// that found no tags is remembered for [`EMPTY_HOT_TAGS_TTL`] seconds and
    /// read as a hit with no tags, so an empty window is not scanned per request.
    pub(crate) async fn read_global_cache_or_fill<T, R, ReadFut>(
        timeframe: &Timeframe,
        ranked_only: bool,
        read: R,
    ) -> ModelResult<Option<T>>
    where
        R: Fn() -> ReadFut,
        ReadFut: Future<Output = RedisResult<Option<T>>>,
    {
        HotTags::read_cache_or_fill_from(
            timeframe,
            HOT_TAGS_CACHE_PREFIX,
            ranked_only,
            EMPTY_HOT_TAGS_TTL,
            read,
            || HotTags::scan_global(timeframe, ranked_only),
        )
        .await
    }

    /// [`HotTags::read_global_cache_or_fill`] with the graph behind `scan`, so
    /// tests can stand in for it, under their own `prefix`.
    async fn read_cache_or_fill_from<T, R, ReadFut, S, ScanFut>(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
        empty_ttl: u64,
        read: R,
        scan: S,
    ) -> ModelResult<Option<T>>
    where
        R: Fn() -> ReadFut,
        ReadFut: Future<Output = RedisResult<Option<T>>>,
        S: FnOnce() -> ScanFut,
        ScanFut: Future<Output = ModelResult<Option<HotTags>>>,
    {
        read_or_fill(read, || async {
            let result = scan().await?;
            HotTags::write_or_mark_empty(result, timeframe, prefix, ranked_only, empty_ttl).await
        })
        .await
    }

    /// Scan the top [`GLOBAL_HOT_TAGS_CACHE_SIZE`] post tags and replace the cache.
    ///
    /// Reads whether a trust ranking exists; callers that already know pass it to
    /// [`HotTags::fetch_and_cache_variant`] instead.
    pub async fn fetch_and_cache(timeframe: &Timeframe) -> ModelResult<()> {
        let ranked_only = ranked_taggers_only().await;
        HotTags::fetch_and_cache_variant(timeframe, ranked_only).await
    }

    /// Scan and replace one cache variant. A result with no tags leaves the previous
    /// ranking in place.
    async fn fetch_and_cache_variant(timeframe: &Timeframe, ranked_only: bool) -> ModelResult<()> {
        let result = HotTags::scan_global(timeframe, ranked_only).await?;
        HotTags::write_or_preserve_cache(result, timeframe, HOT_TAGS_CACHE_PREFIX, ranked_only)
            .await
    }

    /// The graph's top [`GLOBAL_HOT_TAGS_CACHE_SIZE`] post tags of one variant.
    async fn scan_global(timeframe: &Timeframe, ranked_only: bool) -> ModelResult<Option<HotTags>> {
        let query_input = HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        );
        let query = queries::get::get_global_hot_tags(&query_input, ranked_only);
        fetch_key_from_graph::<HotTags>(query, "hot_tags")
            .await
            .map_err(Into::into)
    }

    /// What a miss does with its scan: a result with tags replaces both keys,
    /// anything else leaves the empty marker. Only a miss may mark, as the jobs
    /// keep the previous ranking on an empty result.
    async fn write_or_mark_empty(
        result: Option<HotTags>,
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
        empty_ttl: u64,
    ) -> ModelResult<()> {
        match result {
            Some(hot_tags) if !hot_tags.is_empty() => {
                debug!(%timeframe, ranked_only, count = hot_tags.len(), "Writing hot tags cache");
                HotTags::put_to_global_cache(hot_tags, timeframe, prefix, ranked_only).await?;
            }
            _ => {
                debug!(%timeframe, ranked_only, "Graph returned no hot tags, marking the window empty");
                HotTags::mark_empty(timeframe, prefix, ranked_only, empty_ttl).await?;
            }
        }
        Ok(())
    }

    /// Remember for `ttl` seconds that a scan of this variant found no tags.
    /// The marker and its expiry are written by one command, so it never
    /// exists without a TTL.
    async fn mark_empty(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
        ttl: u64,
    ) -> RedisResult<()> {
        strings::put_with_ttl(prefix, &empty_marker_key(timeframe, ranked_only), "1", ttl).await
    }

    /// Whether a scan found no tags for this variant and its marker is still alive.
    pub(crate) async fn is_marked_empty(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<bool> {
        strings::exists(prefix, &empty_marker_key(timeframe, ranked_only)).await
    }

    /// A result with tags replaces both keys; anything else is a no-op.
    /// Tests pass their own `prefix` so they never touch production keys.
    async fn write_or_preserve_cache(
        result: Option<HotTags>,
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> ModelResult<()> {
        match result {
            Some(hot_tags) if !hot_tags.is_empty() => {
                debug!(%timeframe, ranked_only, count = hot_tags.len(), "Writing hot tags cache");
                HotTags::put_to_global_cache(hot_tags, timeframe, prefix, ranked_only).await?;
            }
            _ => warn!(%timeframe, "Graph returned no hot tags — previous cache left untouched"),
        }
        Ok(())
    }

    /// `None` if either key is missing and no empty marker stands in for them.
    /// `Some([])` if both exist but this window is empty, or the marker is alive.
    ///
    /// * `ranked_only` - Read the cache variant that counts only ranked taggers
    async fn get_from_global_cache(
        hot_tags_input: &HotTagsInputDTO,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<Option<HotTags>> {
        let timeframe = hot_tags_input.timeframe.to_string();
        let key_parts = Self::build_hot_tags_key_parts(&timeframe, ranked_only);

        let taggers_by_label =
            Taggers::get_cached_from_index(&hot_tags_input.timeframe, prefix, ranked_only).await?;
        let scores = HotTags::try_from_index_sorted_set(
            &key_parts,
            None,
            None,
            Some(hot_tags_input.skip),
            Some(hot_tags_input.limit),
            SortOrder::Descending,
            Some(prefix),
        )
        .await?;

        let (Some(scores), Some(taggers_by_label)) = (scores, taggers_by_label) else {
            let marked_empty =
                HotTags::is_marked_empty(&hot_tags_input.timeframe, prefix, ranked_only).await?;
            return Ok(marked_empty.then(HotTags::default));
        };

        let hot_tags = scores
            .into_iter()
            .filter_map(|(label, score)| {
                let cached = taggers_by_label.get(&label)?;
                Some(HotTag {
                    label,
                    taggers_id: Taggers(Taggers::get_taggers_by_pagination(
                        &cached.taggers,
                        0,
                        hot_tags_input.taggers_limit,
                    )),
                    tagged_count: score as u64,
                    taggers_count: cached.total,
                })
            })
            .collect();
        Ok(Some(hot_tags))
    }

    /// Overwrite taggers JSON and atomically replace the score set.
    ///
    /// * `ranked_only` - Write the cache variant that counts only ranked taggers
    async fn put_to_global_cache(
        hot_tags_list: HotTags,
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<()> {
        let timeframe_str = timeframe.to_string();
        let key_parts = Self::build_hot_tags_key_parts(&timeframe_str, ranked_only);
        let scores: Vec<(f64, &str)> = hot_tags_list
            .iter()
            .map(|tag| (tag.tagged_count as f64, tag.label.as_str()))
            .collect();
        let taggers: HashMap<String, CachedTaggers> = hot_tags_list
            .iter()
            .map(|tag| {
                (
                    tag.label.clone(),
                    CachedTaggers {
                        taggers: tag.taggers_id.clone(),
                        total: tag.taggers_count,
                    },
                )
            })
            .collect();

        Taggers::put_to_index(HotTagsTaggers(taggers), timeframe, prefix, ranked_only).await?;
        HotTags::replace_index_sorted_set(
            &key_parts,
            &scores,
            Some(prefix),
            Some(timeframe.to_cache_period()),
        )
        .await
    }

    fn build_hot_tags_key_parts(timeframe: &str, ranked_only: bool) -> Vec<&str> {
        hot_tags_key_parts(ranked_only, &[timeframe])
    }

    /// Warm AllTime and ThisMonth from the graph.
    pub async fn reindex() -> ModelResult<()> {
        let ranked_only = ranked_taggers_only().await;
        HotTags::fetch_and_cache_variant(&Timeframe::AllTime, ranked_only).await?;
        HotTags::fetch_and_cache_variant(&Timeframe::ThisMonth, ranked_only).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::get_redis_conn;
    use crate::db::kv::RedisError;
    use crate::{types::DynError, StackConfig, StackManager};
    use deadpool_redis::redis;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Off the production `HOT_TAGS_CACHE_PREFIX` keys the API tests share.
    const TEST_PREFIX: &str = "HotTagsCacheTest";
    /// The round-trip tests run on the unranked variant. That the ranked one is kept
    /// apart from it: [`ranked_cache_write_is_invisible_to_the_unranked_read`].
    const TEST_RANKED_ONLY: bool = false;
    /// Only the variant test writes under this prefix. The tests under `TEST_PREFIX`
    /// write the unranked variant of their timeframes and nextest runs them in
    /// parallel, which would make asserting a miss there flaky.
    const VARIANT_TEST_PREFIX: &str = "HotTagsCacheVariantTest";
    /// Only the empty-marker tests write under this prefix, each its own timeframe.
    const EMPTY_MARKER_TEST_PREFIX: &str = "HotTagsEmptyMarkerTest";

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_keeps_existing_ranking_on_empty_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::Today;
        HotTags::put_to_global_cache(
            HotTags(vec![
                hot_tag("bitcoin", 10, &["alice"]),
                hot_tag("nostr", 5, &["bob"]),
            ]),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;

        HotTags::write_or_preserve_cache(
            Some(HotTags::default()),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;
        assert_cached_labels(&timeframe, &["bitcoin", "nostr"]).await?;

        clear_test_cache(&timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_keeps_existing_ranking_on_none_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::ThisWeek;
        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("pubky", 20, &["carol"])]),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;

        HotTags::write_or_preserve_cache(None, &timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await?;
        assert_cached_labels(&timeframe, &["pubky"]).await?;

        clear_test_cache(&timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_replaces_existing_ranking_on_non_empty_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::ThisMonth;
        HotTags::put_to_global_cache(
            HotTags(vec![
                hot_tag("stale", 1, &["dave"]),
                hot_tag("dropped", 2, &["erin"]),
            ]),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;

        HotTags::write_or_preserve_cache(
            Some(HotTags(vec![hot_tag("fresh", 99, &["frank"])])),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;
        assert_cached_labels(&timeframe, &["fresh"]).await?;

        clear_test_cache(&timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await?;
        Ok(())
    }

    /// The graph caps `taggers_id` at `GLOBAL_HOT_TAGS_TAGGERS_LIMIT` but counts every
    /// distinct tagger, so `taggers_count` has to round-trip the cache on its own. Read
    /// back through `get_from_global_cache`, since deriving it from the stored sample is
    /// the regression this guards.
    #[tokio_shared_rt::test(shared)]
    async fn tagger_total_round_trips_the_cache_apart_from_the_sample() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::AllTime;
        let mut tag = hot_tag("bitcoin", 42, &["alice", "bob"]);
        tag.taggers_count = 137;
        HotTags::put_to_global_cache(
            HotTags(vec![tag]),
            &timeframe,
            TEST_PREFIX,
            TEST_RANKED_ONLY,
        )
        .await?;

        let cached = read_raw_taggers(&timeframe)
            .await?
            .expect("cache taggers must exist");
        let bitcoin = cached.get("bitcoin").expect("label must be cached");
        assert_eq!(bitcoin.total, 137, "the write must persist the count");
        assert_eq!(bitcoin.taggers.len(), 2, "the sample stays as written");

        let input = HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        );
        let hot_tags = HotTags::get_from_global_cache(&input, TEST_PREFIX, TEST_RANKED_ONLY)
            .await?
            .expect("the snapshot must be a cache hit");
        let [read_back] = &hot_tags.0[..] else {
            panic!("expected exactly one hot tag, got: {:?}", hot_tags.0);
        };
        assert_eq!(
            read_back.taggers_count, 137,
            "the read must report the graph's count, not the sample length"
        );
        assert_eq!(read_back.taggers_id.len(), 2, "the sample is unchanged");
        assert_eq!(
            read_back.tagged_count, 42,
            "the score survives the sorted set"
        );

        clear_test_cache(&timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await?;
        Ok(())
    }

    /// A write of the ranked variant must not be served to a request that asked for
    /// the unranked one. Runs under `VARIANT_TEST_PREFIX`, which no other test
    /// writes, so the unranked miss cannot be filled by a test running in parallel.
    #[tokio_shared_rt::test(shared)]
    async fn ranked_cache_write_is_invisible_to_the_unranked_read() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::Today;
        clear_test_cache(&timeframe, VARIANT_TEST_PREFIX, false).await?;
        clear_test_cache(&timeframe, VARIANT_TEST_PREFIX, true).await?;

        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("bitcoin", 10, &["alice", "bob"])]),
            &timeframe,
            VARIANT_TEST_PREFIX,
            true,
        )
        .await?;

        let input = HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        );
        let unranked = HotTags::get_from_global_cache(&input, VARIANT_TEST_PREFIX, false).await?;
        assert!(
            unranked.is_none(),
            "the unranked read must miss, got: {unranked:?}"
        );
        // The empty score set alone decides the miss above, so check the taggers apart
        let unranked_taggers = Taggers::get_from_index(&timeframe, VARIANT_TEST_PREFIX, false)
            .await?
            .unwrap_or_default();
        assert!(
            unranked_taggers.get("bitcoin").is_none(),
            "the ranked taggers must not land in the unranked map"
        );

        let ranked = HotTags::get_from_global_cache(&input, VARIANT_TEST_PREFIX, true)
            .await?
            .expect("the ranked read must be a cache hit");
        let [read_back] = &ranked.0[..] else {
            panic!("expected exactly one hot tag, got: {:?}", ranked.0);
        };
        assert_eq!(read_back.label, "bitcoin");
        assert_eq!(read_back.tagged_count, 10);
        assert_eq!(read_back.taggers_count, 2);
        assert_eq!(
            read_back.taggers_id.0,
            vec!["alice".to_string(), "bob".to_string()]
        );

        clear_test_cache(&timeframe, VARIANT_TEST_PREFIX, false).await?;
        clear_test_cache(&timeframe, VARIANT_TEST_PREFIX, true).await?;
        Ok(())
    }

    /// One request of each global reader against the `EMPTY_MARKER_TEST_PREFIX`
    /// cache, with a graph that holds no tags. Returns what the readers served.
    async fn read_empty_window(
        timeframe: &Timeframe,
        ranked_only: bool,
        empty_ttl: u64,
        scans: &AtomicUsize,
    ) -> ModelResult<(Option<HotTags>, Option<HotTagsTaggers>)> {
        let input = HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        );
        let scan = || async {
            scans.fetch_add(1, Ordering::SeqCst);
            Ok(Some(HotTags::default()))
        };
        let hot_tags = HotTags::read_cache_or_fill_from(
            timeframe,
            EMPTY_MARKER_TEST_PREFIX,
            ranked_only,
            empty_ttl,
            || HotTags::get_from_global_cache(&input, EMPTY_MARKER_TEST_PREFIX, ranked_only),
            scan,
        )
        .await?;
        let taggers = HotTags::read_cache_or_fill_from(
            timeframe,
            EMPTY_MARKER_TEST_PREFIX,
            ranked_only,
            empty_ttl,
            || Taggers::get_from_index(timeframe, EMPTY_MARKER_TEST_PREFIX, ranked_only),
            scan,
        )
        .await?;
        Ok((hot_tags, taggers))
    }

    #[tokio_shared_rt::test(shared)]
    async fn an_empty_scan_is_not_repeated_until_its_marker_expires() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::Today;
        let scans = AtomicUsize::new(0);

        // Two readers per round: the first one scans, every later one is a hit.
        for _ in 0..2 {
            let (hot_tags, taggers) = read_empty_window(&timeframe, true, 1, &scans).await?;
            let hot_tags = hot_tags.expect("an empty window must be served as a hit");
            assert!(hot_tags.is_empty(), "got: {hot_tags:?}");
            let taggers = taggers.expect("an empty window must be served as a hit");
            assert!(taggers.is_empty(), "got: {taggers:?}");
        }
        assert_eq!(
            scans.load(Ordering::SeqCst),
            1,
            "an empty window must be scanned once per marker TTL"
        );

        // The marker of one variant says nothing about the other.
        assert!(
            Taggers::get_from_index(&timeframe, EMPTY_MARKER_TEST_PREFIX, false)
                .await?
                .is_none(),
            "the ranked marker must not read as an unranked hit"
        );

        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        read_empty_window(&timeframe, true, 1, &scans).await?;
        assert_eq!(
            scans.load(Ordering::SeqCst),
            2,
            "an expired marker must let the window be scanned again"
        );
        Ok(())
    }

    /// The cache jobs write while a marker may still be alive; what they wrote
    /// must be served at once.
    #[tokio_shared_rt::test(shared)]
    async fn a_cache_write_is_served_over_a_live_empty_marker() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::ThisWeek;
        let scans = AtomicUsize::new(0);
        read_empty_window(&timeframe, true, 60, &scans).await?;

        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("bitcoin", 10, &["alice"])]),
            &timeframe,
            EMPTY_MARKER_TEST_PREFIX,
            true,
        )
        .await?;

        let (hot_tags, taggers) = read_empty_window(&timeframe, true, 60, &scans).await?;
        let labels: Vec<String> = hot_tags
            .expect("the write must be a cache hit")
            .iter()
            .map(|tag| tag.label.clone())
            .collect();
        assert_eq!(labels, ["bitcoin"]);
        assert!(taggers
            .expect("the write must be a cache hit")
            .get("bitcoin")
            .is_some());

        clear_test_cache(&timeframe, EMPTY_MARKER_TEST_PREFIX, true).await?;
        Ok(())
    }

    /// The full key the empty marker of one variant and timeframe lives under.
    fn test_empty_marker_key(timeframe: &Timeframe, ranked_only: bool) -> String {
        let timeframe = timeframe.to_string();
        let key = hot_tags_key_parts(ranked_only, &[EMPTY_HOT_TAGS, &timeframe]).join(":");
        format!("{EMPTY_MARKER_TEST_PREFIX}:{key}")
    }

    #[tokio_shared_rt::test(shared)]
    async fn the_empty_marker_is_a_plain_string_that_carries_its_ttl() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::ThisMonth;
        let marker_key = test_empty_marker_key(&timeframe, true);
        let mut conn = get_redis_conn().await?;
        let _: () = redis::cmd("DEL")
            .arg(&marker_key)
            .query_async(&mut conn)
            .await?;

        let scans = AtomicUsize::new(0);
        read_empty_window(&timeframe, true, 60, &scans).await?;

        let kind: String = redis::cmd("TYPE")
            .arg(&marker_key)
            .query_async(&mut conn)
            .await?;
        assert_eq!(kind, "string");
        let ttl: i64 = redis::cmd("TTL")
            .arg(&marker_key)
            .query_async(&mut conn)
            .await?;
        assert!(ttl > 0 && ttl <= 60, "expected a TTL in 1..=60, got {ttl}");

        let _: () = redis::cmd("DEL")
            .arg(&marker_key)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    fn hot_tag(label: &str, tagged_count: u64, taggers: &[&str]) -> HotTag {
        HotTag {
            label: label.to_string(),
            taggers_id: Taggers(taggers.iter().map(|id| id.to_string()).collect()),
            tagged_count,
            taggers_count: taggers.len(),
        }
    }

    async fn assert_cached_labels(
        timeframe: &Timeframe,
        expected: &[&str],
    ) -> Result<(), DynError> {
        let scores = read_raw_scores(timeframe)
            .await?
            .expect("cache scores must exist");
        let taggers = read_raw_taggers(timeframe)
            .await?
            .expect("cache taggers must exist");
        assert_eq!(scores.len(), expected.len());
        assert_eq!(taggers.len(), expected.len());
        for label in expected {
            assert!(
                scores.iter().any(|(cached, _)| cached == label),
                "missing {label} in scores"
            );
            assert!(taggers.get(*label).is_some(), "missing {label} in taggers");
        }
        Ok(())
    }

    async fn read_raw_scores(timeframe: &Timeframe) -> RedisResult<Option<Vec<(String, f64)>>> {
        let timeframe_str = timeframe.to_string();
        let key_parts = HotTags::build_hot_tags_key_parts(&timeframe_str, TEST_RANKED_ONLY);
        HotTags::try_from_index_sorted_set(
            &key_parts,
            None,
            None,
            Some(0),
            Some(GLOBAL_HOT_TAGS_CACHE_SIZE),
            SortOrder::Descending,
            Some(TEST_PREFIX),
        )
        .await
    }

    async fn read_raw_taggers(timeframe: &Timeframe) -> RedisResult<Option<HotTagsTaggers>> {
        Taggers::get_from_index(timeframe, TEST_PREFIX, TEST_RANKED_ONLY).await
    }

    async fn clear_test_cache(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<()> {
        let timeframe_str = timeframe.to_string();
        let key_parts = HotTags::build_hot_tags_key_parts(&timeframe_str, ranked_only);
        HotTags::replace_index_sorted_set(&key_parts, &[], Some(prefix), None).await?;
        Taggers::put_to_index(
            HotTagsTaggers(HashMap::new()),
            timeframe,
            prefix,
            ranked_only,
        )
        .await
    }

    #[tokio::test]
    async fn read_or_fill_serves_a_hit_without_filling() -> Result<(), DynError> {
        let cache = Cell::new(Some("cached"));

        let served = read_or_fill(
            || async { Ok(cache.get()) },
            || async {
                cache.set(Some("filled"));
                Ok(())
            },
        )
        .await?;

        assert_eq!(served, Some("cached"));
        assert_eq!(cache.get(), Some("cached"), "a hit must not run the fill");
        Ok(())
    }

    #[tokio::test]
    async fn read_or_fill_fills_a_miss_and_serves_what_was_filled() -> Result<(), DynError> {
        let cache = Cell::new(None);

        let served = read_or_fill(
            || async { Ok(cache.get()) },
            || async {
                cache.set(Some("filled"));
                Ok(())
            },
        )
        .await?;

        assert_eq!(served, Some("filled"));
        Ok(())
    }

    #[tokio::test]
    async fn read_or_fill_stays_a_miss_when_the_fill_writes_nothing() -> Result<(), DynError> {
        let served: Option<&str> = read_or_fill(|| async { Ok(None) }, || async { Ok(()) }).await?;

        assert_eq!(served, None);
        Ok(())
    }

    #[tokio::test]
    async fn read_or_fill_surfaces_a_failed_read_without_filling() {
        let filled = Cell::new(false);

        let served: ModelResult<Option<&str>> = read_or_fill(
            || async { Err(RedisError::ConnectionNotInitialized) },
            || async {
                filled.set(true);
                Ok(())
            },
        )
        .await;

        assert!(served.is_err(), "a failed read must not read as a miss");
        assert!(!filled.get(), "a failed read must not start a scan");
    }

    #[test]
    fn a_failed_ranking_read_serves_hot_tags_unfiltered() {
        assert!(ranked_only_from(Ok(true)));
        assert!(!ranked_only_from(Ok(false)));
        assert!(!ranked_only_from(Err(RedisError::ConnectionNotInitialized)));
    }

    #[test]
    fn ranked_hot_tags_live_under_their_own_keys() {
        assert_eq!(
            HotTags::build_hot_tags_key_parts("AllTime", false),
            ["Tags", "Post", "Hot", "AllTime"]
        );
        assert_eq!(
            HotTags::build_hot_tags_key_parts("AllTime", true),
            ["Tags", "Post", "Hot", "Ranked", "AllTime"]
        );
        assert_eq!(
            hot_tags_key_parts(true, &["Taggers", "AllTime"]),
            ["Tags", "Post", "Hot", "Ranked", "Taggers", "AllTime"]
        );
    }
}
