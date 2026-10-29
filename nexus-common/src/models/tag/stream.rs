use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{fetch_key_from_graph, get_redis_conn, queries, RedisOps};
use crate::models::error::ModelResult;
use crate::types::routes::HotTagsInputDTO;
use crate::types::{StreamReach, Timeframe};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use tracing::{debug, warn};
use utoipa::ToSchema;

use super::global::{CachedTaggers, HotTagsTaggers, Taggers};
use super::TaggedType;

pub const HOT_TAGS_CACHE_PREFIX: &str = "Cache";
pub const POST_HOT_TAGS: [&str; 3] = ["Tags", "Post", "Hot"];
/// The global cache under [`HotTagsRule::Trust`], apart from [`POST_HOT_TAGS`]:
/// a flip never serves an order built under the other rule.
const TRUST_RANKED_POST_HOT_TAGS: [&str; 4] = ["Tags", "Post", "Hot", "Trust"];
/// Present while hot tags rank by tagger trust: the switch every service
/// follows, which the watcher stores from `[features] trust_ranked_hot_tags`
/// at startup.
const TRUST_RANKED_KEY: &str = "HotTags:TrustRanked";
/// Snapshot size per timeframe. A skip past this cannot be filled from cache.
pub const GLOBAL_HOT_TAGS_CACHE_SIZE: usize = 100;
const GLOBAL_HOT_TAGS_TAGGERS_LIMIT: usize = 20;

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct HotTag {
    pub label: String,
    /// The most trusted taggers first, ties by id.
    pub taggers_id: Taggers,
    pub tagged_count: u64,
    pub taggers_count: usize,
    /// Each tagger's trust times 1 + 1/2 + ... + 1/n, summed over the taggers,
    /// n being the posts they tagged with the label in the timeframe. The list
    /// is ordered by it or by post count.
    pub score: f64,
}

/// How hot tags are ordered, as `[features] trust_ranked_hot_tags` selects.
/// Both report the same counts, score and taggers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotTagsRule {
    /// By posts tagged with the label.
    Posts,
    /// By [`HotTag::score`].
    Trust,
}

impl HotTagsRule {
    /// Stores the switch every service follows.
    pub(crate) async fn switch(trust_ranked: bool) -> RedisResult<()> {
        let mut conn = get_redis_conn().await?;
        let _: () = match trust_ranked {
            true => conn.set(TRUST_RANKED_KEY, 1).await?,
            false => conn.del(TRUST_RANKED_KEY).await?,
        };
        Ok(())
    }

    /// The rule the stored switch selects, [`HotTagsRule::Posts`] without one.
    pub(crate) async fn stored() -> RedisResult<Self> {
        let mut conn = get_redis_conn().await?;
        let trust_ranked: bool = conn.exists(TRUST_RANKED_KEY).await?;
        Ok(match trust_ranked {
            true => HotTagsRule::Trust,
            false => HotTagsRule::Posts,
        })
    }

    /// Where the rule keeps its global cache.
    pub(super) fn cache_key_parts(self) -> &'static [&'static str] {
        match self {
            HotTagsRule::Posts => &POST_HOT_TAGS,
            HotTagsRule::Trust => &TRUST_RANKED_POST_HOT_TAGS,
        }
    }
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
        let rule = HotTagsRule::stored().await?;
        match user_id {
            Some(user_id) => {
                HotTags::get_hot_tags_by_reach(
                    user_id,
                    reach.unwrap_or(StreamReach::Following),
                    hot_tags_input,
                    rule,
                )
                .await
            }
            None => HotTags::get_global_hot_tags(hot_tags_input, rule).await,
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
    /// * `rule` - How the list is ordered
    async fn get_hot_tags_by_reach(
        user_id: String,
        reach: StreamReach,
        hot_tags_input: &HotTagsInputDTO,
        rule: HotTagsRule,
    ) -> ModelResult<Option<HotTags>> {
        let query =
            queries::get::get_hot_tags_by_reach(user_id.as_str(), reach, hot_tags_input, rule);
        fetch_key_from_graph::<HotTags>(query, "hot_tags")
            .await
            .map_err(Into::into)
    }

    /// Cache hit, including an empty page. On a missing key, refresh then re-read
    /// so `skip`/`limit`/`taggers_limit` apply to the snapshot, not the graph result.
    async fn get_global_hot_tags(
        hot_tags_input: &HotTagsInputDTO,
        rule: HotTagsRule,
    ) -> ModelResult<Option<HotTags>> {
        if let Some(cached) =
            HotTags::get_from_global_cache(hot_tags_input, rule, HOT_TAGS_CACHE_PREFIX).await?
        {
            return Ok(Some(cached));
        }

        // A skip past the snapshot is an empty page no matter what the graph holds,
        // so refreshing for it would only let a caller walk `skip` to force scans.
        if hot_tags_input.skip >= GLOBAL_HOT_TAGS_CACHE_SIZE {
            return Ok(Some(HotTags::default()));
        }

        HotTags::fetch_and_cache_by_rule(&hot_tags_input.timeframe, rule).await?;
        HotTags::get_from_global_cache(hot_tags_input, rule, HOT_TAGS_CACHE_PREFIX)
            .await
            .map_err(Into::into)
    }

    /// Scan the top [`GLOBAL_HOT_TAGS_CACHE_SIZE`] post tags and replace the cache
    /// of the stored rule. A result with no tags leaves the previous ranking in place.
    pub async fn fetch_and_cache(timeframe: &Timeframe) -> ModelResult<()> {
        HotTags::fetch_and_cache_by_rule(timeframe, HotTagsRule::stored().await?).await
    }

    async fn fetch_and_cache_by_rule(timeframe: &Timeframe, rule: HotTagsRule) -> ModelResult<()> {
        let query_input = HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        );
        let query = queries::get::get_global_hot_tags(&query_input, rule);
        let result = fetch_key_from_graph::<HotTags>(query, "hot_tags").await?;
        HotTags::write_or_preserve_cache(result, timeframe, rule, HOT_TAGS_CACHE_PREFIX).await
    }

    /// A result with tags replaces both keys; anything else is a no-op.
    /// Tests pass their own `prefix` so they never touch production keys.
    async fn write_or_preserve_cache(
        result: Option<HotTags>,
        timeframe: &Timeframe,
        rule: HotTagsRule,
        prefix: &str,
    ) -> ModelResult<()> {
        match result {
            Some(hot_tags) if !hot_tags.is_empty() => {
                debug!(%timeframe, count = hot_tags.len(), "Writing hot tags cache");
                HotTags::put_to_global_cache(hot_tags, timeframe, rule, prefix).await?;
            }
            _ => warn!(%timeframe, "Graph returned no hot tags — previous cache left untouched"),
        }
        Ok(())
    }

    /// `None` if either key is missing. `Some([])` if both exist but this window is empty.
    /// The order comes from the sorted set, everything else from the JSON.
    async fn get_from_global_cache(
        hot_tags_input: &HotTagsInputDTO,
        rule: HotTagsRule,
        prefix: &str,
    ) -> RedisResult<Option<HotTags>> {
        let timeframe = hot_tags_input.timeframe.to_string();
        let key_parts = Self::build_hot_tags_key_parts(rule, &timeframe);

        let cached_by_label =
            Taggers::get_from_index(&hot_tags_input.timeframe, rule, prefix).await?;
        let ranking = HotTags::try_from_index_sorted_set(
            &key_parts,
            None,
            None,
            Some(hot_tags_input.skip),
            Some(hot_tags_input.limit),
            SortOrder::Descending,
            Some(prefix),
        )
        .await?;

        let (Some(ranking), Some(cached_by_label)) = (ranking, cached_by_label) else {
            return Ok(None);
        };

        let hot_tags = ranking
            .into_iter()
            .filter_map(|(label, _)| {
                let cached = cached_by_label.get(&label)?;
                Some(HotTag {
                    label,
                    taggers_id: Taggers(Taggers::get_taggers_by_pagination(
                        &cached.taggers,
                        0,
                        hot_tags_input.taggers_limit,
                    )),
                    tagged_count: cached.tagged_count,
                    taggers_count: cached.total,
                    score: cached.score,
                })
            })
            .collect();
        Ok(Some(hot_tags))
    }

    /// Overwrite the JSON and atomically replace the sorted set. Its member scores
    /// are the ranks, first label highest: a sorted set orders equal scores by
    /// label, descending, so storing the rule's own score would drop the graph's
    /// tie-breaks, and under the trust rule every label without a trusted tagger
    /// ties at zero.
    async fn put_to_global_cache(
        hot_tags_list: HotTags,
        timeframe: &Timeframe,
        rule: HotTagsRule,
        prefix: &str,
    ) -> RedisResult<()> {
        let timeframe_str = timeframe.to_string();
        let key_parts = Self::build_hot_tags_key_parts(rule, &timeframe_str);
        let ranking: Vec<(f64, &str)> = hot_tags_list
            .iter()
            .enumerate()
            .map(|(rank, tag)| ((hot_tags_list.len() - rank) as f64, tag.label.as_str()))
            .collect();
        let cached_by_label: HashMap<String, CachedTaggers> = hot_tags_list
            .iter()
            .map(|tag| {
                (
                    tag.label.clone(),
                    CachedTaggers {
                        taggers: tag.taggers_id.clone(),
                        total: tag.taggers_count,
                        tagged_count: tag.tagged_count,
                        score: tag.score,
                    },
                )
            })
            .collect();

        Taggers::put_to_index(HotTagsTaggers(cached_by_label), timeframe, rule, prefix).await?;
        HotTags::replace_index_sorted_set(
            &key_parts,
            &ranking,
            Some(prefix),
            Some(timeframe.to_cache_period()),
        )
        .await
    }

    /// Drops the rule's global cache for `timeframe`; the next read refreshes it.
    pub async fn clear_global_cache(timeframe: &Timeframe, rule: HotTagsRule) -> RedisResult<()> {
        HotTags::clear_cache(timeframe, rule, HOT_TAGS_CACHE_PREFIX).await
    }

    async fn clear_cache(
        timeframe: &Timeframe,
        rule: HotTagsRule,
        prefix: &str,
    ) -> RedisResult<()> {
        let timeframe_str = timeframe.to_string();
        let key_parts = Self::build_hot_tags_key_parts(rule, &timeframe_str);
        HotTags::replace_index_sorted_set(&key_parts, &[], Some(prefix), None).await?;
        Taggers::clear_index(timeframe, rule, prefix).await
    }

    fn build_hot_tags_key_parts(rule: HotTagsRule, timeframe: &str) -> Vec<&str> {
        [rule.cache_key_parts(), &[timeframe]].concat()
    }

    /// Warm AllTime and ThisMonth from the graph.
    pub async fn reindex() -> ModelResult<()> {
        HotTags::fetch_and_cache(&Timeframe::AllTime).await?;
        HotTags::fetch_and_cache(&Timeframe::ThisMonth).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::DynError, StackConfig, StackManager};

    /// Off the production `HOT_TAGS_CACHE_PREFIX` keys the API tests share. The
    /// tests run in parallel, so each writes a timeframe of its own under it, or
    /// a prefix of its own.
    const TEST_PREFIX: &str = "HotTagsCacheTest";

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_keeps_existing_ranking_on_empty_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let (timeframe, rule) = (Timeframe::Today, HotTagsRule::Trust);
        HotTags::put_to_global_cache(
            HotTags(vec![
                hot_tag("bitcoin", 10, &["alice"]),
                hot_tag("nostr", 5, &["bob"]),
            ]),
            &timeframe,
            rule,
            TEST_PREFIX,
        )
        .await?;

        HotTags::write_or_preserve_cache(Some(HotTags::default()), &timeframe, rule, TEST_PREFIX)
            .await?;
        assert_cached_labels(&timeframe, rule, &["bitcoin", "nostr"]).await?;

        HotTags::clear_cache(&timeframe, rule, TEST_PREFIX).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_keeps_existing_ranking_on_none_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let (timeframe, rule) = (Timeframe::ThisWeek, HotTagsRule::Trust);
        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("pubky", 20, &["carol"])]),
            &timeframe,
            rule,
            TEST_PREFIX,
        )
        .await?;

        HotTags::write_or_preserve_cache(None, &timeframe, rule, TEST_PREFIX).await?;
        assert_cached_labels(&timeframe, rule, &["pubky"]).await?;

        HotTags::clear_cache(&timeframe, rule, TEST_PREFIX).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn write_or_preserve_cache_replaces_existing_ranking_on_non_empty_graph_result(
    ) -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let (timeframe, rule) = (Timeframe::ThisMonth, HotTagsRule::Trust);
        HotTags::put_to_global_cache(
            HotTags(vec![
                hot_tag("stale", 1, &["dave"]),
                hot_tag("dropped", 2, &["erin"]),
            ]),
            &timeframe,
            rule,
            TEST_PREFIX,
        )
        .await?;

        HotTags::write_or_preserve_cache(
            Some(HotTags(vec![hot_tag("fresh", 99, &["frank"])])),
            &timeframe,
            rule,
            TEST_PREFIX,
        )
        .await?;
        assert_cached_labels(&timeframe, rule, &["fresh"]).await?;

        HotTags::clear_cache(&timeframe, rule, TEST_PREFIX).await?;
        Ok(())
    }

    /// The graph caps `taggers_id` at `GLOBAL_HOT_TAGS_TAGGERS_LIMIT` but counts every
    /// distinct tagger, so `taggers_count` has to round-trip the cache on its own. Read
    /// back through `get_from_global_cache`, since deriving it from the stored sample is
    /// the regression this guards. The sorted set holds only the ranks, so
    /// `tagged_count` and `score` round-trip apart from it, under either rule.
    #[tokio_shared_rt::test(shared)]
    async fn tagger_total_round_trips_the_cache_apart_from_the_sample() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let timeframe = Timeframe::AllTime;
        for rule in [HotTagsRule::Posts, HotTagsRule::Trust] {
            let mut tag = hot_tag("bitcoin", 42, &["alice", "bob"]);
            tag.taggers_count = 137;
            tag.score = 0.25;
            HotTags::put_to_global_cache(HotTags(vec![tag]), &timeframe, rule, TEST_PREFIX).await?;

            let cached = Taggers::get_from_index(&timeframe, rule, TEST_PREFIX)
                .await?
                .expect("cache taggers must exist");
            let bitcoin = cached.get("bitcoin").expect("label must be cached");
            assert_eq!(bitcoin.total, 137, "the write must persist the count");
            assert_eq!(bitcoin.taggers.len(), 2, "the sample stays as written");

            let hot_tags =
                HotTags::get_from_global_cache(&whole_snapshot(&timeframe), rule, TEST_PREFIX)
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
                "{rule:?}: the post count survives"
            );
            assert_eq!(read_back.score, 0.25, "{rule:?}: the score survives");

            HotTags::clear_cache(&timeframe, rule, TEST_PREFIX).await?;
        }
        Ok(())
    }

    /// Each rule keeps its own keys, so a flip never serves an order built under
    /// the other rule: the first read after it misses and refreshes.
    #[tokio_shared_rt::test(shared)]
    async fn hot_tags_cache_keys_differ_by_rule() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        const PREFIX: &str = "HotTagsCacheTest:Rules";
        let timeframe = Timeframe::Today;
        let input = whole_snapshot(&timeframe);
        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("by-posts", 5, &["alice"])]),
            &timeframe,
            HotTagsRule::Posts,
            PREFIX,
        )
        .await?;
        let trust_before = HotTags::get_from_global_cache(&input, HotTagsRule::Trust, PREFIX).await;

        HotTags::put_to_global_cache(
            HotTags(vec![hot_tag("by-trust", 1, &["bob"])]),
            &timeframe,
            HotTagsRule::Trust,
            PREFIX,
        )
        .await?;
        let posts = HotTags::get_from_global_cache(&input, HotTagsRule::Posts, PREFIX).await;
        let trust = HotTags::get_from_global_cache(&input, HotTagsRule::Trust, PREFIX).await;
        for rule in [HotTagsRule::Posts, HotTagsRule::Trust] {
            HotTags::clear_cache(&timeframe, rule, PREFIX).await?;
        }

        assert!(
            trust_before?.is_none(),
            "the trust rule must not read the post ranking"
        );
        assert_eq!(labels(posts?), ["by-posts"]);
        assert_eq!(labels(trust?), ["by-trust"]);
        Ok(())
    }

    fn hot_tag(label: &str, tagged_count: u64, taggers: &[&str]) -> HotTag {
        HotTag {
            label: label.to_string(),
            taggers_id: Taggers(taggers.iter().map(|id| id.to_string()).collect()),
            tagged_count,
            taggers_count: taggers.len(),
            score: 0.0,
        }
    }

    /// The whole cached snapshot of `timeframe`, as one page.
    fn whole_snapshot(timeframe: &Timeframe) -> HotTagsInputDTO {
        HotTagsInputDTO::new(
            timeframe.clone(),
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            GLOBAL_HOT_TAGS_TAGGERS_LIMIT,
            Some(TaggedType::Post),
        )
    }

    fn labels(hot_tags: Option<HotTags>) -> Vec<String> {
        let hot_tags = hot_tags.expect("the snapshot must be a cache hit");
        hot_tags.iter().map(|tag| tag.label.clone()).collect()
    }

    async fn assert_cached_labels(
        timeframe: &Timeframe,
        rule: HotTagsRule,
        expected: &[&str],
    ) -> Result<(), DynError> {
        let timeframe_str = timeframe.to_string();
        let ranking = HotTags::try_from_index_sorted_set(
            &HotTags::build_hot_tags_key_parts(rule, &timeframe_str),
            None,
            None,
            Some(0),
            Some(GLOBAL_HOT_TAGS_CACHE_SIZE),
            SortOrder::Descending,
            Some(TEST_PREFIX),
        )
        .await?
        .expect("cache ranking must exist");
        let taggers = Taggers::get_from_index(timeframe, rule, TEST_PREFIX)
            .await?
            .expect("cache taggers must exist");
        assert_eq!(ranking.len(), expected.len());
        assert_eq!(taggers.len(), expected.len());
        for label in expected {
            assert!(
                ranking.iter().any(|(cached, _)| cached == label),
                "missing {label} in the ranking"
            );
            assert!(taggers.get(*label).is_some(), "missing {label} in taggers");
        }
        Ok(())
    }
}
