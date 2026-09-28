use super::{
    stream::{hot_tags_key_parts, ranked_taggers_only, HotTags, HOT_TAGS_CACHE_PREFIX},
    Taggers as TaggersType,
};
use crate::db::{fetch_key_from_graph, kv::RedisResult, queries, GraphResult, RedisOps};
use crate::models::error::ModelResult;
use crate::types::{StreamReach, Timeframe};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref};
use utoipa::ToSchema;

/// Versioned: `Taggers` holds the pre-[`CachedTaggers`] shape, a bare id array per
/// label, which this type cannot deserialize. A new segment lets those keys age out
/// on their own TTL instead of erroring every read until they do.
pub const TAGGERS_INDEX: &str = "TaggersV2";

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
pub struct Taggers(pub TaggersType);

impl Deref for Taggers {
    type Target = TaggersType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl RedisOps for Taggers {}

impl AsRef<[String]> for Taggers {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

/// One label's cached taggers. `total` is the graph's distinct tagger count, which
/// can exceed `taggers.len()`: the list is a sample capped at write time, so it
/// cannot stand in for the count.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CachedTaggers {
    pub taggers: Taggers,
    pub total: usize,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HotTagsTaggers(pub HashMap<String, CachedTaggers>);

impl RedisOps for HotTagsTaggers {}

impl Deref for HotTagsTaggers {
    type Target = HashMap<String, CachedTaggers>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Taggers {
    /// Cached taggers map for one timeframe. An empty map if the key is missing
    /// and the variant's empty marker is alive.
    ///
    /// * `ranked_only` - Read the cache variant that counts only ranked taggers
    pub async fn get_from_index(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<Option<HotTagsTaggers>> {
        if let Some(cached) = Self::get_cached_from_index(timeframe, prefix, ranked_only).await? {
            return Ok(Some(cached));
        }
        let marked_empty = HotTags::is_marked_empty(timeframe, prefix, ranked_only).await?;
        Ok(marked_empty.then(HotTagsTaggers::default))
    }

    /// The taggers map as written, `None` if the key is missing whether or not
    /// an empty marker is alive.
    pub(super) async fn get_cached_from_index(
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<Option<HotTagsTaggers>> {
        let timeframe_str = timeframe.to_string();
        HotTagsTaggers::try_from_index_json(
            &Self::build_key_parts(&timeframe_str, ranked_only),
            Some(prefix.into()),
        )
        .await
    }

    /// Overwrites the timeframe's taggers JSON and arms its TTL.
    ///
    /// * `ranked_only` - Write the cache variant that counts only ranked taggers
    pub async fn put_to_index(
        taggers: HotTagsTaggers,
        timeframe: &Timeframe,
        prefix: &str,
        ranked_only: bool,
    ) -> RedisResult<()> {
        let timeframe_str = timeframe.to_string();
        taggers
            .put_index_json(
                &Self::build_key_parts(&timeframe_str, ranked_only),
                Some(prefix.to_string()),
                Some(timeframe.to_cache_period()),
            )
            .await
    }

    /// Global taggers come from the hot-tags cache, which a miss fills from the
    /// graph first; reach-scoped taggers hit the graph.
    ///
    /// Once a trust ranking exists, only taggers with a positive trust score
    /// are returned, on both paths.
    pub async fn get_global_taggers(
        label: String,
        user_id: Option<String>,
        reach: Option<StreamReach>,
        skip: usize,
        limit: usize,
        timeframe: Timeframe,
    ) -> ModelResult<Option<TaggersType>> {
        let ranked_only = ranked_taggers_only().await;
        Ok(match user_id {
            None => {
                Self::get_from_global_timeline(&label, skip, limit, &timeframe, ranked_only).await?
            }
            Some(id) => {
                Self::get_tag_taggers_by_reach(
                    &label,
                    &id,
                    reach.unwrap_or(StreamReach::Following),
                    skip,
                    limit,
                    ranked_only,
                )
                .await?
            }
        })
    }

    /// Page of taggers for `label` from the cached map. A missing timeframe key
    /// is filled from the graph first; `None` if the label is not in the snapshot.
    ///
    /// * `ranked_only` - Read the cache variant that counts only ranked taggers
    async fn get_from_global_timeline(
        label: &str,
        skip: usize,
        limit: usize,
        timeframe: &Timeframe,
        ranked_only: bool,
    ) -> ModelResult<Option<TaggersType>> {
        let by_label = HotTags::read_global_cache_or_fill(timeframe, ranked_only, || {
            Self::get_from_index(timeframe, HOT_TAGS_CACHE_PREFIX, ranked_only)
        })
        .await?;
        Ok(by_label
            .as_ref()
            .and_then(|by_label| by_label.get(label))
            .map(|cached| Self::get_taggers_by_pagination(&cached.taggers, skip, limit)))
    }

    /// Slice a cached tagger list. Used by both the taggers route and hot-tag reconstruction.
    pub fn get_taggers_by_pagination(
        taggers_list: &Taggers,
        skip: usize,
        limit: usize,
    ) -> TaggersType {
        taggers_list
            .iter()
            .skip(skip)
            .take(limit)
            .cloned()
            .collect()
    }

    /// `ranked_only` returns only taggers with a positive trust score.
    async fn get_tag_taggers_by_reach(
        label: &str,
        user_id: &str,
        reach: StreamReach,
        skip: usize,
        limit: usize,
        ranked_only: bool,
    ) -> GraphResult<Option<TaggersType>> {
        let query =
            queries::get::get_tag_taggers_by_reach(label, user_id, reach, skip, limit, ranked_only);
        fetch_key_from_graph::<TaggersType>(query, "tagger_ids").await
    }

    fn build_key_parts(timeframe: &str, ranked_only: bool) -> Vec<&str> {
        hot_tags_key_parts(ranked_only, &[TAGGERS_INDEX, timeframe])
    }
}
