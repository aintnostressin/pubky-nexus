use anyhow::Result;
use axum::http::StatusCode;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use nexus_common::models::tag::global::TAGGERS_INDEX;
use nexus_common::models::tag::stream::{hot_tags_key_parts, HOT_TAGS_CACHE_PREFIX};
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};
use nexus_common::types::Timeframe;
use serde_json::Value;

use crate::utils::server::TestServiceServer;
use crate::utils::{get_request, invalid_get_request};

const PEER_PUBKY: &str = "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";
// mocks/hot-tags.cypher users
pub const USER_1: &str = "pyc598poqkdgtx1wc4aeptx67mqg71mmywyh7uzkffzittjmbiuo";
const USER_4: &str = "r91hi8kc3x6761gwfiigr7yn6nca1z47wm6jadhw1jbx1co93r9y";
const USER_5: &str = "tkpeqpx3ywoawiw6q8e6kuo9o3egr7fnhx83rudznbrrmqgdmomo";

// mocks/wot.cypher: the observer follows D1, D1B and the mod bot. D1 and D1B
// carry trust; the mod bot is one of the on-ramp accounts trust.cypher leaves
// unranked, so its tags are hidden once the ranking exists (it does, db mock
// builds it from the fixture).
const WOT_OBSERVER: &str = "y6apowjmcg8rocmd9jirg95fyf3yykwuhqxozzts4mjipk4n7iao";
const WOT_D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
const WOT_D1B: &str = "t5ixbtatg4tq5q5ixg16qqrg1bmem75ksg6cweuftuydwzw91pzy";
const WOT_MODBOT: &str = "qsfngw6xm9kk7yp99xustjfj8mu9auufkixas5f8goeujuxt45ao";
// A label the global snapshot serves, tagged by ranked skunk users and, on one
// more post, by the mod bot.
const MODBOT_HOT_LABEL: &str = "sentimental";

struct StreamTagMockup {
    label: String,
    tagger_ids: usize,
    tagged_count: u64,
    taggers_count: usize,
}

impl StreamTagMockup {
    fn new(label: String, tagger_ids: usize, tagged_count: u64, taggers_count: usize) -> Self {
        Self {
            label,
            tagger_ids,
            tagged_count,
            taggers_count,
        }
    }
}

// Small unit test to compare all the tags composition
fn analyse_hot_tags_structure(tags: &Vec<Value>) {
    for tag in tags {
        assert!(tag["label"].is_string(), "label should be a string");
        assert!(
            tag["taggers_id"].is_array(),
            "tagger_ids should be an array"
        );
        assert!(
            tag["tagged_count"].is_number(),
            "tagged_count should be a number"
        );
        assert!(
            tag["taggers_count"].is_number(),
            "taggers_count should be a number"
        );
    }
}

// Small unit test to compare the tag properties
fn compare_unit_hot_tag(tag: &Value, hot_tag: StreamTagMockup) {
    assert_eq!(tag["tagged_count"], hot_tag.tagged_count);
    assert_eq!(tag["label"], hot_tag.label);
    assert_eq!(tag["taggers_count"], hot_tag.taggers_count);
    let tagger_ids = tag["taggers_id"].as_array().unwrap();
    assert_eq!(tagger_ids.len(), hot_tag.tagger_ids);
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags() -> Result<()> {
    let body = get_request("/v0/tags/hot").await?;

    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 4th index
    let hot_tag = StreamTagMockup::new(String::from("pubky"), 9, 15, 9);
    compare_unit_hot_tag(&tags[4], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_with_today_timeframe() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=today").await?;

    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    let hot_tag = StreamTagMockup::new(String::from("today"), 3, 3, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_with_this_week_timeframe() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=this_week").await?;

    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    analyse_hot_tags_structure(tags);

    // "today" tagged items are within 7 days, so they should appear
    let hot_tag = StreamTagMockup::new(String::from("today"), 3, 3, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_with_this_month_timeframe() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=this_month").await?;

    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the first tag
    let hot_tag = StreamTagMockup::new(String::from("today"), 3, 3, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_skip_limit() -> Result<()> {
    let body = get_request("/v0/tags/hot?skip=3&limit=5").await?;

    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // assert limit
    assert_eq!(tags.len(), 5);

    // Analyse the first tag
    let hot_tag = StreamTagMockup::new(String::from("ha"), 9, 16, 9);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

/// Global is a 100-entry cache, including AllTime. A skip past it is an empty
/// page, not an error, so a client paging by `limit` terminates the same way at
/// every `limit`. Reach still pages on the graph.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_skip_past_cache() -> Result<()> {
    for skip in [100, 101, 10_000] {
        let body = get_request(&format!("/v0/tags/hot?skip={skip}")).await?;
        assert_eq!(
            body.as_array().map(Vec::len),
            Some(0),
            "skip={skip} must yield an empty page, got: {body}"
        );
    }

    let body = get_request(&format!(
        "/v0/tags/hot?user_id={PEER_PUBKY}&reach=following&skip=101"
    ))
    .await?;
    assert!(body.is_array());

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=following");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("pubky"), 4, 5, 4);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach_and_today_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_1}&reach=following&timeframe=today");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 3);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("today"), 2, 2, 2);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach_and_month_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_1}&reach=following&timeframe=this_month");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 5);
    let hot_tag = StreamTagMockup::new(String::from("month"), 3, 2, 3);

    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach_and_month_timeframe_skip_and_limit() -> Result<()> {
    let endpoint = &format!(
        "/v0/tags/hot?user_id={USER_1}&reach=following&timeframe=this_month&skip=1&limit=2"
    );

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 2);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("today"), 2, 2, 2);
    compare_unit_hot_tag(&tags[0], hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("tag1"), 3, 1, 3);
    compare_unit_hot_tag(&tags[1], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach_and_all_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_1}&reach=following&timeframe=all_time");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 6);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("all"), 4, 2, 4);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_reach_with_skip_limit() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=following&skip=3&limit=1",);

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("💯"), 1, 3, 1);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_reach_no_user_id() -> Result<()> {
    let endpoint = "/v0/tags/hot?reach=following";

    invalid_get_request(endpoint, StatusCode::BAD_REQUEST).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_reach_no_reach() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}");

    invalid_get_request(endpoint, StatusCode::BAD_REQUEST).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_following_using_taggers_limit() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=following&taggers_limit=3",);

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("pubky"), 3, 5, 4);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=followers");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Post stream should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 1st index
    let hot_tag = StreamTagMockup::new(String::from("pubky"), 2, 3, 2);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach_with_skip_limit() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=followers&skip=3&limit=1",);

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("✅"), 1, 2, 1);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach_and_today_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_4}&reach=followers&timeframe=today");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 3);

    let hot_tag = StreamTagMockup::new(String::from("today"), 3, 3, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach_and_month_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_4}&reach=followers&timeframe=this_month");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 5);

    let hot_tag = StreamTagMockup::new(String::from("month"), 3, 3, 3);

    // Analyse the tag that is in the 0 index
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach_and_month_timeframe_skip_and_limit() -> Result<()> {
    let endpoint = &format!(
        "/v0/tags/hot?user_id={USER_4}&reach=followers&timeframe=this_month&skip=1&limit=2"
    );

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 2);

    let hot_tag = StreamTagMockup::new(String::from("today"), 3, 3, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("tag1"), 3, 2, 3);
    compare_unit_hot_tag(&tags[1], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_followers_reach_and_all_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_4}&reach=followers&timeframe=all_time");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 6);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("all"), 4, 3, 4);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=friends");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Post stream should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 1st index
    let hot_tag = StreamTagMockup::new(String::from("pubky"), 2, 3, 2);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach_with_skip_limit() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach=friends&skip=2&limit=1",);

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("bitkit"), 2, 2, 2);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach_and_today_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_5}&reach=friends&timeframe=today");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 1);

    let hot_tag = StreamTagMockup::new(String::from("today"), 1, 1, 1);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach_and_month_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_5}&reach=friends&timeframe=this_month");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 4);
    let hot_tag = StreamTagMockup::new(String::from("month"), 1, 1, 1);

    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach_and_month_timeframe_skip_and_limit() -> Result<()> {
    let endpoint =
        &format!("/v0/tags/hot?user_id={USER_5}&reach=friends&timeframe=this_month&skip=1&limit=2");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 2);

    let hot_tag = StreamTagMockup::new(String::from("tag1"), 1, 1, 1);
    compare_unit_hot_tag(&tags[0], hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("tag2"), 1, 1, 1);
    compare_unit_hot_tag(&tags[1], hot_tag);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_friends_reach_and_all_timeframe() -> Result<()> {
    let endpoint = &format!("/v0/tags/hot?user_id={USER_5}&reach=friends&timeframe=all_time");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let tags = body.as_array().expect("Stream tags should be an array");

    // Validate that the posts belong to the specified user's bookmarks
    analyse_hot_tags_structure(tags);

    assert_eq!(tags.len(), 6);

    // Analyse the tag that is in the 0 index
    let hot_tag = StreamTagMockup::new(String::from("all"), 1, 1, 1);
    compare_unit_hot_tag(&tags[0], hot_tag);

    Ok(())
}

const PUBKY_TAG: &str = "pubky";

// ##### Trust filter #####
// Reach queries hit the graph on every request, so they observe the filter
// directly. The global tests above hold with the filter on or off, because
// every hot-tags and skunk tagger is ranked; only the mod bot's labels tell
// the two apart on the global path.

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_hide_unranked_taggers() -> Result<()> {
    // The whole snapshot, page by page.
    let mut tags: Vec<Value> = Vec::new();
    for skip in (0..100).step_by(40) {
        let endpoint = &format!("/v0/tags/hot?timeframe=all_time&skip={skip}&limit=40");
        let body = get_request(endpoint).await?;
        tags.extend(
            body.as_array()
                .expect("Stream tags should be an array")
                .iter()
                .cloned(),
        );
    }
    analyse_hot_tags_structure(&tags);

    // The snapshot serves `MODBOT_HOT_LABEL`, so its unranked tagger can only
    // be missing because of the filter.
    let hot_tag = tags
        .iter()
        .find(|tag| tag["label"] == MODBOT_HOT_LABEL)
        .expect("the hot label should be served");
    let taggers = hot_tag["taggers_id"].as_array().expect("taggers_id array");
    assert!(!taggers.is_empty());
    assert!(
        !taggers.iter().any(|tagger| tagger == WOT_MODBOT),
        "unranked tagger served under {MODBOT_HOT_LABEL}"
    );
    // The taggers fit in the sample, so the count has nobody else to cover.
    assert_eq!(
        hot_tag["taggers_count"],
        taggers.len(),
        "unranked tagger counted under {MODBOT_HOT_LABEL}"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_label_taggers_hide_unranked_taggers() -> Result<()> {
    let body = get_request(&format!("/v0/tags/taggers/{MODBOT_HOT_LABEL}")).await?;
    let taggers = body.as_array().expect("Taggers ids should be an array");

    assert!(!taggers.is_empty(), "the ranked taggers should be served");
    assert!(
        !taggers.iter().any(|tagger| tagger == WOT_MODBOT),
        "unranked tagger served under {MODBOT_HOT_LABEL}"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_reach_hide_unranked_taggers() -> Result<()> {
    let endpoint =
        &format!("/v0/tags/hot?user_id={WOT_OBSERVER}&reach=following&timeframe=all_time");

    let body = get_request(endpoint).await?;
    let tags = body.as_array().expect("Stream tags should be an array");
    analyse_hot_tags_structure(tags);

    // D1: wmtag1, wmtag2; D1B: wmtag3, wmtag4; both: wotreview. Every label
    // tags one post, so they order by label. The mod bot's nudity, wotflag and
    // wmtagflag are gone.
    let labels: Vec<&str> = tags
        .iter()
        .filter_map(|tag| tag["label"].as_str())
        .collect();
    assert_eq!(
        labels,
        ["wmtag1", "wmtag2", "wmtag3", "wmtag4", "wotreview"],
        "unranked taggers' labels must be hidden"
    );
    compare_unit_hot_tag(
        &tags[4],
        StreamTagMockup::new(String::from("wotreview"), 2, 1, 2),
    );
    for tag in tags {
        for tagger in tag["taggers_id"].as_array().expect("taggers_id array") {
            let tagger = tagger.as_str().unwrap_or_default();
            assert!(
                [WOT_D1, WOT_D1B].contains(&tagger),
                "unranked tagger {tagger} served under {}",
                tag["label"]
            );
        }
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_by_reach_hide_unranked_taggers() -> Result<()> {
    // Tagged by D1 and D1B, latest first.
    let endpoint = &format!("/v0/tags/taggers/wotreview?user_id={WOT_OBSERVER}&reach=following");
    let body = get_request(endpoint).await?;
    assert_eq!(body, serde_json::json!([WOT_D1B, WOT_D1]));

    // Tagged by the mod bot only, so nobody is left.
    let endpoint = &format!("/v0/tags/taggers/wmtagflag?user_id={WOT_OBSERVER}&reach=following");
    let body = get_request(endpoint).await?;
    assert_eq!(body, serde_json::json!([]));

    Ok(())
}

const TAGGERS: [&str; 9] = [
    "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy",
    "s1empmp4x6owkewyijcbnn1faejhhu536w8i7n9oqh57om9qjfho",
    "emq37ky6fbnaun7q1ris6rx3mqmw3a33so1txfesg9jj3ak9ryoy",
    "7w4hmktqa7gia5thmk7zki8px7ttwpwjtgaaaou4tbqx64re8d1o",
    "ze86rtgp6x1qdyno4uzp8gexbb887dtemmonoh4j3iisbzitcppo",
    "end1obs8cy3ssqzhm73hiojwpakb4ac1fiubbmk5zfuruaaumwso",
    "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro",
    "omynbjw4ksjc4at5gretyoatw1g5h53tkee5z55fh69sng1d3jpy",
    "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy",
];

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers() -> Result<()> {
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 9);

    for (index, tagger) in TAGGERS.into_iter().enumerate() {
        assert_eq!(TAGGERS[index], tagger);
    }

    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}?skip=4&limit=2");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers_with_filters = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers_with_filters.len(), 2);

    let skip_and_limit_taggers: Vec<String> = taggers
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .skip(4)
        .take(2)
        .collect();

    for (index, tagger) in taggers_with_filters.iter().enumerate() {
        assert_eq!(&skip_and_limit_taggers[index], tagger);
    }
    Ok(())
}

/// Redis keys of one global all_time cache variant: the score set and the
/// taggers map.
fn global_all_time_cache_keys(ranked_only: bool) -> [String; 2] {
    let timeframe = Timeframe::AllTime.to_string();
    [
        hot_tags_key_parts(ranked_only, &[&timeframe]),
        hot_tags_key_parts(ranked_only, &[TAGGERS_INDEX, &timeframe]),
    ]
    .map(|key_parts| format!("{HOT_TAGS_CACHE_PREFIX}:{}", key_parts.join(":")))
}

/// What the global readers serve from a cold cache variant.
struct ColdVariantProbe {
    taggers: Value,
    /// Whether the taggers request left the score set and the taggers map written.
    keys_written: [bool; 2],
    /// Taggers of the label only the unranked variant serves the mod bot under.
    modbot_label_taggers: Value,
    hot_tags: Value,
}

async fn probe_cold_variant(ranked_only: bool) -> Result<ColdVariantProbe> {
    let mut redis_conn = get_redis_conn().await?;
    let keys = global_all_time_cache_keys(ranked_only);

    let _: () = redis_conn.del(&keys).await?;
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}?timeframe=all_time");
    let taggers = get_request(endpoint).await?;
    let mut keys_written = [false; 2];
    for (written, key) in keys_written.iter_mut().zip(&keys) {
        *written = redis_conn.exists(key).await?;
    }
    let endpoint = &format!("/v0/tags/taggers/{MODBOT_HOT_LABEL}?timeframe=all_time");
    let modbot_label_taggers = get_request(endpoint).await?;

    let _: () = redis_conn.del(&keys).await?;
    let hot_tags = get_request("/v0/tags/hot?timeframe=all_time").await?;

    Ok(ColdVariantProbe {
        taggers,
        keys_written,
        modbot_label_taggers,
        hot_tags,
    })
}

fn assert_cold_variant_was_filled(probe: ColdVariantProbe, ranked_only: bool) {
    let variant = if ranked_only { "ranked" } else { "unranked" };
    let mut taggers: Vec<&str> = probe
        .taggers
        .as_array()
        .expect("Taggers ids should be an array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    taggers.sort_unstable();
    let mut expected = TAGGERS;
    expected.sort_unstable();
    assert_eq!(
        taggers, expected,
        "the taggers route must fill the cold {variant} variant"
    );
    assert_eq!(
        probe.keys_written,
        [true, true],
        "the taggers route must write the {variant} score set and taggers map"
    );
    // The mod bot is unranked, so it tells the content of the two variants apart.
    let modbot_label_taggers = probe
        .modbot_label_taggers
        .as_array()
        .expect("Taggers ids should be an array");
    assert!(!modbot_label_taggers.is_empty());
    assert_eq!(
        modbot_label_taggers
            .iter()
            .any(|tagger| tagger == WOT_MODBOT),
        !ranked_only,
        "the taggers route must fill the cold {variant} variant with {variant} taggers"
    );

    let hot_tags = probe
        .hot_tags
        .as_array()
        .expect("Stream tags should be an array");
    assert!(
        hot_tags.iter().any(|tag| tag["label"] == PUBKY_TAG),
        "the hot tags route must fill the cold {variant} variant"
    );
    let modbot_hot_tag = hot_tags
        .iter()
        .find(|tag| tag["label"] == MODBOT_HOT_LABEL)
        .expect("the hot label should be served");
    let taggers = modbot_hot_tag["taggers_id"]
        .as_array()
        .expect("taggers_id array");
    assert_eq!(
        taggers.iter().any(|tagger| tagger == WOT_MODBOT),
        !ranked_only,
        "the hot tags route must fill the cold {variant} variant with {variant} taggers"
    );
}

/// A ranking appearing or being dropped selects a cache variant nothing has
/// warmed. Every global reader has to fill it on the miss.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_fill_the_variant_a_ranking_flip_selects() -> Result<()> {
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let ranking_key = format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"));
    let _: () = redis_conn.del(&ranking_key).await?;

    // Probed without `?` so an error between the delete and the rebuild cannot
    // strand the ranking for every other test.
    let unranked = probe_cold_variant(false).await;
    SocialGraphStatus::reindex().await?;
    assert_cold_variant_was_filled(unranked?, false);

    assert_cold_variant_was_filled(probe_cold_variant(true).await?, true);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_skip_limit_and_timeframe() -> Result<()> {
    let endpoint = &format!(
        "/v0/tags/taggers/{}?skip=1&limit=2&timeframe=this_month",
        "tag1"
    );

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 2);

    assert_eq!(
        &taggers[0],
        "qumq6fady4bmw4w5tpsrj1tg36g3qo4tcfedga9p4bg4so4ikyzy"
    );
    assert_eq!(
        &taggers[1],
        "r4irb481b8qspaixq1brwre8o87cxybsbk9iwe1f6f9ukrxxs7bo"
    );
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_following() -> Result<()> {
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}?reach=following&user_id={PEER_PUBKY}");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 4);

    assert_eq!(
        &taggers[0],
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    assert_eq!(
        &taggers[2],
        "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_following_skip_and_limit() -> Result<()> {
    let endpoint = &format!(
        "/v0/tags/taggers/{PUBKY_TAG}?reach=following&user_id={PEER_PUBKY}&skip=1&limit=2"
    );

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 2);

    assert_eq!(
        &taggers[0],
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
    );

    assert_eq!(
        &taggers[1],
        "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_followers() -> Result<()> {
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}?reach=followers&user_id={PEER_PUBKY}");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 2);

    assert_eq!(
        &taggers[0],
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
    );

    assert_eq!(
        &taggers[1],
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_followers_skip_and_limit() -> Result<()> {
    let endpoint = &format!(
        "/v0/tags/taggers/{PUBKY_TAG}?reach=followers&user_id={PEER_PUBKY}&skip=1&limit=1"
    );

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 1);

    assert_eq!(
        &taggers[0],
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_friends() -> Result<()> {
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}?reach=friends&user_id={PEER_PUBKY}");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 2);

    assert_eq!(
        &taggers[0],
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
    );

    assert_eq!(
        &taggers[1],
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers_with_reach_friends_skip_and_limit() -> Result<()> {
    let endpoint =
        &format!("/v0/tags/taggers/{PUBKY_TAG}?reach=friends&user_id={PEER_PUBKY}&skip=1&limit=1");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 1);

    assert_eq!(
        &taggers[0],
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    Ok(())
}
