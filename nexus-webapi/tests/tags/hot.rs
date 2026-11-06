use std::collections::HashMap;

use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::fetch_key_from_graph;
use nexus_common::db::graph::Query;
use serde_json::{json, Value};

use crate::utils::{get_request, invalid_get_request};

const PEER_PUBKY: &str = "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";
// mocks/hot-tags.cypher users
pub const USER_1: &str = "pyc598poqkdgtx1wc4aeptx67mqg71mmywyh7uzkffzittjmbiuo";
const USER_4: &str = "r91hi8kc3x6761gwfiigr7yn6nca1z47wm6jadhw1jbx1co93r9y";
const USER_5: &str = "tkpeqpx3ywoawiw6q8e6kuo9o3egr7fnhx83rudznbrrmqgdmomo";
// mocks/hot-tags.cypher's trust-ranked block, tagged today: Peer follows Author,
// Second and Newcomer, who has no trust and the lowest id
const HOT_PEER: &str = "zp9hooj5djnji9py6td7qj13fju4ta9j1i67h618ps7bkjf6xr5o";
const HOT_SECOND: &str = "zwz9ek4dx65b8sw7xj78kucokissw1xh9m48jq7jzpwuf817ifky";
const HOT_NEWCOMER: &str = "z1yrrut7hwr3cnpsiu768qcj1yojuz54g4ecqz5ejjjzen1ppb4y";

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
    let hot_tag = StreamTagMockup::new(String::from("opensource"), 5, 8, 5);
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
    let hot_tag = StreamTagMockup::new(String::from("month"), 4, 3, 4);
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
    let hot_tag = StreamTagMockup::new(String::from("sentimental"), 8, 11, 8);
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
    let hot_tag = StreamTagMockup::new(String::from("tag1"), 3, 1, 3);
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
    let hot_tag = StreamTagMockup::new(String::from("tag1"), 3, 1, 3);
    compare_unit_hot_tag(&tags[0], hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("tag3"), 3, 1, 3);
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
    let hot_tag = StreamTagMockup::new(String::from("🤘"), 3, 3, 3);
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
    let hot_tag = StreamTagMockup::new(String::from("test"), 3, 3, 3);
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
    let hot_tag = StreamTagMockup::new(String::from("👀"), 2, 2, 2);
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
    let hot_tag = StreamTagMockup::new(String::from("test"), 3, 3, 3);
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

// The most trusted first: they all have the same trust, so by id
const TAGGERS: [&str; 9] = [
    "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro",
    "7w4hmktqa7gia5thmk7zki8px7ttwpwjtgaaaou4tbqx64re8d1o",
    "emq37ky6fbnaun7q1ris6rx3mqmw3a33so1txfesg9jj3ak9ryoy",
    "end1obs8cy3ssqzhm73hiojwpakb4ac1fiubbmk5zfuruaaumwso",
    "omynbjw4ksjc4at5gretyoatw1g5h53tkee5z55fh69sng1d3jpy",
    "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy",
    "s1empmp4x6owkewyijcbnn1faejhhu536w8i7n9oqh57om9qjfho",
    "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy",
    "ze86rtgp6x1qdyno4uzp8gexbb887dtemmonoh4j3iisbzitcppo",
];

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_label_taggers() -> Result<()> {
    let endpoint = &format!("/v0/tags/taggers/{PUBKY_TAG}");

    let body = get_request(endpoint).await?;
    assert!(body.is_array());

    let taggers = body.as_array().expect("Taggers ids should be an array");
    assert_eq!(taggers.len(), 9);

    for (index, tagger) in TAGGERS.into_iter().enumerate() {
        assert_eq!(taggers[index], tagger);
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
        "omynbjw4ksjc4at5gretyoatw1g5h53tkee5z55fh69sng1d3jpy"
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
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    assert_eq!(
        &taggers[1],
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
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
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
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
        "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro"
    );

    assert_eq!(
        &taggers[1],
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
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
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy"
    );

    Ok(())
}

fn tagger_ids(tag: &Value) -> Vec<String> {
    tag["taggers_id"]
        .as_array()
        .expect("taggers_id should be an array")
        .iter()
        .map(|id| {
            id.as_str()
                .expect("tagger id should be a string")
                .to_owned()
        })
        .collect()
}

fn find_tag<'a>(tags: &'a [Value], label: &str) -> &'a Value {
    tags.iter()
        .find(|tag| tag["label"] == label)
        .unwrap_or_else(|| panic!("hot tag {label} missing"))
}

fn labels(tags: &[Value]) -> Vec<&str> {
    tags.iter()
        .map(|tag| tag["label"].as_str().expect("label should be a string"))
        .collect()
}

/// Each listed tagger's trust, from the graph; zero without one.
async fn trust_of(tags: &[Value]) -> Result<HashMap<String, f64>> {
    let ids: Vec<String> = tags.iter().flat_map(tagger_ids).collect();
    let query = Query::new(
        "test_tagger_trust",
        "UNWIND $ids AS id MATCH (u:User {id: id})
        RETURN COLLECT([u.id, coalesce(u.trust, 0.0)]) AS trust",
    )
    .param("ids", ids);
    let trust = fetch_key_from_graph::<Vec<(String, f64)>>(query, "trust").await?;
    Ok(trust.unwrap_or_default().into_iter().collect())
}

/// `taggers_id` lists the most trusted taggers first, ties by id.
async fn assert_taggers_by_trust(endpoint: &str, tags: &[Value]) -> Result<()> {
    let trust = trust_of(tags).await?;
    for tag in tags {
        let ids = tagger_ids(tag);
        let mut by_trust = ids.clone();
        by_trust.sort_by(|a, b| trust[b].total_cmp(&trust[a]).then_with(|| a.cmp(b)));
        assert_eq!(
            ids, by_trust,
            "{endpoint}: taggers of {} are not the most trusted first, ties by id",
            tag["label"]
        );
    }
    Ok(())
}

/// `taggers_id` is cut to `taggers_limit` after collecting, so without an order the
/// kept taggers depend on graph storage order and can change between snapshots. The
/// most trusted come first, ties by id, so a card's avatars are the people who put
/// the label there. In hotpair the zero-trust Newcomer has the lowest id, so trust
/// order differs from id order.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_list_taggers_by_trust() -> Result<()> {
    for endpoint in ["/v0/tags/hot", "/v0/tags/hot?timeframe=today"] {
        let body = get_request(endpoint).await?;
        let tags = body.as_array().expect("Stream tags should be an array");
        assert!(!tags.is_empty(), "{endpoint} returned no hot tags");

        assert_taggers_by_trust(endpoint, tags).await?;
    }

    let body = get_request("/v0/tags/hot?timeframe=today").await?;
    let hotpair = find_tag(body.as_array().unwrap(), "hotpair");
    assert_eq!(tagger_ids(hotpair), [HOT_PEER, HOT_SECOND, HOT_NEWCOMER]);
    // The global taggers route reads the same cached list
    let taggers = get_request("/v0/tags/taggers/hotpair?timeframe=today").await?;
    assert_eq!(taggers, json!([HOT_PEER, HOT_SECOND, HOT_NEWCOMER]));

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_reach_list_taggers_by_trust() -> Result<()> {
    let hot_peer_following =
        format!("/v0/tags/hot?user_id={HOT_PEER}&reach=following&timeframe=today");
    let mut endpoints: Vec<String> = ["following", "followers", "friends"]
        .iter()
        .map(|reach| format!("/v0/tags/hot?user_id={PEER_PUBKY}&reach={reach}"))
        .collect();
    endpoints.push(hot_peer_following.clone());
    for endpoint in &endpoints {
        let body = get_request(endpoint).await?;
        let tags = body.as_array().expect("Stream tags should be an array");
        assert!(!tags.is_empty(), "{endpoint} returned no hot tags");

        assert_taggers_by_trust(endpoint, tags).await?;
    }

    let body = get_request(&hot_peer_following).await?;
    let hotpair = find_tag(body.as_array().unwrap(), "hotpair");
    assert_eq!(tagger_ids(hotpair), [HOT_SECOND, HOT_NEWCOMER]);

    Ok(())
}

/// A truncated taggers list is the first `taggers_limit` taggers of the full list.
#[tokio_shared_rt::test(shared)]
async fn test_hot_tags_by_reach_taggers_limit_keeps_most_trusted() -> Result<()> {
    let full = get_request(&format!(
        "/v0/tags/hot?user_id={PEER_PUBKY}&reach=following"
    ))
    .await?;
    let cut = get_request(&format!(
        "/v0/tags/hot?user_id={PEER_PUBKY}&reach=following&taggers_limit=3"
    ))
    .await?;

    let full_tag = find_tag(full.as_array().unwrap(), "pubky");
    let full_ids = tagger_ids(full_tag);
    let cut_ids = tagger_ids(find_tag(cut.as_array().unwrap(), "pubky"));
    assert_eq!(full_ids.len(), 4);
    assert_eq!(cut_ids, full_ids[..3]);

    assert_taggers_by_trust("following", std::slice::from_ref(full_tag)).await
}

/// hotthree and hotzero both have four posts from one tagger, but only Author
/// has trust: 0.3 × (1 + 1/2 + 1/3 + 1/4) against nothing, so hotzero comes last.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_weighs_taggers_by_trust() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=today").await?;
    let tags = body.as_array().expect("Stream tags should be an array");

    let hot_tag = StreamTagMockup::new(String::from("hotthree"), 1, 4, 1);
    compare_unit_hot_tag(find_tag(tags, "hotthree"), hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("hotzero"), 1, 4, 1);
    compare_unit_hot_tag(tags.last().expect("hot tags"), hot_tag);

    Ok(())
}

/// Repeat tags add less and less: Peer's two hotpair posts and Second's one,
/// 0.3 × (1 + 1/2) + 0.3 = 0.75, outweigh Author's four hotthree posts,
/// 0.3 × (1 + 1/2 + 1/3 + 1/4) = 0.625, which still outweigh tag2's two
/// taggers on one post, 0.6. Without the bonus hotthree would score 0.3, after
/// tag2; with a linear one 1.2, before hotpair; by post count it would lead.
/// Newcomer's hotpair tag adds nothing.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_gives_repeat_tags_a_diminishing_bonus() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=today").await?;
    let tags = body.as_array().expect("Stream tags should be an array");

    let order = labels(tags);
    let position = |label| order.iter().position(|listed| *listed == label);
    assert!(
        position("hotpair") < position("hotthree") && position("hotthree") < position("tag2"),
        "hotthree should rank between hotpair and tag2: {order:?}"
    );
    let hot_tag = StreamTagMockup::new(String::from("hotpair"), 3, 3, 3);
    compare_unit_hot_tag(find_tag(tags, "hotpair"), hot_tag);
    let hot_tag = StreamTagMockup::new(String::from("hotthree"), 1, 4, 1);
    compare_unit_hot_tag(find_tag(tags, "hotthree"), hot_tag);

    Ok(())
}

/// The whole order of the global Today list: by score, a label on one post
/// included. Peer's following list, without Peer's own hotpair tags, ranks
/// hotpair at 0.3, after hotone by label.
#[tokio_shared_rt::test(shared)]
async fn test_global_hot_tags_order_by_score() -> Result<()> {
    let body = get_request("/v0/tags/hot?timeframe=today").await?;
    let tags = body.as_array().expect("Stream tags should be an array");
    assert_eq!(
        labels(tags),
        ["today", "tag1", "hotpair", "hotthree", "tag2", "hotone", "hotzero"]
    );
    let hot_tag = StreamTagMockup::new(String::from("hotone"), 1, 1, 1);
    compare_unit_hot_tag(find_tag(tags, "hotone"), hot_tag);

    let endpoint = format!("/v0/tags/hot?user_id={HOT_PEER}&reach=following&timeframe=today");
    let body = get_request(&endpoint).await?;
    let tags = body.as_array().expect("Stream tags should be an array");
    assert_eq!(labels(tags), ["hotthree", "hotone", "hotpair", "hotzero"]);
    let hot_tag = StreamTagMockup::new(String::from("hotone"), 1, 1, 1);
    compare_unit_hot_tag(find_tag(tags, "hotone"), hot_tag);

    Ok(())
}
