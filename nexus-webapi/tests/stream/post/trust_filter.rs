//! `source=all` trust filter: posts by authors absent from the trust ranking
//! are hidden. The no-ranking fallback is covered by the unit tests in
//! `nexus_common::models::post::stream` and `models::user::social_graph`.
//!
//! Fixture: every user carries trust (docker/test-graph/mocks/trust.cypher)
//! except the wot on-ramp accounts, so within the wot window only D1, D2 and
//! D1B are ranked. Every wot fixture post sits in `indexed_at`
//! 1650000000001..=1650000000014, a window no other fixture uses, so
//! `start`/`end` pin the requests to it.
use crate::utils::get_request;
use anyhow::Result;
use serde_json::Value;

use super::utils::ids_in;
use super::{KEYS_ROOT_PATH, ROOT_PATH};

const WOT_D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
const WOT_D2: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y";
const WOT_D1B: &str = "t5ixbtatg4tq5q5ixg16qqrg1bmem75ksg6cweuftuydwzw91pzy";
const RANKED: [&str; 3] = [WOT_D1, WOT_D2, WOT_D1B];

/// Root posts by the ranked users.
const D1_POST: &str = "WOTPOSTD10002";
const D1B_POST: &str = "WOTPOSTD1B003";
const D2_POST: &str = "WOTPOSTD20004";

/// The wot fixture window, newest first: `start` is the upper bound.
const WINDOW: &str = "source=all&sorting=timeline&end=1650000000001";
const WINDOW_START: u64 = 1650000000014;
/// The oldest ranked post in the window.
const D1_POST_SCORE: u64 = 1650000000002;

async fn posts(start: u64, query: &str) -> Result<Value> {
    Ok(get_request(&format!("{ROOT_PATH}?{WINDOW}&start={start}&{query}")).await?)
}

async fn keys(start: u64, query: &str) -> Result<Value> {
    Ok(get_request(&format!("{KEYS_ROOT_PATH}?{WINDOW}&start={start}&{query}")).await?)
}

fn author_of(post_key: &str) -> &str {
    post_key.split(':').next().unwrap_or(post_key)
}

fn post_keys_in(response: &Value) -> Vec<String> {
    response["post_keys"]
        .as_array()
        .expect("post_keys array")
        .iter()
        .map(|k| k.as_str().unwrap_or_default().to_string())
        .collect()
}

fn assert_only_ranked_authors(path: &str, response: &Value) {
    let ids = ids_in(response);
    for post in response.as_array().expect("Post stream should be an array") {
        let author = post["details"]["author"].as_str().unwrap_or_default();
        assert!(
            RANKED.contains(&author),
            "{path}: post by unranked author {author} was served: {ids:?}"
        );
    }
    for id in [D1_POST, D1B_POST, D2_POST] {
        assert!(
            ids.contains(&id.to_string()),
            "{path}: root post {id} by a ranked author must be served: {ids:?}"
        );
    }
}

/// Redis path, Cypher fallback (`kind=`), and the keys route hide the same authors.
#[tokio_shared_rt::test(shared)]
async fn test_all_hides_posts_by_unranked_authors() -> Result<()> {
    assert_only_ranked_authors("redis", &posts(WINDOW_START, "limit=50").await?);
    assert_only_ranked_authors("cypher", &posts(WINDOW_START, "kind=short&limit=50").await?);

    let page = keys(WINDOW_START, "limit=50").await?;
    let unranked: Vec<String> = post_keys_in(&page)
        .into_iter()
        .filter(|key| !RANKED.contains(&author_of(key)))
        .collect();
    assert!(
        unranked.is_empty(),
        "keys: unranked authors served: {unranked:?}"
    );
    assert_eq!(page["last_post_score"], Value::from(D1_POST_SCORE));

    Ok(())
}

/// A full page of ranked posts, a disjoint next page by score cursor, then
/// the end of the stream (no cursor).
#[tokio_shared_rt::test(shared)]
async fn test_all_pages_by_score() -> Result<()> {
    let first = keys(WINDOW_START, "limit=2").await?;
    let first_keys = post_keys_in(&first);
    assert_eq!(
        first_keys.len(),
        2,
        "a page is full while ranked posts remain"
    );

    let cursor = first["last_post_score"].as_u64().expect("cursor");
    let second = keys(cursor, "limit=2&skip=1").await?;
    let second_keys = post_keys_in(&second);
    assert!(
        !second_keys.is_empty(),
        "score paging continues past the first page"
    );
    assert!(
        first_keys.iter().all(|key| !second_keys.contains(key)),
        "score paging must not repeat posts: {first_keys:?} then {second_keys:?}"
    );

    let cursor = second["last_post_score"].as_u64().expect("cursor");
    let end = keys(cursor, "limit=2&skip=1").await?;
    assert!(
        end["last_post_score"].is_null(),
        "end of stream has no cursor: {end}"
    );

    Ok(())
}

/// artist1: a wot on-ramp account with a root post, left unranked by the fixture.
const ARTIST1: &str = "w153s1dr9rw6t8s3nd1de6pqquuprb37dwrnwh3nk85jt9ys9k7o";
const ARTIST1_POST: &str = "WOTPOSTART1A";

/// The ranked sets rebuilt by the reindex: the engagement stream, walked
/// whole by offset (scores tie, so a score cursor cannot page it exactly),
/// serves only ranked authors, and the unranked artist1 post sits in both
/// global sets but in neither ranked one.
#[tokio_shared_rt::test(shared)]
async fn test_all_ranked_sets_hide_unranked_authors() -> Result<()> {
    use nexus_common::db::RedisOps;
    use nexus_common::models::post::{
        PostStream, POST_RANKED_TIMELINE_KEY_PARTS, POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS,
        POST_TIMELINE_KEY_PARTS, POST_TOTAL_ENGAGEMENT_KEY_PARTS,
    };
    use nexus_common::models::user::SocialGraphStatus;

    const LIMIT: usize = 30;
    let mut authors: Vec<String> = Vec::new();
    let mut keys: Vec<String> = Vec::new();
    for page in 0..50 {
        let skip = page * LIMIT;
        let response = get_request(&format!(
            "{KEYS_ROOT_PATH}?source=all&sorting=total_engagement&limit={LIMIT}&skip={skip}"
        ))
        .await?;
        let page_keys = post_keys_in(&response);
        let short = page_keys.len() < LIMIT;
        authors.extend(page_keys.iter().map(|key| author_of(key).to_string()));
        keys.extend(page_keys);
        if short {
            break;
        }
    }
    assert!(
        keys.len() > LIMIT,
        "the fixture engagement stream spans several pages: {}",
        keys.len()
    );
    assert!(
        !keys
            .iter()
            .any(|key| key == &format!("{ARTIST1}:{ARTIST1_POST}")),
        "the unranked artist1 post was served"
    );

    // A user absent from an existing ranking classifies as `New`, and `None`
    // means no ranking at all, so both must fail here: only the ranked tiers
    // may appear.
    let statuses = SocialGraphStatus::get_by_ids(&authors).await?;
    let unranked: Vec<&String> = authors
        .iter()
        .zip(&statuses)
        .filter(|(_, status)| {
            !matches!(
                status,
                Some(SocialGraphStatus::Established | SocialGraphStatus::Networked)
            )
        })
        .map(|(author, _)| author)
        .collect();
    assert!(unranked.is_empty(), "unranked authors served: {unranked:?}");

    let member = [ARTIST1, ARTIST1_POST];
    for (global, ranked) in [
        (
            &POST_TOTAL_ENGAGEMENT_KEY_PARTS[..],
            &POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS[..],
        ),
        (
            &POST_TIMELINE_KEY_PARTS[..],
            &POST_RANKED_TIMELINE_KEY_PARTS[..],
        ),
    ] {
        assert!(
            PostStream::check_sorted_set_member(None, global, &member)
                .await?
                .is_some(),
            "artist1's root post is in the global set {global:?}"
        );
        assert!(
            PostStream::check_sorted_set_member(None, ranked, &member)
                .await?
                .is_none(),
            "artist1's root post is not in the ranked set {ranked:?}"
        );
    }

    Ok(())
}
