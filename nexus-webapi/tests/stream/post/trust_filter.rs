//! `source=all` trust filter: posts by authors absent from the trust ranking
//! are hidden, and the stream is served unfiltered when no ranking exists.
//!
//! Fixture (docker/test-graph/mocks/wot.cypher): D1 0.4, D2 0.2, D1B 0.1 and
//! nobody else carries trust, so the ranking is exactly three deep. Every wot
//! fixture post sits in `indexed_at` 1650000000001..=1650000000014, a window
//! no other fixture uses, so `start`/`end` pin the requests to it.
//!
//! The test server turns the filter off (utils/server.rs) because the fixture
//! ranks only three users: this file turns it on for its own duration, after
//! `get_test_server()`, since a server start switches it off again. It also drops
//! and rebuilds the ranking, so `.config/nextest.toml` runs it alone. Under
//! plain `cargo test` the switch is process-global and would filter every
//! concurrent `source=all` request: run this file with `--test-threads=1`.
use crate::utils::{get_request, server::TestServiceServer};
use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use futures_util::FutureExt;
use nexus_common::db::get_redis_conn;
use nexus_common::models::post::set_hide_unranked_authors;
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};
use serde_json::Value;
use std::panic::{resume_unwind, AssertUnwindSafe};

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
/// A root post by the spammer, who carries no trust.
const SPAMMER_POST: &str = "WOTPOSTS00006";

/// The wot fixture window, newest first: `start` is the upper bound.
const WINDOW: &str = "source=all&sorting=timeline&end=1650000000001";
const WINDOW_START: u64 = 1650000000014;
/// The oldest post in the window, by the unranked observer.
const OLDEST_SCORE: u64 = 1650000000001;

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

/// One test on purpose: the switch and the ranking are both global, so the
/// teardown below must run on every exit path, failed assertions included.
#[tokio_shared_rt::test(shared)]
async fn test_all_hides_posts_by_unranked_authors() -> Result<()> {
    TestServiceServer::get_test_server().await;

    let outcome = AssertUnwindSafe(async {
        set_hide_unranked_authors(true);

        // Redis path, Cypher fallback (`kind=`), keys route.
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
        // The cursor is the last entry examined, hidden or not.
        assert_eq!(page["last_post_score"], Value::from(OLDEST_SCORE));

        // Page fill and score paging: a full page past hidden entries, a
        // disjoint next page, then the end of the stream (no cursor).
        let first = keys(WINDOW_START, "limit=2").await?;
        let first_keys = post_keys_in(&first);
        assert_eq!(
            first_keys.len(),
            2,
            "the page is filled past hidden entries"
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

        // No ranking: nothing is hidden, on either path.
        let ranking_key = format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"));
        let _: () = get_redis_conn().await?.del(&ranking_key).await?;
        for (path, response) in [
            ("redis", posts(WINDOW_START, "limit=50").await?),
            ("cypher", posts(WINDOW_START, "kind=short&limit=50").await?),
        ] {
            let ids = ids_in(&response);
            assert!(
                ids.contains(&SPAMMER_POST.to_string()),
                "{path}: without a ranking the spammer's post is served: {ids:?}"
            );
        }
        anyhow::Ok(())
    })
    .catch_unwind()
    .await;

    set_hide_unranked_authors(false);
    SocialGraphStatus::reindex().await?;
    outcome.unwrap_or_else(|panic| resume_unwind(panic))?;

    // Switch off (the test server default): everything shows again.
    let ids = ids_in(&posts(WINDOW_START, "limit=50").await?);
    assert!(
        ids.contains(&SPAMMER_POST.to_string()),
        "switch off: {ids:?}"
    );
    Ok(())
}
