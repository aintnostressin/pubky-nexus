//! `source=all` trust filter: posts by authors absent from the trust ranking
//! are hidden (`[api] hide_unranked_authors`), and the stream is served
//! unfiltered when no ranking exists.
//!
//! Fixture (docker/test-graph/mocks/wot.cypher): D1 0.4, D2 0.2, D1B 0.1 and
//! nobody else carries trust, so the ranking is exactly three deep. Every wot
//! fixture post sits in `indexed_at` 1650000000001..=1650000000014, a window
//! no other fixture uses, so `start`/`end` pin the requests to it.
//!
//! The test server starts with the switch off (utils/server.rs): this file
//! turns it on for its own duration. Isolation: it also drops and rebuilds
//! the ranking, so `.config/nextest.toml` runs it alone. Under plain
//! `cargo test` the switch is process-global and would filter every
//! concurrent `source=all` request: run this file with `--test-threads=1`.
use crate::utils::{get_request, server::TestServiceServer};
use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use futures_util::FutureExt;
use nexus_common::db::get_redis_conn;
use nexus_common::models::post::set_hide_unranked_authors;
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};
use serde_json::Value;
use std::collections::HashSet;
use std::panic::{resume_unwind, AssertUnwindSafe};

use super::utils::ids_in;
use super::{KEYS_ROOT_PATH, ROOT_PATH};

const WOT_D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
const WOT_D2: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y";
const WOT_D1B: &str = "t5ixbtatg4tq5q5ixg16qqrg1bmem75ksg6cweuftuydwzw91pzy";
const RANKED: [&str; 3] = [WOT_D1, WOT_D2, WOT_D1B];

/// Root posts by the ranked users, oldest first.
const D1_POST: &str = "WOTPOSTD10002";
const D1B_POST: &str = "WOTPOSTD1B003";
const D2_POST: &str = "WOTPOSTD20004";
const D1_POST_SCORE: u64 = 1650000000002;
/// A root post by the spammer, who carries no trust.
const SPAMMER_POST: &str = "WOTPOSTS00006";

/// The wot fixture window, newest first.
const WINDOW: &str = "source=all&sorting=timeline&start=1650000000014&end=1650000000001";

async fn posts(query: &str) -> Result<Value> {
    Ok(get_request(&format!("{ROOT_PATH}?{WINDOW}&{query}")).await?)
}

async fn keys(query: &str) -> Result<Value> {
    Ok(get_request(&format!("{KEYS_ROOT_PATH}?{WINDOW}&{query}")).await?)
}

fn authors_in(response: &Value) -> Vec<String> {
    response
        .as_array()
        .expect("Post stream should be an array")
        .iter()
        .map(|p| {
            p["details"]["author"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
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
    for author in authors_in(response) {
        assert!(
            RANKED.contains(&author.as_str()),
            "{path}: post by unranked author {author} was served: {ids:?}"
        );
    }
    for id in [D1_POST, D1B_POST, D2_POST] {
        assert!(
            ids.contains(&id.to_string()),
            "{path}: root post {id} by a ranked author must be served: {ids:?}"
        );
    }
    assert!(
        !ids.contains(&SPAMMER_POST.to_string()),
        "{path}: the spammer's post must be hidden: {ids:?}"
    );
}

struct Probe {
    redis_path: Value,
    cypher_path: Value,
    key_path: Value,
    first_page: Value,
    second_page: Value,
    first_keys: Value,
    second_keys: Value,
    without_ranking: Value,
    without_ranking_cypher: Value,
}

/// One test on purpose: the switch and the ranking are both global. The probe
/// runs under `catch_unwind` so a panic (`get_request` asserts 200) cannot
/// unwind past the teardown, which resets the switch and rebuilds the ranking
/// on every exit path; the panic is then resumed and the assertions run after.
#[tokio_shared_rt::test(shared)]
async fn test_all_hides_posts_by_unranked_authors() -> Result<()> {
    TestServiceServer::get_test_server().await;

    let probe = AssertUnwindSafe(async move {
        set_hide_unranked_authors(true);

        // Redis path (global timeline set) and Cypher fallback (`kind=`).
        let redis_path = posts("limit=50").await?;
        let cypher_path = posts("kind=short&limit=50").await?;
        let key_path = keys("limit=50").await?;

        // A page is filled from the underlying stream and the cursor is the
        // last served score, so score paging (`start` + `skip=1`) is exact.
        let first_page = posts("limit=2").await?;
        let first_keys = keys("limit=2").await?;
        let cursor = first_keys["last_post_score"]
            .as_u64()
            .expect("a non-empty keys page carries a cursor");
        let second_page = posts(&format!("limit=2&skip=1&start={cursor}")).await?;
        let second_keys = keys(&format!("limit=2&skip=1&start={cursor}")).await?;

        // No ranking at all: nothing is hidden, on either path.
        let mut redis_conn = get_redis_conn().await?;
        let ranking_key = format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"));
        let _: () = redis_conn.del(&ranking_key).await?;
        let without_ranking = posts("limit=50").await?;
        let without_ranking_cypher = posts("kind=short&limit=50").await?;

        anyhow::Ok(Probe {
            redis_path,
            cypher_path,
            key_path,
            first_page,
            second_page,
            first_keys,
            second_keys,
            without_ranking,
            without_ranking_cypher,
        })
    })
    .catch_unwind()
    .await;

    set_hide_unranked_authors(false);
    SocialGraphStatus::reindex().await?;
    let probe = probe.unwrap_or_else(|panic| resume_unwind(panic))?;

    assert_only_ranked_authors("redis", &probe.redis_path);
    assert_only_ranked_authors("cypher", &probe.cypher_path);

    // The keys route serves the same filtered page, its cursor on the last
    // served key: the oldest ranked post in the window.
    let key_authors: Vec<String> = post_keys_in(&probe.key_path)
        .iter()
        .map(|key| key.split(':').next().unwrap_or_default().to_string())
        .collect();
    assert!(
        !key_authors.is_empty() && key_authors.iter().all(|a| RANKED.contains(&a.as_str())),
        "keys: only ranked authors are served: {key_authors:?}"
    );
    assert_eq!(
        probe.key_path["last_post_score"],
        Value::from(D1_POST_SCORE),
        "keys: the cursor is the last served post's score"
    );

    // Page fill: a `limit=2` page is full although the newest raw entries in
    // the window are all unranked, and the next page resumes past it.
    let first = ids_in(&probe.first_page);
    let second = ids_in(&probe.second_page);
    assert_eq!(first.len(), 2, "the page is filled past hidden entries");
    for (name, page) in [("first", &probe.first_page), ("second", &probe.second_page)] {
        for author in authors_in(page) {
            assert!(
                RANKED.contains(&author.as_str()),
                "{name} page served unranked author {author}"
            );
        }
    }
    assert!(
        !second.is_empty(),
        "score paging continues past the first page"
    );
    let overlap: Vec<&String> = first.iter().filter(|id| second.contains(id)).collect();
    assert!(
        overlap.is_empty(),
        "score paging must not repeat posts: {overlap:?}"
    );
    assert_eq!(
        post_keys_in(&probe.first_keys).len(),
        2,
        "the keys page is filled past hidden entries"
    );
    let first_keys: HashSet<String> = post_keys_in(&probe.first_keys).into_iter().collect();
    let second_keys: HashSet<String> = post_keys_in(&probe.second_keys).into_iter().collect();
    assert!(
        first_keys.is_disjoint(&second_keys),
        "score paging on the keys route must not repeat keys"
    );

    // Without a ranking the filter has nothing to say and steps aside.
    for (path, response) in [
        ("redis", &probe.without_ranking),
        ("cypher", &probe.without_ranking_cypher),
    ] {
        let ids = ids_in(response);
        assert!(
            ids.contains(&SPAMMER_POST.to_string()),
            "{path}: without a ranking the spammer's post is served: {ids:?}"
        );
        assert!(
            ids.contains(&D1_POST.to_string()),
            "{path}: without a ranking ranked posts are still served: {ids:?}"
        );
    }

    // Switch off (the test server default): everything shows again.
    let unfiltered = ids_in(&posts("limit=50").await?);
    assert!(
        unfiltered.contains(&SPAMMER_POST.to_string()),
        "with the switch off the spammer's post is served: {unfiltered:?}"
    );

    Ok(())
}
