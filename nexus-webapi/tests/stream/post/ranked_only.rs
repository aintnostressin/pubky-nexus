//! Shared-surface filter: `source=all` hides root posts by authors absent
//! from the trust ranking (`PostStream::filter_shared_surface`); fail-open
//! when no ranking exists.
//!
//! Fixture (docker/test-graph/mocks/wot.cypher): D1 0.4, D2 0.2, D1B 0.1 and
//! nobody else carries trust, so the ranking is exactly three deep. The test
//! server starts with the filter off (utils/server.rs) or every other fixture
//! post would vanish from `all`; the test switches it on for its own window
//! and back off afterwards.
//!
//! Isolation: this test seeds posts at the head of the shared global timeline
//! and drops the ranking, so `.config/nextest.toml` runs it alone. Under
//! plain `cargo test` the switch is process-global and would filter every
//! concurrent request to the default source: run this file with
//! `--test-threads=1`.
use crate::utils::{get_request, server::TestServiceServer};
use anyhow::Result;
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use futures_util::FutureExt;
use nexus_common::db::{execute_graph_operation, get_redis_conn, queries};
use nexus_common::models::post::{set_hide_unranked_authors, PostDetails, PostRelationships};
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};
use pubky_app_specs::{post_uri_builder, PubkyAppPostKind};
use serde_json::Value;
use std::panic::{resume_unwind, AssertUnwindSafe};

use super::utils::ids_in;
use super::{KEYS_ROOT_PATH, ROOT_PATH, USER_ID};

/// Ranked in the fixture (trust 0.4).
const WOT_D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
/// Aldert carries no trust, so he is absent from the ranking.
const UNRANKED: &str = USER_ID;

// Root posts seeded newer than every fixture post, so they lead the page.
const RANKED_POST: &str = "0RANKD00000P1";
const UNRANKED_POST: &str = "0RANKD00000P2";

fn post_key(author: &str, post_id: &str) -> String {
    format!("{author}:{post_id}")
}

fn contains(ids: &[String], id: &str) -> bool {
    ids.iter().any(|seen| seen == id)
}

/// Writes a root post to both stores the way the watcher does: graph first,
/// then the Redis indexes (details JSON, timeline set).
async fn seed_post(author: &str, post_id: &str, indexed_at: i64) -> Result<()> {
    let details = PostDetails {
        content: format!("ranked-only fixture {post_id}"),
        id: post_id.to_string(),
        indexed_at,
        author: author.to_string(),
        kind: PubkyAppPostKind::Short,
        uri: post_uri_builder(author.to_string(), post_id.to_string()),
        attachments: None,
        lock: None,
    };
    let relationships = PostRelationships::default();
    details.put_to_graph(&relationships).await?;
    relationships.put_to_index(author, post_id).await?;
    details.put_to_index(author, None, false).await?;
    Ok(())
}

/// Idempotent, so teardown can run whether or not seeding got this far.
async fn remove_post(author: &str, post_id: &str) -> Result<()> {
    PostDetails::delete_from_index(author, post_id, None).await?;
    PostRelationships::delete(author, post_id).await?;
    execute_graph_operation(queries::del::delete_post(author, post_id)).await?;
    Ok(())
}

async fn all_ids(query: &str) -> Result<Vec<String>> {
    Ok(ids_in(&get_request(&format!("{ROOT_PATH}?{query}")).await?))
}

async fn all_keys(query: &str) -> Result<Value> {
    Ok(get_request(&format!("{KEYS_ROOT_PATH}?{query}")).await?)
}

/// Both paths, the keys cursor and the no-ranking half live in one test on
/// purpose: the ranking and the filter switch are global. Seeding and probing
/// run under `catch_unwind` so a panic (`get_request` asserts 200) is caught
/// rather than unwinding past the teardown: the switch is reset, the ranking
/// rebuilt and the seeded posts removed on every exit path; the panic is then
/// resumed and the assertions run afterwards. (`tokio::spawn` would do the
/// same, but the test client's future is not `Send`.)
#[tokio_shared_rt::test(shared)]
async fn test_all_hides_root_posts_by_unranked_authors() -> Result<()> {
    TestServiceServer::get_test_server().await;
    let now = Utc::now().timestamp_millis();

    let probe = AssertUnwindSafe(async move {
        // The unranked post is the newest, so `limit=1` is a fully hidden window.
        seed_post(WOT_D1, RANKED_POST, now).await?;
        seed_post(UNRANKED, UNRANKED_POST, now + 1).await?;

        // The cursors a keys client sees with the filter off; they must not move.
        let unfiltered_keys = all_keys("source=all&limit=50").await?;
        let unfiltered_head = all_keys("source=all&limit=1").await?;

        set_hide_unranked_authors(true);
        let redis_path = all_ids("source=all&limit=50").await?;
        // `kind=` routes to the Cypher fallback.
        let cypher_path = all_ids("source=all&kind=short&limit=50").await?;
        let filtered_keys = all_keys("source=all&limit=50").await?;
        let hidden_head = all_keys("source=all&limit=1").await?;

        let mut redis_conn = get_redis_conn().await?;
        let ranking_key = format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"));
        let _: () = redis_conn.del(&ranking_key).await?;
        let without_ranking = all_ids("source=all&limit=50").await?;

        anyhow::Ok((
            unfiltered_keys,
            unfiltered_head,
            redis_path,
            cypher_path,
            filtered_keys,
            hidden_head,
            without_ranking,
        ))
    })
    .catch_unwind()
    .await;

    set_hide_unranked_authors(false);
    SocialGraphStatus::reindex().await?;
    remove_post(UNRANKED, UNRANKED_POST).await?;
    remove_post(WOT_D1, RANKED_POST).await?;
    let (
        unfiltered_keys,
        unfiltered_head,
        redis_path,
        cypher_path,
        filtered_keys,
        hidden_head,
        without_ranking,
    ) = probe.unwrap_or_else(|panic| resume_unwind(panic))?;

    for (path, ids) in [("redis", &redis_path), ("cypher", &cypher_path)] {
        assert!(
            !contains(ids, UNRANKED_POST),
            "{path}: a root post by an unranked author must be hidden"
        );
        assert!(
            contains(ids, RANKED_POST),
            "{path}: a root post by a ranked author is served"
        );
    }

    // The keys route filters the keys but keeps the cursor of the last
    // fetched key, so a keys client always steps past a hidden window.
    assert_eq!(
        filtered_keys["last_post_score"], unfiltered_keys["last_post_score"],
        "last_post_score must be the last fetched score, filtered or not"
    );
    let keys: Vec<&str> = filtered_keys["post_keys"]
        .as_array()
        .expect("post_keys array")
        .iter()
        .filter_map(|key| key.as_str())
        .collect();
    assert!(!keys.contains(&post_key(UNRANKED, UNRANKED_POST).as_str()));
    assert!(keys.contains(&post_key(WOT_D1, RANKED_POST).as_str()));

    // A fully hidden window: no keys, cursor intact (not the null of an empty
    // raw window), so the client advances instead of reading end-of-stream.
    assert_eq!(hidden_head["post_keys"], serde_json::json!([]));
    assert_eq!(hidden_head["last_post_score"], Value::from(now + 1));
    assert_eq!(
        hidden_head["last_post_score"], unfiltered_head["last_post_score"],
        "a fully hidden window keeps the cursor of the last fetched key"
    );

    // No ranking at all: fail open, everything shows.
    for id in [RANKED_POST, UNRANKED_POST] {
        assert!(
            contains(&without_ranking, id),
            "without a ranking {id} must be served"
        );
    }

    Ok(())
}
