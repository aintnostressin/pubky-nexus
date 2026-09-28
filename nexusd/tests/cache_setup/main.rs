//! Tests of the Redis cache bootstrap and warm-up. Require the docker stack
//! (Redis with the query engine + Neo4j) to be up. The mock data is what
//! `clear_redis_recreates_post_content_index` restores the cache from; the
//! warm-up test does not read it.
//!
//! Both tests destroy shared Redis keys, so they live in their own binary.
//! `.config/nextest.toml` gives this binary `threads-required =
//! 'num-test-threads'` so it never overlaps with any other test in the run.
//!
//! `clear_redis_recreates_post_content_index` pins the schema bootstrap
//! (`setup_cache`). `clear_redis()` issues FLUSHDB, which destroys the
//! RediSearch index along with the keys, so `setup_cache` must run again on
//! every flush rather than once per process. Wrapping it in a `OnceCell` (the
//! way `setup_graph` is) would pass every other test, because they boot the
//! stack once and never flush, while silently breaking `nexusd db clear --yes`
//! and the reindex bench. It is the only nexusd test that flushes Redis, and
//! it rebuilds the cache with `reindex::sync()` before returning so a local
//! run leaves the mock data in place for whatever runs next.
//!
//! `warm_up_builds_the_ranking_before_the_hot_tags` pins the order inside
//! `reindex::warm_up()`: the hot tags pick their cache variant from the trust
//! ranking existing, so the ranking has to be rebuilt first. It needs a user
//! with `trust > 0` who tagged a post, and brings its own rather than relying
//! on the seeded scores: the `trust` binary of this package recomputes `trust`
//! over the whole `:User` label and leaves every fixture user at `0.0`. It
//! drops the ranking and both global all_time hot-tags variants, removes its
//! user, post and tag label again, and leaves the cache warmed from the graph
//! as it found it.

use anyhow::{Context, Result};
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, get_redis_conn, kv::clear_redis, reindex, RedisOps};
use nexus_common::models::post::PostDetails;
use nexus_common::models::tag::global::TAGGERS_INDEX;
use nexus_common::models::tag::search::{TagSearch, TAGS_LABEL};
use nexus_common::models::tag::stream::{hot_tags_key_parts, HOT_TAGS_CACHE_PREFIX};
use nexus_common::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;
use nexus_common::types::Timeframe;
use nexus_common::{StackConfig, StackManager};
use redis::{AsyncCommands, Value};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const POST_CONTENT_INDEX: &str = "postContentIdx";

/// Flattens an FT.INFO reply into its leaf strings so assertions don't depend
/// on the exact nesting RediSearch uses for the `attributes` section.
fn flatten(value: Value, out: &mut Vec<String>) {
    match value {
        Value::Array(items) | Value::Set(items) => items.into_iter().for_each(|v| flatten(v, out)),
        Value::Map(pairs) => pairs.into_iter().for_each(|(k, v)| {
            flatten(k, out);
            flatten(v, out);
        }),
        Value::BulkString(bytes) => out.push(String::from_utf8_lossy(&bytes).into_owned()),
        Value::SimpleString(s) | Value::VerbatimString { text: s, .. } => out.push(s),
        Value::Int(i) => out.push(i.to_string()),
        _ => {}
    }
}

/// Returns every leaf string of `FT.INFO postContentIdx`, or the error the
/// server replied with (e.g. "Unknown index name" when the index is absent).
async fn post_content_index_info() -> Result<Vec<String>> {
    let mut conn = get_redis_conn().await?;
    let raw: Value = redis::cmd("FT.INFO")
        .arg(POST_CONTENT_INDEX)
        .query_async(&mut conn)
        .await
        .with_context(|| format!("FT.INFO {POST_CONTENT_INDEX} failed"))?;
    let mut leaves = Vec::new();
    flatten(raw, &mut leaves);
    Ok(leaves)
}

async fn db_size() -> Result<i64> {
    let mut conn = get_redis_conn().await?;
    Ok(redis::cmd("DBSIZE").query_async(&mut conn).await?)
}

fn assert_post_content_schema(info: &[String], prefix: &str, stage: &str) {
    for field in ["$.content", "$.author", "$.kind"] {
        assert!(
            info.iter().any(|s| s == field),
            "{stage}: {POST_CONTENT_INDEX} should index {field}, got {info:?}"
        );
    }
    assert!(
        info.iter().any(|s| s == prefix),
        "{stage}: {POST_CONTENT_INDEX} should be scoped to the {prefix} prefix, got {info:?}"
    );
}

#[tokio_shared_rt::test(shared)]
async fn clear_redis_recreates_post_content_index() -> Result<()> {
    StackManager::setup(&StackConfig::default())
        .await
        .map_err(|e| anyhow::anyhow!("could not initialise the stack: {e:?}"))?;

    // Same derivation as setup_cache, so a rename of PostDetails moves both sides.
    let prefix = format!("{}:", PostDetails::prefix().await);

    // Connector init already applied the schema before registering the pool.
    let before = post_content_index_info().await?;
    assert_post_content_schema(&before, &prefix, "after stack setup");

    // FLUSHDB destroys the index together with the keys; clear_redis must bring
    // the schema back on its own, without a migration run.
    clear_redis().await?;

    let after = post_content_index_info()
        .await
        .context("post content index must survive clear_redis()")?;
    assert_post_content_schema(&after, &prefix, "after clear_redis");

    // Restore the mock cache from the graph so the flush is not observable
    // by whatever runs after this test.
    reindex::sync().await;
    assert!(
        db_size().await? > 0,
        "reindex::sync should have repopulated Redis from the graph"
    );

    Ok(())
}

/// Redis key of the trust ranking sorted set.
fn trust_ranking_key() -> String {
    format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"))
}

/// Redis keys of one global all_time hot-tags cache variant: the score set and
/// the taggers map.
fn global_all_time_hot_tags_keys(ranked_only: bool) -> [String; 2] {
    let timeframe = Timeframe::AllTime.to_string();
    [
        hot_tags_key_parts(ranked_only, &[&timeframe]),
        hot_tags_key_parts(ranked_only, &[TAGGERS_INDEX, &timeframe]),
    ]
    .map(|key_parts| format!("{HOT_TAGS_CACHE_PREFIX}:{}", key_parts.join(":")))
}

/// The ranked tagger the warm-up test brings along. The post has no author, so
/// the post tag indexes, which read tags through `AUTHORED`, never see it.
const TAGGER_ID: &str = "cachesetup-warmup-tagger";
const TAGGED_POST_ID: &str = "cachesetup-warmup-post";
const TAG_LABEL: &str = "cachesetupwarmup";

/// Removes the tagger and its post, also what an interrupted run left behind.
async fn remove_trusted_tagger() -> Result<()> {
    let query = Query::new(
        "cachesetup_remove_trusted_tagger",
        "MATCH (n) WHERE (n:User AND n.id = $user_id) OR (n:Post AND n.id = $post_id)
         DETACH DELETE n",
    )
    .param("user_id", TAGGER_ID)
    .param("post_id", TAGGED_POST_ID);
    exec_single_row(query).await?;
    Ok(())
}

/// A user with positive trust who tagged one post: enough for a non-empty
/// ranking and a non-empty ranked hot-tags scan, whatever the fixture holds.
async fn create_trusted_tagger() -> Result<()> {
    remove_trusted_tagger().await?;
    let query = Query::new(
        "cachesetup_create_trusted_tagger",
        "CREATE (u:User {id: $user_id, name: 'cachesetup tagger', trust: 1.0})
         CREATE (p:Post {id: $post_id, indexed_at: $indexed_at})
         CREATE (u)-[:TAGGED {id: $tag_id, label: $label, indexed_at: $indexed_at}]->(p)",
    )
    .param("user_id", TAGGER_ID)
    .param("post_id", TAGGED_POST_ID)
    .param("tag_id", "cachesetup-warmup-tag")
    .param("label", TAG_LABEL)
    .param("indexed_at", 1_700_000_000_000_i64);
    exec_single_row(query).await?;
    Ok(())
}

/// Which of the keys the warm-up is judged by exist.
struct WarmedKeys {
    ranking: bool,
    ranked: Vec<(String, bool)>,
    unranked: Vec<(String, bool)>,
}

async fn existing(keys: &[String]) -> Result<Vec<(String, bool)>> {
    let mut redis_conn = get_redis_conn().await?;
    let mut found = Vec::new();
    for key in keys {
        found.push((key.clone(), redis_conn.exists(key).await?));
    }
    Ok(found)
}

/// Drops the ranking and both global all_time hot-tags variants, so the next
/// warm-up writes them from the graph alone.
async fn drop_ranking_and_hot_tags() -> Result<()> {
    let mut redis_conn = get_redis_conn().await?;
    let _: () = redis_conn.del(trust_ranking_key()).await?;
    let _: () = redis_conn.del(&global_all_time_hot_tags_keys(true)).await?;
    let _: () = redis_conn
        .del(&global_all_time_hot_tags_keys(false))
        .await?;
    Ok(())
}

async fn warm_up_from_dropped_keys() -> Result<WarmedKeys> {
    create_trusted_tagger().await?;
    drop_ranking_and_hot_tags().await?;

    reindex::warm_up().await;

    let mut redis_conn = get_redis_conn().await?;
    Ok(WarmedKeys {
        ranking: redis_conn.exists(trust_ranking_key()).await?,
        ranked: existing(&global_all_time_hot_tags_keys(true)).await?,
        unranked: existing(&global_all_time_hot_tags_keys(false)).await?,
    })
}

/// Takes the tagger out of the graph and out of what the warm-up wrote for it,
/// then warms the cache again from the graph as it was before the test.
async fn restore_cache_without_tagger() -> Result<()> {
    remove_trusted_tagger().await?;
    drop_ranking_and_hot_tags().await?;
    TagSearch::remove_from_index_sorted_set(None, &TAGS_LABEL, &[TAG_LABEL]).await?;
    reindex::warm_up().await;
    Ok(())
}

/// Runs `body`, then `restore`, then hands back what `body` returned. `body`
/// runs on its own task, so a panic inside it (`reindex::warm_up()` reports
/// every failure with `expect`) comes back as an error after the restore ran.
/// `restore` runs on its own task as well, so its panic is a failure like any
/// other. When both fail, the body's error comes back with the restore's as
/// context.
async fn restored_after<T, B, R>(body: B, restore: R) -> Result<T>
where
    T: Send + 'static,
    B: Future<Output = Result<T>> + Send + 'static,
    R: Future<Output = Result<()>> + Send + 'static,
{
    let outcome = tokio::spawn(body)
        .await
        .map_err(|e| anyhow::anyhow!("panicked before the restore: {e}"))
        .and_then(|returned| returned);
    let restored = tokio::spawn(restore)
        .await
        .map_err(|e| anyhow::anyhow!("the restore panicked: {e}"))
        .and_then(|returned| returned);
    match (outcome, restored) {
        (outcome, Ok(())) => outcome,
        (Ok(_), Err(restore_error)) => Err(restore_error),
        (Err(body_error), Err(restore_error)) => Err(body_error.context(format!(
            "the restore failed as well ({restore_error:#}), after the body failed"
        ))),
    }
}

#[tokio_shared_rt::test(shared)]
async fn restore_runs_after_a_body_that_panics() {
    let restored = Arc::new(AtomicBool::new(false));
    let flag = restored.clone();

    let outcome: Result<()> = restored_after(async { panic!("the warm-up gave up") }, async move {
        flag.store(true, Ordering::SeqCst);
        Ok(())
    })
    .await;

    assert!(
        restored.load(Ordering::SeqCst),
        "the restore should run after a body that panicked"
    );
    let error = outcome.expect_err("the panic should come back as an error");
    assert!(
        error.to_string().contains("the warm-up gave up"),
        "the error should carry the panic message, got {error}"
    );
}

#[tokio_shared_rt::test(shared)]
async fn restore_hands_back_the_body_value_or_its_own_failure() {
    let outcome = restored_after(async { Ok(7) }, async { Ok(()) }).await;
    assert_eq!(outcome.expect("the body's value should come back"), 7);

    let outcome = restored_after(async { Ok(7) }, async {
        Err(anyhow::anyhow!("restore failed"))
    })
    .await;
    let error = outcome.expect_err("a failed restore should fail the run");
    assert!(
        error.to_string().contains("restore failed"),
        "the restore's error should come back, got {error}"
    );
}

#[tokio_shared_rt::test(shared)]
async fn restore_failure_keeps_the_body_failure() {
    let outcome: Result<()> =
        restored_after(async { Err(anyhow::anyhow!("body failed")) }, async {
            Err(anyhow::anyhow!("restore failed"))
        })
        .await;

    let error = outcome.expect_err("two failures should fail the run");
    let rendered = format!("{error:#}");
    for message in ["body failed", "restore failed"] {
        assert!(
            rendered.contains(message),
            "the error should carry \"{message}\", got {rendered}"
        );
    }
}

#[tokio_shared_rt::test(shared)]
async fn restore_panic_keeps_the_body_failure() {
    let outcome: Result<()> =
        restored_after(async { Err(anyhow::anyhow!("body failed")) }, async {
            panic!("the restore gave up")
        })
        .await;

    let error = outcome.expect_err("two failures should fail the run");
    let rendered = format!("{error:#}");
    for message in ["body failed", "the restore gave up"] {
        assert!(
            rendered.contains(message),
            "the error should carry \"{message}\", got {rendered}"
        );
    }
}

#[tokio_shared_rt::test(shared)]
async fn warm_up_builds_the_ranking_before_the_hot_tags() -> Result<()> {
    StackManager::setup(&StackConfig::default())
        .await
        .map_err(|e| anyhow::anyhow!("could not initialise the stack: {e:?}"))?;

    // Cleaned up before anything is asserted, so a failed assertion, an error
    // or a panic in the warm-up leaves nothing behind.
    let warmed =
        restored_after(warm_up_from_dropped_keys(), restore_cache_without_tagger()).await?;

    let ranking_key = trust_ranking_key();
    assert!(
        warmed.ranking,
        "the warm-up should have rebuilt the trust ranking at {ranking_key}"
    );
    for (key, exists) in &warmed.ranked {
        assert!(
            exists,
            "the warm-up should have written the ranked hot tags at {key}"
        );
    }
    // Warmed before the ranking exists, the hot tags land here instead.
    for (key, exists) in &warmed.unranked {
        assert!(
            !exists,
            "the warm-up should not have written the unranked hot tags at {key}"
        );
    }

    Ok(())
}
