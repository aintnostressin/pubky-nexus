//! A Sybil attack on recommended users, over docker/test-graph/mocks/recommended-sybil.cypher.
//!
//! The attacker runs a farm that nobody honest follows, each account with the five posts the
//! activity threshold asks for. Its entry account follows an honest user and gets followed
//! back. That one edge puts the farm at depth 2 of the follow-backer and at depth 3 of
//! everyone who follows them, which is where `recommend_users` looks. Without a trust
//! ranking nothing tells a farm account from a real one; with one, unranked accounts are
//! never recommended.

use std::collections::HashSet;

use crate::utils::{
    ranking::without_ranking,
    recommended::{recommend, recommend_cold, recommended_cache_key},
    recommended_sybil::{ENTRY, FARM, FOLLOWBACKER, FOLLOWER, HONEST_A, HONEST_B, HONEST_C},
    server::TestServiceServer,
};
use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use nexus_common::models::user::{
    CACHE_USER_RECOMMENDED_EMPTY_MARKER, CACHE_USER_RECOMMENDED_EMPTY_TTL,
};

fn ids(list: &[&str]) -> HashSet<String> {
    list.iter().map(|id| id.to_string()).collect()
}

/// The farm sits at depth 2 of the account that followed its entry back, and every account
/// in it is unranked.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_sybil_farm_is_not_recommended_to_the_followbacker() -> Result<()> {
    let (_, pool) = recommend_cold(FOLLOWBACKER, 20).await?;

    assert_eq!(
        pool,
        ids(&[HONEST_C]),
        "Only the ranked candidate should be recommended, not the farm behind the follow-back"
    );

    Ok(())
}

/// Followers of the follow-backer reach the entry at depth 2 and the farm at depth 3. Their
/// honest candidates come back most trusted first: HONEST_C leads though its id sorts last.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_sybil_farm_is_not_recommended_to_the_followbackers_followers(
) -> Result<()> {
    let (page, pool) = recommend_cold(FOLLOWER, 1).await?;

    assert_eq!(
        pool,
        ids(&[HONEST_A, HONEST_B, HONEST_C]),
        "Neither the entry nor the farm should be recommended to the follow-backer's followers"
    );
    assert_eq!(
        page,
        [HONEST_C],
        "The most trusted candidate should be served first"
    );

    Ok(())
}

/// A pool cached before the ranking was published, or before its farm left the ranking, is not
/// served as cached: a hit drops and evicts users absent from the published ranking. The pool
/// is planted under HONEST_B, whose cache no other test reads, and fits one page, so the hit
/// draws all of it.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_sybil_farm_is_dropped_from_a_stale_pool() -> Result<()> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let key = recommended_cache_key(HONEST_B);
    let stale: Vec<&str> = [ENTRY, HONEST_A, HONEST_C]
        .into_iter()
        .chain(FARM.into_iter().take(15))
        .collect();
    let _: () = redis_conn.del(&key).await?;
    let _: () = redis_conn.sadd(&key, &stale).await?;

    let served = recommend(HONEST_B, 20).await;
    let left: HashSet<String> = redis_conn.smembers(&key).await?;
    let _: () = redis_conn.del(&key).await?;

    assert_eq!(
        served?.into_iter().collect::<HashSet<_>>(),
        ids(&[HONEST_A, HONEST_C]),
        "Only the ranked users of a stale pool should be served"
    );
    assert_eq!(
        left,
        ids(&[HONEST_A, HONEST_C]),
        "The entry and the farm should be evicted from the cached pool"
    );

    Ok(())
}

/// With a ranking published, an empty pool is cached briefly, as a marker, for a user who
/// follows someone, so their reach is not walked on every request. HONEST_A follows only
/// HONEST_C, who follows nobody.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_empty_pool_is_cached_briefly() -> Result<()> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let key = recommended_cache_key(HONEST_A);
    let _: () = redis_conn.del(&key).await?;

    let served = recommend(HONEST_A, 20).await;
    // The second request hits the marker, which has to survive it.
    let served_again = recommend(HONEST_A, 20).await;
    let pool: HashSet<String> = redis_conn.smembers(&key).await?;
    let ttl: i64 = redis_conn.ttl(&key).await?;
    let _: () = redis_conn.del(&key).await?;

    assert!(served?.is_empty(), "HONEST_A should have no candidates");
    assert!(
        served_again?.is_empty(),
        "The cached empty pool should be served empty"
    );
    assert_eq!(
        pool,
        ids(&[CACHE_USER_RECOMMENDED_EMPTY_MARKER]),
        "The empty pool should be cached as the marker alone"
    );
    assert!(
        ttl > 0 && ttl <= CACHE_USER_RECOMMENDED_EMPTY_TTL,
        "The empty pool should expire within {CACHE_USER_RECOMMENDED_EMPTY_TTL}s, got {ttl}s"
    );

    Ok(())
}

/// A user who follows nobody is not cached empty, so their first follows bring recommendations
/// right away. HONEST_C follows nobody.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_empty_pool_of_a_user_following_nobody_is_not_cached() -> Result<()> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let key = recommended_cache_key(HONEST_C);
    let _: () = redis_conn.del(&key).await?;

    let served = recommend(HONEST_C, 20).await;
    let cached: bool = redis_conn.exists(&key).await?;
    let _: () = redis_conn.del(&key).await?;

    assert!(served?.is_empty(), "HONEST_C should have no candidates");
    assert!(
        !cached,
        "An empty pool should not be cached for a user following nobody"
    );

    Ok(())
}

/// Without a ranking, candidates are neither filtered nor reordered, and the attack works:
/// the entry and the whole farm reach the follow-backer's followers. Runs alone, see
/// `.config/nextest.toml`.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_sybil_farm_is_recommended_without_a_ranking() -> Result<()> {
    let (_, pool) = without_ranking(recommend_cold(FOLLOWER, 20)).await?;

    let mut expected = ids(&FARM);
    expected.extend(ids(&[ENTRY, HONEST_A, HONEST_B, HONEST_C]));
    assert_eq!(
        pool, expected,
        "Without a ranking, the entry and the farm should be recommended with the honest candidates"
    );

    Ok(())
}
