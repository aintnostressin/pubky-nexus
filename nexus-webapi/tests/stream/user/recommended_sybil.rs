//! A Sybil attack on recommended users, over docker/test-graph/mocks/recommended-sybil.cypher.
//!
//! The attacker runs a farm that nobody honest follows, each account with the five posts the
//! activity threshold asks for. Its entry account follows an honest user and gets followed
//! back. That one edge puts the farm at depth 2 of the follow-backer and at depth 3 of
//! everyone who follows them, which is where `recommend_users` looks. Without a trust
//! ranking nothing tells a farm account from a real one; with one, unranked accounts are
//! never recommended.
//!
//! These tests are in the `ranked-sets` nextest group: they read the shared trust ranking,
//! which the no-ranking test drops and rebuilds.

use std::collections::HashSet;

use crate::utils::{
    get_request,
    recommended::recommended_cache_key,
    recommended_sybil::{ENTRY, FARM, FOLLOWBACKER, FOLLOWER, HONEST_A, HONEST_B, HONEST_C},
    server::TestServiceServer,
};
use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};

/// Requests `user_id`'s recommendations on a cold cache. Returns the served page, in the
/// order the graph query produced it, and the whole pool the request cached, which is what
/// later requests draw from at random until it expires.
async fn recommend_cold(user_id: &str, limit: usize) -> Result<(Vec<String>, HashSet<String>)> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let key = recommended_cache_key(user_id);
    let _: () = redis_conn.del(&key).await?;

    let res = get_request(&format!(
        "/v0/stream/users/ids?source=recommended&user_id={user_id}&limit={limit}"
    ))
    .await?;
    let page: Vec<String> = res
        .as_array()
        .expect("User id stream should be an array")
        .iter()
        .map(|id| id.as_str().expect("User id should be a string").to_string())
        .collect();

    let pool: HashSet<String> = redis_conn.smembers(&key).await?;
    Ok((page, pool))
}

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

/// Without a ranking, candidates are neither filtered nor reordered, and the attack works:
/// the entry and the whole farm reach the follow-backer's followers.
#[tokio_shared_rt::test(shared)]
async fn test_recommended_sybil_farm_is_recommended_without_a_ranking() -> Result<()> {
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let ranking = format!("Sorted:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":"));
    let _: () = redis_conn.del(&ranking).await?;

    // Probed without `?` so an error between the delete and the rebuild cannot
    // strand the ranking for every other test.
    let probe = recommend_cold(FOLLOWER, 20).await;
    SocialGraphStatus::reindex().await?;
    let (_, pool) = probe?;

    let mut expected = ids(&FARM);
    expected.extend(ids(&[ENTRY, HONEST_A, HONEST_B, HONEST_C]));
    assert_eq!(
        pool, expected,
        "Without a ranking, the entry and the farm should be recommended with the honest candidates"
    );

    Ok(())
}
