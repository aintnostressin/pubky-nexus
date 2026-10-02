//! Ids from docker/test-graph/mocks/recommended.cypher, the fixture behind the
//! recommended users tests. See its header for the follow topology.

use std::collections::HashSet;

use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use nexus_common::models::user::CACHE_USER_RECOMMENDED_KEY_PARTS;

use super::{get_request, server::TestServiceServer};

pub const OBS: &str = "w39631pxk8epa77ztwmrw5qxgwp1nimeqtahw31d7bnzzrtg94ao";
pub const HOP: &str = "wtqrgw3s7w4qgxwhychgho191aqotmoejcdp4enctkrcthn3y37o";
pub const FOLLOWED: &str = "xmnoqzjw956gpboe3ne95feqbk9yf6wbem347ukayxq1jdi1jmxy";
pub const SHORT: &str = "xos98pobdo9wm4qoiq5rgkrdjmfysn8q5wk7g4yb8yy59kufnory";
pub const D2: &str = "ya4cjt3j6b58hqf4sujfuj954pymkd6j8ukrbb8pqud5dyguigio";
pub const D3: &str = "ywpwmrrndzumk68oszuieigct5bap68oobbdkou8ks1djxyh3w5o";
pub const D4: &str = "yx8ztwc65s1djkoy518n5p6m67kjbcj1wmmjn7jrhzb1yx9mzwcy";
pub const DELETED: &str = "z48xiqc4mcfiicwi1ewgn7swkhuem86wbkyoudt9yqidcxadt4to";

/// The Redis key caching the recommendations of `user_id`.
pub fn recommended_cache_key(user_id: &str) -> String {
    format!("{}:{user_id}", CACHE_USER_RECOMMENDED_KEY_PARTS.join(":"))
}

/// Requests `user_id`'s recommendations as they are served, from the cache when it holds them.
pub async fn recommend(user_id: &str, limit: usize) -> Result<Vec<String>> {
    let res = get_request(&format!(
        "/v0/stream/users/ids?source=recommended&user_id={user_id}&limit={limit}"
    ))
    .await?;
    Ok(res
        .as_array()
        .expect("User id stream should be an array")
        .iter()
        .map(|id| id.as_str().expect("User id should be a string").to_string())
        .collect())
}

/// Requests `user_id`'s recommendations on a cold cache, so they come straight from the
/// graph query. Returns the served page, in query order and with any duplicate the set-backed
/// cache would hide, and the whole pool the request cached, which is then dropped again so no
/// test leaves one behind for another reader.
pub async fn recommend_cold(user_id: &str, limit: usize) -> Result<(Vec<String>, HashSet<String>)> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let key = recommended_cache_key(user_id);
    let _: () = redis_conn.del(&key).await?;

    let page = recommend(user_id, limit).await?;

    let pool: HashSet<String> = redis_conn.smembers(&key).await?;
    let _: () = redis_conn.del(&key).await?;
    Ok((page, pool))
}
