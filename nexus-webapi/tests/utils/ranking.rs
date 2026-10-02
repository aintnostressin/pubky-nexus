//! The shared trust ranking, `Sorted:Users:SocialGraph`, for tests that need it gone.

use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};

use anyhow::Result;
use futures_util::FutureExt;
use nexus_common::db::RedisOps;
use nexus_common::models::user::{SocialGraphStatus, USER_SOCIAL_GRAPH_KEY_PARTS};

use super::server::TestServiceServer;

/// Runs `probe` with the trust ranking dropped, then rebuilds the ranking from the graph,
/// whether the probe succeeds, fails or panics, so it is never left missing for later tests.
///
/// Every read of the ranking sees the gap, so callers run alone (`.config/nextest.toml`).
pub async fn without_ranking<T>(probe: impl Future<Output = Result<T>>) -> Result<T> {
    // Ensure the server is running, so the Redis and Neo4j pools are initialized
    TestServiceServer::get_test_server().await;
    // Replacing with nothing drops the key, as a rebuild with nothing to rank does.
    SocialGraphStatus::replace_index_sorted_set(&USER_SOCIAL_GRAPH_KEY_PARTS, &[], None, None)
        .await?;

    let outcome = AssertUnwindSafe(probe).catch_unwind().await;
    SocialGraphStatus::reindex().await?;
    outcome.unwrap_or_else(|panic| resume_unwind(panic))
}
