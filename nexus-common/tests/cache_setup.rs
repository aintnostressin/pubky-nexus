//! Integration coverage for the RediSearch schema bootstrap in `setup_cache`.
//!
//! Requires a running Redis with the query engine module:
//! `docker compose -f docker/docker-compose.yml up -d redis`.
//! This is the first integration test in `nexus-common` — the rest of the suite
//! is pure unit tests that need no services.
//!
//! # Why this test is not destructive
//!
//! The intended coverage was the full self-heal cycle: create the index, `FLUSHDB`,
//! assert the index is gone, then `clear_redis()` and assert it came back. That has
//! to run against an isolated logical database so it cannot wipe the mock dataset in
//! DB 0 that the webapi and watcher suites read.
//!
//! RediSearch does not allow it. `SELECT 9` succeeds, but the very next `FT.CREATE`
//! fails with `Cannot create index on db != 0` (verified against the pinned
//! `redis:8.0.6-alpine`, search module 80001). Indexes exist only in DB 0, so there
//! is no isolated database to be destructive in, and pointing `FLUSHDB` at DB 0
//! would race every other test reading the same keyspace.
//!
//! # What this test does and does not cover
//!
//! Covered: schema drift. `ft_create_post_content_index` swallows `"already exists"`,
//! so an index left behind by an older schema (the v1 `content`-only index, say) is
//! never rewritten, and the field assertions below fail on it.
//!
//! Covered only conditionally: that `setup_cache` is wired into
//! `RedisConnector::init`. The index is DB 0 server state that outlives this process,
//! so `FT.INFO` succeeding cannot by itself distinguish "this process created it"
//! from "it was already there". Against a Redis with no index the test does catch an
//! unwired `RedisConnector::init` (verified by deleting the `setup_cache().await?`
//! call: the test fails with `no such index`). Against CI it does not —
//! `.github/workflows/test.yml` runs `nexusd db mock` before this suite, and that
//! reaches `setup_cache` through `clear_redis` as well, so the index is already
//! present by the time the test runs. Do not rely on this test to protect the
//! connector wiring; like the self-heal cycle above it needs an isolated logical
//! database RediSearch will not give us, and stays a manual check
//! (`nexusd db clear --yes` followed by `redis-cli FT.INFO postContentIdx`).

use nexus_common::db::{get_redis_conn, RedisConnector};
use nexus_common::StackConfig;

/// The live `postContentIdx` indexes the three fields the current schema declares.
///
/// Catches "index created with the wrong schema" — see the module docs for why it
/// catches "`setup_cache` never ran" only when the index is absent to begin with,
/// which is not the case under CI. Non-destructive: `FT.CREATE` no-ops when the
/// index exists, so this leaves a populated DB 0 untouched.
#[tokio::test]
async fn post_content_index_has_expected_schema() {
    let uri = StackConfig::default().db.redis;
    RedisConnector::init(&uri)
        .await
        .expect("Failed to initialise RedisConnector — is Redis running?");

    let mut conn = get_redis_conn()
        .await
        .expect("Failed to get a Redis connection");

    let info = redis::cmd("FT.INFO")
        .arg("postContentIdx")
        .query_async::<redis::Value>(&mut conn)
        .await
        .expect("FT.INFO postContentIdx failed — no post content index in Redis DB 0");

    // Assert on the debug string rather than the parsed reply: FT.INFO returns a flat
    // array under RESP2 and a map under RESP3, so the shape depends on the negotiated
    // protocol while the field names are present either way.
    let rendered = format!("{info:?}");
    for field in ["content", "author", "kind"] {
        assert!(
            rendered.contains(field),
            "FT.INFO reply is missing the '{field}' field: {rendered}"
        );
    }
}
