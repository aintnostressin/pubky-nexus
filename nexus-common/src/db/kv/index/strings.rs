//! Redis primitives for plain string keys that must expire.

use crate::db::get_redis_conn;
use crate::db::kv::error::RedisResult;
use deadpool_redis::redis;

/// Writes `value` under `prefix:key`, expiring after `ttl_secs`. `SET EX` writes
/// the value and its expiry in one command, so the key never exists without a TTL.
pub async fn put_with_ttl(prefix: &str, key: &str, value: &str, ttl_secs: u64) -> RedisResult<()> {
    let index_key = format!("{prefix}:{key}");
    let mut conn = get_redis_conn().await?;
    let _: () = redis::cmd("SET")
        .arg(&index_key)
        .arg(value)
        .arg("EX")
        .arg(ttl_secs)
        .query_async(&mut conn)
        .await?;
    Ok(())
}

/// Whether `prefix:key` exists.
pub async fn exists(prefix: &str, key: &str) -> RedisResult<bool> {
    let index_key = format!("{prefix}:{key}");
    let mut conn = get_redis_conn().await?;
    let found: bool = redis::cmd("EXISTS")
        .arg(&index_key)
        .query_async(&mut conn)
        .await?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::get_redis_conn;
    use crate::{types::DynError, StackConfig, StackManager};
    use deadpool_redis::redis::{self, AsyncCommands};
    use std::time::Duration;

    const TEST_PREFIX: &str = "StringTtlTest";

    async fn ttl_of(full_key: &str) -> Result<i64, DynError> {
        let mut conn = get_redis_conn().await?;
        let ttl: i64 = redis::cmd("TTL")
            .arg(full_key)
            .query_async(&mut conn)
            .await?;
        Ok(ttl)
    }

    #[tokio_shared_rt::test(shared)]
    async fn put_with_ttl_leaves_no_key_without_a_ttl() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;

        let key = "armed";
        let full_key = format!("{TEST_PREFIX}:{key}");
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(&full_key).await?;

        put_with_ttl(TEST_PREFIX, key, "1", 60).await?;
        let ttl = ttl_of(&full_key).await?;
        assert!(ttl > 0 && ttl <= 60, "expected a TTL in 1..=60, got {ttl}");
        assert!(exists(TEST_PREFIX, key).await?);

        // A key that was persistent before the write must come out with a TTL.
        let _: () = conn.set(&full_key, "persistent").await?;
        assert_eq!(ttl_of(&full_key).await?, -1);

        put_with_ttl(TEST_PREFIX, key, "1", 60).await?;
        let ttl = ttl_of(&full_key).await?;
        assert!(ttl > 0 && ttl <= 60, "expected a TTL in 1..=60, got {ttl}");

        let _: () = conn.del(&full_key).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn exists_is_false_for_a_missing_key_and_after_expiry() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;

        let key = "expiring";
        let full_key = format!("{TEST_PREFIX}:{key}");
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(&full_key).await?;

        assert!(!exists(TEST_PREFIX, key).await?);

        put_with_ttl(TEST_PREFIX, key, "1", 1).await?;
        assert!(exists(TEST_PREFIX, key).await?);

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!exists(TEST_PREFIX, key).await?);

        Ok(())
    }
}
