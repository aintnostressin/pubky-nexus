//! Redis primitives for a TTL-based distributed lock. The TTL frees the lock if
//! a holder dies without releasing it.

use crate::db::get_redis_conn;
use crate::db::kv::error::RedisResult;
use deadpool_redis::redis::{self, Script};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

/// Compare-and-delete: release only if the token still matches, so a re-taken
/// lease isn't dropped by the previous holder.
static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end",
    )
});

/// Compare-and-expire: renew only while the token still holds the lock.
static EXTEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('expire', KEYS[1], ARGV[2])
        else
            return 0
        end",
    )
});

/// A token unique to one lock holder: `<pid>-<process start>-<counter>`. The
/// start time keeps tokens distinct when a later process reuses the pid.
pub fn new_lock_token() -> String {
    static STARTED: LazyLock<i64> = LazyLock::new(|| chrono::Utc::now().timestamp_micros());
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        *STARTED,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Tries to claim `key` with `token` for `ttl_secs`. `Ok(false)` when another
/// holder has it. `SET NX EX` acquires and arms the expiry atomically.
pub async fn try_acquire_lock(key: &str, token: &str, ttl_secs: u64) -> RedisResult<bool> {
    let mut conn = get_redis_conn().await?;
    // SET returns Some when NX succeeds, None when the key exists.
    let acquired: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg(token)
        .arg("NX")
        .arg("EX")
        .arg(ttl_secs)
        .query_async(&mut conn)
        .await?;
    Ok(acquired.is_some())
}

/// Releases `key` only if still held by `token` (see [`RELEASE`]).
pub async fn release_lock(key: &str, token: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: i64 = RELEASE.key(key).arg(token).invoke_async(&mut conn).await?;
    Ok(())
}

/// Renews `key` for `ttl_secs` only if still held by `token` (see [`EXTEND`]).
/// `Ok(false)` once the lock is lost.
async fn extend_lock(key: &str, token: &str, ttl_secs: u64) -> RedisResult<bool> {
    let mut conn = get_redis_conn().await?;
    let extended: i64 = EXTEND
        .key(key)
        .arg(token)
        .arg(ttl_secs)
        .invoke_async(&mut conn)
        .await?;
    Ok(extended == 1)
}

/// A lock on `key` under a token of its own, released however its holder exits.
///
/// Create it before the first acquire attempt: an acquire whose reply is lost
/// after `SET` landed, or a holder cancelled mid-acquire, then still releases.
/// The release is token-scoped, so it does nothing if the token never took the
/// key.
///
/// Prefer [`release`](Self::release). Dropping a lease that may still hold the
/// lock, as when its future is cancelled, releases in a spawned task: that
/// needs the Tokio runtime to outlive it, and the TTL frees the lock otherwise.
pub struct LockLease {
    key: String,
    token: String,
    armed: bool,
}

impl LockLease {
    pub fn new(key: impl Into<String>) -> Self {
        LockLease {
            key: key.into(),
            token: new_lock_token(),
            armed: true,
        }
    }

    /// Tries to take the lock for `ttl_secs`. `Ok(false)` while another holder
    /// has it.
    pub async fn try_acquire(&self, ttl_secs: u64) -> RedisResult<bool> {
        try_acquire_lock(&self.key, &self.token, ttl_secs).await
    }

    /// Renews the lock for `ttl_secs`. `Ok(false)` once it has been lost.
    pub async fn extend(&self, ttl_secs: u64) -> RedisResult<bool> {
        extend_lock(&self.key, &self.token, ttl_secs).await
    }

    /// Forgets a lock this lease never took, skipping the release.
    pub fn disarm(mut self) {
        self.armed = false;
    }

    /// Releases the lock. On an error the TTL frees it; a release cancelled
    /// midway is retried from `Drop`.
    pub async fn release(mut self) -> RedisResult<()> {
        let released = release_lock(&self.key, &self.token).await;
        self.armed = false;
        released
    }
}

impl Drop for LockLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Outside a runtime there is nothing to release on; the TTL frees the lock.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let key = std::mem::take(&mut self.key);
        let token = std::mem::take(&mut self.token);
        runtime.spawn(async move {
            if let Err(e) = release_lock(&key, &token).await {
                tracing::warn!(%key, "Could not release a dropped lock, its TTL frees it: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DynError;
    use crate::{StackConfig, StackManager};
    use redis::AsyncCommands;
    use std::time::Duration;

    type TestResult = Result<(), DynError>;

    async fn setup(key: &str) -> TestResult {
        StackManager::setup(&StackConfig::default()).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(key).await?;
        Ok(())
    }

    async fn holder(key: &str) -> Result<Option<String>, DynError> {
        let mut conn = get_redis_conn().await?;
        Ok(conn.get(key).await?)
    }

    #[test]
    fn tokens_are_unique() {
        assert_ne!(new_lock_token(), new_lock_token());
    }

    /// The release runs in a spawned task, so it is awaited by polling.
    #[tokio_shared_rt::test(shared)]
    async fn a_dropped_lease_releases_its_lock() -> TestResult {
        let key = "test:lock-lease:dropped";
        setup(key).await?;
        let lease = LockLease::new(key);
        assert!(lease.try_acquire(60).await?);

        drop(lease);
        let mut released = false;
        for _ in 0..100 {
            if holder(key).await?.is_none() {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(released, "a dropped lease left its lock held");
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn a_lease_leaves_another_holders_lock_alone() -> TestResult {
        let key = "test:lock-lease:other-holder";
        setup(key).await?;
        let other = new_lock_token();
        assert!(try_acquire_lock(key, &other, 60).await?);

        let lease = LockLease::new(key);
        let taken = lease.try_acquire(60).await?;
        let extended = lease.extend(60).await?;
        lease.release().await?;
        let kept = holder(key).await?;
        release_lock(key, &other).await?;

        assert!(!taken);
        assert!(!extended);
        assert_eq!(kept, Some(other));
        Ok(())
    }
}
