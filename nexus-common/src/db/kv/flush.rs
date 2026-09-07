use crate::db::get_redis_conn;
use crate::db::kv::setup_cache;
use crate::db::kv::RedisResult;

/// Drops every key in the configured Redis logical database and returns Redis to
/// a bootstrapped-empty state: keys are removed and the RediSearch schema is
/// re-applied so full-text search keeps working.
pub async fn clear_redis() -> RedisResult<()> {
    let mut redis_conn = get_redis_conn().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    // FLUSHDB drops the FT index, so re-apply the schema
    setup_cache().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::clear_redis;
    use crate::db::{get_redis_conn, RedisConnector, REDIS_URI};
    use crate::types::DynError;

    /// Regression test: `clear_redis` must leave the `postContentIdx` RediSearch
    /// index in place, not just empty the keyspace. Before the schema bootstrap
    /// fix, callers that flushed Redis without recreating the index left search
    /// silently broken.
    #[tokio_shared_rt::test(shared)]
    async fn clear_redis_leaves_search_index() -> Result<(), DynError> {
        RedisConnector::init(REDIS_URI).await?;

        clear_redis().await?;

        let mut conn = get_redis_conn().await?;
        let info: Vec<redis::Value> = redis::cmd("FT.INFO")
            .arg("postContentIdx")
            .query_async(&mut conn)
            .await?;

        assert!(!info.is_empty(), "FT.INFO postContentIdx returned no data");
        Ok(())
    }
}
