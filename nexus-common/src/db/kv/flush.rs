use crate::db::get_redis_conn;
use crate::db::kv::setup::setup_cache;
use crate::db::kv::RedisResult;

pub async fn clear_redis() -> RedisResult<()> {
    let mut redis_conn = get_redis_conn().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    // FLUSHDB drops the FT index, so re-apply the schema
    setup_cache().await?;
    Ok(())
}
