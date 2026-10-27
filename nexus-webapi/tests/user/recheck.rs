use crate::utils::invalid_put_request;
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::utils::test_utils::random_pubky_id;
use nexus_webapi::routes::v0::endpoints::RECHECK_USER_ROUTE;
use serde_json::Value;

/// A user nexus does not know cannot be rechecked: 404, ingest first.
#[tokio_shared_rt::test(shared)]
async fn test_recheck_unknown_user_is_not_found() -> Result<()> {
    let user_id = random_pubky_id();
    let url_path = RECHECK_USER_ROUTE.replace("{user_id}", user_id.as_ref());

    invalid_put_request(&url_path, Value::Null, StatusCode::NOT_FOUND).await?;

    Ok(())
}
