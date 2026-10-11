use anyhow::Result;
use axum::http::StatusCode;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use pubky_social_specs::legacy_v0::file_uri_builder;

use crate::utils::server::TestServiceServer;
use crate::utils::{get_request, invalid_get_request};

fn file_route(uri: &str) -> String {
    format!(
        "/v0/files/file/{}",
        url::form_urlencoded::byte_serialize(uri.as_bytes()).collect::<String>()
    )
}

#[tokio_shared_rt::test(shared)]
async fn test_file_details() -> Result<()> {
    let test_file_id = "2ZK2H8P2T5NG0";
    let test_file_user = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

    let test_file_uri = file_uri_builder(test_file_user.into(), test_file_id.into());

    let json_body = get_request(&file_route(&test_file_uri)).await?;

    assert_eq!(json_body["id"], test_file_id);
    assert_eq!(json_body["owner_id"], test_file_user);

    Ok(())
}

/// A URI missing from the index is looked up in the graph, which indexes it again.
#[tokio_shared_rt::test(shared)]
async fn test_file_details_without_uri_index_entry() -> Result<()> {
    let test_file_id = "2ZKH7K7B2RY00";
    let test_file_user = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";
    let test_file_uri = file_uri_builder(test_file_user.into(), test_file_id.into());

    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;
    let mut redis_conn = get_redis_conn().await?;
    let index_key = format!("File:Uri:{test_file_uri}");
    let _: () = redis_conn.del(&index_key).await?;

    let json_body = get_request(&file_route(&test_file_uri)).await?;
    assert_eq!(json_body["id"], test_file_id);
    assert_eq!(json_body["owner_id"], test_file_user);

    let indexed: bool = redis_conn.exists(&index_key).await?;
    assert!(indexed, "the graph lookup should index the URI");

    Ok(())
}

/// A string that is no URI is a bad request; a URI no indexed file has is not found.
#[tokio_shared_rt::test(shared)]
async fn test_file_details_unknown_and_malformed_uris() -> Result<()> {
    invalid_get_request(&file_route("not a uri"), StatusCode::BAD_REQUEST).await?;

    let unknown = file_uri_builder(
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy".into(),
        "0000000000000".into(),
    );
    invalid_get_request(&file_route(&unknown), StatusCode::NOT_FOUND).await?;

    Ok(())
}
