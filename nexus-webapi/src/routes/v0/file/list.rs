use crate::models::FileUris;
use crate::routes::v0::endpoints::FILE_LIST_ROUTE;
use crate::routes::Json as RequestJson;
use crate::Result;
use axum::Json;
use nexus_common::models::file::FileDetails;
use serde::Deserialize;
use tracing::{debug, warn};
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, ToSchema)]
pub struct FilesByIdsBody {
    uris: FileUris,
}

#[utoipa::path(
    post,
    path = FILE_LIST_ROUTE,
    description = "List files by URIs. This is a POST request because we're passing a potentially large list of file URIs in the request body.",
    tag = "File",
    request_body = FilesByIdsBody,
    responses(
        (status = 200, description = "List of File Details", body = Vec<FileDetails>),
        (status = 429, description = "Rate limit exceeded", headers(("Retry-After" = u64, description = "Seconds until retry"))),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn file_details_by_uris_handler(
    RequestJson(body): RequestJson<FilesByIdsBody>,
) -> Result<Json<Vec<FileDetails>>> {
    debug!("GET {FILE_LIST_ROUTE} uris:{:?}", body.uris);

    let uris: Vec<&str> = body.uris.iter().map(String::as_str).collect();
    let files = FileDetails::get_by_uris(&uris).await?;
    for (uri, file) in uris.iter().zip(&files) {
        if file.is_none() {
            warn!("No file found for URI: {}", uri);
        }
    }
    let data: Vec<FileDetails> = files.into_iter().flatten().collect();
    Ok(Json(data))
}

#[derive(OpenApi)]
#[openapi(
    paths(file_details_by_uris_handler),
    components(schemas(FileDetails, FilesByIdsBody))
)]
pub struct FilesListApiDoc;
