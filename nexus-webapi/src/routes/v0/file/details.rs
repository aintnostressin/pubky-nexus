use crate::routes::v0::endpoints::FILE_ROUTE;
use crate::routes::Path;
use crate::{Error, Result};
use axum::Json;
use nexus_common::models::file::FileDetails;
use nexus_common::models::file::FileUrls;
use tracing::debug;
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = FILE_ROUTE,
    description = "File details",
    tag = "File",
    params(
        ("file_id" = String, Path, description = "File Pubky Uri")
    ),
    responses(
        (status = 200, description = "File Details", body = FileDetails),
        (status = 400, description = "Malformed file URI"),
        (status = 404, description = "File not found"),
        (status = 429, description = "Rate limit exceeded", headers(("Retry-After" = u64, description = "Seconds until retry"))),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn file_details_handler(Path(file_uri): Path<String>) -> Result<Json<FileDetails>> {
    debug!("GET {FILE_ROUTE} file_uri:{}", file_uri);

    if url::Url::parse(&file_uri).is_err() {
        return Err(Error::invalid_input("Malformed file URI"));
    }
    let files = FileDetails::get_by_uris(&[&file_uri]).await?;

    match files.into_iter().next().flatten() {
        None => Err(Error::FileNotFound {}),
        Some(value) => Ok(Json(value)),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(file_details_handler),
    components(schemas(FileDetails, FileUrls))
)]
pub struct FileDetailsApiDoc;
