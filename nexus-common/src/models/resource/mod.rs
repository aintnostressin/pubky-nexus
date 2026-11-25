pub mod stream;
pub mod tag;
pub mod view;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::{exec_single_row, fetch_all_rows_from_graph, queries};
use crate::models::error::ModelResult;

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct ResourceDetails {
    pub id: String,
    pub uri: String,
    pub scheme: String,
    pub indexed_at: i64,
}

impl ResourceDetails {
    /// The id and address of every Resource whose address starts with `prefix`.
    pub async fn ids_by_uri_prefix(prefix: &str) -> ModelResult<Vec<(String, String)>> {
        let rows = fetch_all_rows_from_graph(queries::get::resources_by_uri_prefix(prefix)).await?;
        rows.iter()
            .map(|row| Ok((row.get("resource_id")?, row.get("uri")?)))
            .collect()
    }

    /// Deletes the Resources `resource_ids` names, with every tag on them. A missing one is
    /// skipped.
    pub async fn delete(resource_ids: &[String]) -> ModelResult<()> {
        if resource_ids.is_empty() {
            return Ok(());
        }
        exec_single_row(queries::del::delete_resources(resource_ids)).await?;
        Ok(())
    }
}
