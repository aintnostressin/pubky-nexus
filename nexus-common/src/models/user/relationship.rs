use crate::db::{fetch_all_rows_from_graph, queries};
use crate::models::error::ModelResult;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Represents the relationship of the user that views and user being viewed.
#[derive(Serialize, Deserialize, ToSchema, Debug, Default, Clone, PartialEq)]
pub struct Relationship {
    pub following: bool,
    pub followed_by: bool,
}

impl Relationship {
    /// `None` without a viewer or when either user has no graph node.
    pub async fn get_by_id(user_id: &str, viewer_id: Option<&str>) -> ModelResult<Option<Self>> {
        let Some(viewer_id) = viewer_id else {
            return Ok(None);
        };
        let mut relationships = Self::get_by_ids(&[user_id], viewer_id).await?;
        Ok(relationships.pop().flatten())
    }

    /// The viewer's relationship with each of `user_ids`, read from the graph
    /// in one query: one slot per id, in the same order. A slot is `None` when
    /// that user or the viewer has no graph node.
    pub async fn get_by_ids<T: AsRef<str>>(
        user_ids: &[T],
        viewer_id: &str,
    ) -> ModelResult<Vec<Option<Self>>> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<&str> = user_ids.iter().map(AsRef::as_ref).collect();
        let rows =
            fetch_all_rows_from_graph(queries::get::user_relationships(&ids, viewer_id)).await?;

        let mut by_id: HashMap<String, Self> = HashMap::with_capacity(rows.len());
        for row in rows {
            let relationship = Self {
                following: row.get("following")?,
                followed_by: row.get("followed_by")?,
            };
            by_id.insert(row.get("id")?, relationship);
        }
        Ok(ids.iter().map(|id| by_id.get(*id).cloned()).collect())
    }
}
