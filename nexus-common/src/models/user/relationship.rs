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

        let mut found = Vec::with_capacity(rows.len());
        for row in rows {
            let relationship = Self {
                following: row.get("following")?,
                followed_by: row.get("followed_by")?,
            };
            found.push((row.get("id")?, relationship));
        }
        Ok(match_to_ids(&ids, found))
    }
}

/// Gives each id the relationship of the row that carries it, as the query
/// keeps no order: `None` where no row came back, and the same relationship in
/// every slot of a repeated id.
fn match_to_ids(ids: &[&str], found: Vec<(String, Relationship)>) -> Vec<Option<Relationship>> {
    let by_id: HashMap<String, Relationship> = found.into_iter().collect();
    ids.iter().map(|id| by_id.get(*id).cloned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FOLLOWING: Relationship = Relationship {
        following: true,
        followed_by: false,
    };
    const FOLLOWED_BY: Relationship = Relationship {
        following: false,
        followed_by: true,
    };

    /// Rows in any order: each id gets the relationship whose row carries it.
    #[test]
    fn match_to_ids_follows_the_ids_not_the_rows() {
        let rows = vec![("b".to_string(), FOLLOWED_BY), ("a".to_string(), FOLLOWING)];

        let slots = match_to_ids(&["a", "b"], rows);

        assert_eq!(slots, vec![Some(FOLLOWING), Some(FOLLOWED_BY)]);
    }

    /// An id the query returned no row for, a user without a node, stays empty.
    #[test]
    fn match_to_ids_leaves_a_missing_id_empty() {
        let rows = vec![("b".to_string(), FOLLOWED_BY)];

        let slots = match_to_ids(&["a", "b", "c"], rows);

        assert_eq!(slots, vec![None, Some(FOLLOWED_BY), None]);
    }

    /// The query returns one row for an id asked for twice: both slots get it.
    #[test]
    fn match_to_ids_fills_each_slot_of_a_repeated_id() {
        let rows = vec![("a".to_string(), FOLLOWING), ("b".to_string(), FOLLOWED_BY)];

        let slots = match_to_ids(&["a", "b", "a"], rows);

        assert_eq!(
            slots,
            vec![Some(FOLLOWING), Some(FOLLOWED_BY), Some(FOLLOWING)]
        );
    }
}
