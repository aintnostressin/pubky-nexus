use crate::db::kv::SortOrder;
use crate::db::queries::get::MAX_QUERY_LIMIT;
use crate::db::{queries, RedisOps};
use crate::models::error::ModelResult;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::following::Following;
use super::traits::fetch_follow_page;

#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct Friends(pub Vec<String>);

impl Friends {
    /// One page of the users `user_id` follows who follow back, read from the
    /// graph in the order of the following list, the oldest follow first.
    /// `None` when the user has no node; an existing user with no friends gets
    /// an empty page. Without a `limit` the page holds at most
    /// [`MAX_QUERY_LIMIT`] ids.
    pub async fn get_by_id(
        user_id: &str,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<Option<Self>> {
        let query = queries::get::get_user_friends_page(
            user_id,
            &SortOrder::Ascending,
            skip.unwrap_or(0),
            limit.unwrap_or(MAX_QUERY_LIMIT),
        );
        Ok(fetch_follow_page(query).await?.map(Self))
    }

    /// The `limit` friends `user_id` followed most recently, the newest first.
    /// See [`super::UserFollows::get_most_recent`].
    pub async fn get_most_recent(user_id: &str, limit: usize) -> ModelResult<Option<Self>> {
        let query = queries::get::get_user_friends_page(user_id, &SortOrder::Descending, 0, limit);
        Ok(fetch_follow_page(query).await?.map(Self))
    }

    /// Whether both users are in each other's `Following` set. The watcher's
    /// check: it reads the sets the follow handlers maintain, not the graph.
    pub async fn check(user_a_id: &str, user_b_id: &str) -> ModelResult<bool> {
        let user_a_key_parts = &[user_a_id][..];
        let user_b_key_parts = &[user_b_id][..];

        let ((_, a_follows_b), (_, b_follows_a)) = tokio::try_join!(
            Following::check_set_member(user_a_key_parts, user_b_id),
            Following::check_set_member(user_b_key_parts, user_a_id),
        )?;

        Ok(a_follows_b && b_follows_a)
    }
}
