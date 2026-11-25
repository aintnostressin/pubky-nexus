use crate::db::graph::Query;
use crate::db::kv::{RedisResult, SortOrder};
use crate::db::queries::get::MAX_QUERY_LIMIT;
use crate::db::{
    execute_graph_operation, fetch_key_from_graph, fetch_row_from_graph, queries, GraphResult,
    OperationOutcome, RedisOps,
};
use crate::models::error::ModelResult;
use async_trait::async_trait;
use chrono::Utc;

/// Runs one of the follow page queries: the page's ids, or `None` when the
/// user has no node.
pub(super) async fn fetch_follow_page(query: Query) -> GraphResult<Option<Vec<String>>> {
    fetch_key_from_graph(query, "ids").await
}

#[async_trait]
pub trait UserFollows: Sized + RedisOps + AsRef<[String]> + Default {
    fn from_vec(vec: Vec<String>) -> Self;

    async fn put_to_graph(follower_id: &str, followee_id: &str) -> GraphResult<OperationOutcome> {
        let indexed_at = Utc::now().timestamp_millis();
        let query = queries::put::create_follow(follower_id, followee_id, indexed_at);
        execute_graph_operation(query).await
    }

    /// One page of the list, read from the graph, the oldest follow first: a
    /// new follow goes to the end, so the pages before it keep their ids. The
    /// Redis set is neither read nor written. `None` when the user has no
    /// node; an existing user past the end of the list gets an empty page.
    /// Without a `limit` the page holds at most [`MAX_QUERY_LIMIT`] ids.
    ///
    /// The query caps `skip` at 10,000, and the routes refuse a larger one, so
    /// the follows past that many and one page, the newest of a longer list,
    /// cannot be reached by paging.
    async fn get_by_id(
        user_id: &str,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<Option<Self>> {
        let query = Self::get_page_query(
            user_id,
            &SortOrder::Ascending,
            skip.unwrap_or(0),
            limit.unwrap_or(MAX_QUERY_LIMIT),
        );
        Ok(fetch_follow_page(query).await?.map(Self::from_vec))
    }

    /// The `limit` most recent follows of the list, the newest first, read
    /// from the graph like [`Self::get_by_id`]. For a caller that cuts the
    /// list and must not lose the follows made last.
    async fn get_most_recent(user_id: &str, limit: usize) -> ModelResult<Option<Self>> {
        let query = Self::get_page_query(user_id, &SortOrder::Descending, 0, limit);
        Ok(fetch_follow_page(query).await?.map(Self::from_vec))
    }

    /// The whole list in one read, for [`Self::reindex`].
    async fn get_from_graph(user_id: &str) -> GraphResult<Option<Self>> {
        let query = Self::get_query(user_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        let Some(row) = maybe_row else {
            return Ok(None);
        };

        let user_exists: bool = row.get("user_exists").unwrap_or(false);
        if !user_exists {
            return Ok(None);
        }

        let user_follows = match row.get(Self::get_ids_field_name()) {
            Ok(Some(connections)) => Some(Self::from_vec(connections)),
            Ok(None) => Some(Self::default()),
            Err(_e) => None,
        };

        Ok(user_follows)
    }

    async fn put_to_index(&self, user_id: &str) -> RedisResult<()> {
        let user_list_ref: Vec<&str> = self.as_ref().iter().map(|id| id.as_str()).collect();
        Self::put_index_set(&[user_id], &user_list_ref, None, None).await
    }

    async fn reindex(user_id: &str) -> ModelResult<()> {
        match Self::get_from_graph(user_id).await? {
            Some(follow) => follow.put_to_index(user_id).await?,
            None => tracing::error!(
                "{}: Could not found user follow relationship in the graph",
                user_id
            ),
        }
        Ok(())
    }

    async fn del_from_graph(follower_id: &str, followee_id: &str) -> GraphResult<OperationOutcome> {
        let query = queries::del::delete_follow(follower_id, followee_id);
        execute_graph_operation(query).await
    }

    async fn del_from_index(&self, user_id: &str) -> RedisResult<()> {
        self.remove_from_index_set(&[user_id]).await
    }

    /// The whole list, for [`Self::get_from_graph`].
    fn get_query(user_id: &str) -> Query;

    /// One page of the list by the time of the follow in `order`, for
    /// [`Self::get_by_id`] and [`Self::get_most_recent`].
    fn get_page_query(user_id: &str, order: &SortOrder, skip: usize, limit: usize) -> Query;

    fn get_ids_field_name() -> &'static str;

    // Checks whether user_a is (following | follower) of user_b
    async fn check_in_index(user_a_id: &str, user_b_id: &str) -> RedisResult<bool> {
        let user_a_key_parts = &[user_a_id][..];
        let (_, follow) = Self::check_set_member(user_a_key_parts, user_b_id).await?;
        Ok(follow)
    }
}
