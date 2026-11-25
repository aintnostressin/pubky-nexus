use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::db::kv::{RedisResult, SortOrder};
use crate::db::RedisOps;

const PER_USER_READ: [&str; 2] = ["Homeservers", "PerUserRead"];

/// How many users [`PerUserRead::starts`] reads per reply. The set only grows, and a reply
/// with every member of a very large one would outlast the time a Redis client waits for it.
const STARTS_PAGE: usize = 10_000;

/// Redis key parts of the sorted set of `hs_id`: `["Homeservers", "PerUserRead", <hs_id>]`.
fn key(hs_id: &str) -> [&str; 3] {
    [PER_USER_READ[0], PER_USER_READ[1], hs_id]
}

/// The users a homeserver's bulk read has left to its per-user read, each with the cursor
/// their per-user read starts after.
///
/// A bulk read has one cursor for every user, so an event it passes over, because its user's
/// mapping is stale or points elsewhere, is never delivered again. When it passes over an
/// event of a user it leaves the user here, with the cursor that batch was requested from: it
/// applied everything of theirs up to that cursor and nothing after it. The per-user read,
/// which has a cursor per user, reads them from there once they are mapped to the homeserver.
///
/// A user is never taken back. While they have a mapping the bulk read applies none of their
/// events, so the two reads do not interleave. The per-user read cannot list a user without
/// one, so the bulk read applies that user's events, and the per-user read replays them from
/// its cursor once the user is mapped again.
///
/// Both endpoints count in the homeserver's event ids, so a bulk cursor is a valid per-user one.
///
/// A marker type that owns the Redis operations; the users live in one sorted set per
/// homeserver, scored by their start. `Serialize`/`Deserialize` are only present to satisfy
/// the [`RedisOps`] trait bounds.
#[derive(Serialize, Deserialize)]
pub struct PerUserRead;

#[async_trait::async_trait]
impl RedisOps for PerUserRead {}

impl PerUserRead {
    /// Leaves `user_id` to the per-user read of `hs_id`, starting after `cursor`. A user
    /// already left keeps the start they have (`ZADD NX`): no event of theirs before it was
    /// passed over.
    pub async fn leave(hs_id: &str, user_id: &str, cursor: u64) -> RedisResult<()> {
        Self::add_index_sorted_set_if_absent(&key(hs_id), cursor as f64, user_id, None).await
    }

    /// The cursor `user_id` starts after on the per-user read of `hs_id`, if they are left to it.
    pub async fn start(hs_id: &str, user_id: &str) -> RedisResult<Option<u64>> {
        let start = Self::check_sorted_set_member(None, &key(hs_id), &[user_id]).await?;
        Ok(start.map(|start| start as u64))
    }

    /// Whether any user is left to the per-user read of `hs_id`.
    pub async fn any(hs_id: &str) -> RedisResult<bool> {
        let (left, _) = Self::index_sorted_set_card_and_members(&key(hs_id), &[], None).await?;
        Ok(left > 0)
    }

    /// The users left to the per-user read of `hs_id`, each with the cursor they start after.
    pub async fn starts(hs_id: &str) -> RedisResult<HashMap<String, u64>> {
        Self::starts_by_page(hs_id, STARTS_PAGE).await
    }

    /// [`Self::starts`], read `page` users per reply. A user left between two replies moves
    /// the ones after them in the set one place on, so a user can be read twice but none is
    /// missed.
    async fn starts_by_page(hs_id: &str, page: usize) -> RedisResult<HashMap<String, u64>> {
        let mut starts = HashMap::new();
        let mut read = 0;
        loop {
            let users = Self::try_from_index_sorted_set(
                &key(hs_id),
                None,
                None,
                Some(read),
                Some(page),
                SortOrder::Ascending,
                None,
            )
            .await?
            .unwrap_or_default();
            let last_page = users.len() < page;
            read += users.len();
            starts.extend(users.into_iter().map(|(id, start)| (id, start as u64)));
            if last_page {
                return Ok(starts);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{types::DynError, utils::test_utils::random_pubky_id, StackConfig, StackManager};

    /// A homeserver lists the users left to its per-user read with their starts, a user left
    /// twice keeps the first start, and another homeserver lists none of them.
    #[tokio_shared_rt::test(shared)]
    async fn test_a_user_left_to_the_per_user_read_keeps_their_first_start() -> Result<(), DynError>
    {
        StackManager::setup(&StackConfig::default()).await?;
        let hs_id = random_pubky_id().to_string();
        let other_hs_id = random_pubky_id().to_string();
        let first = random_pubky_id().to_string();
        let second = random_pubky_id().to_string();

        assert!(PerUserRead::starts(&hs_id).await?.is_empty());
        assert!(!PerUserRead::any(&hs_id).await?);

        PerUserRead::leave(&hs_id, &first, 42).await?;
        PerUserRead::leave(&hs_id, &second, 0).await?;
        PerUserRead::leave(&hs_id, &first, 100).await?;

        assert_eq!(PerUserRead::start(&hs_id, &first).await?, Some(42));
        assert_eq!(PerUserRead::start(&hs_id, &second).await?, Some(0));
        assert_eq!(PerUserRead::start(&other_hs_id, &first).await?, None);
        let expected = HashMap::from([(first, 42), (second, 0)]);
        assert_eq!(PerUserRead::starts(&hs_id).await?, expected);
        assert!(PerUserRead::starts(&other_hs_id).await?.is_empty());
        assert!(PerUserRead::any(&hs_id).await?);
        assert!(!PerUserRead::any(&other_hs_id).await?);
        Ok(())
    }

    /// Every user is read once, whether the last reply is short, full or empty.
    #[tokio_shared_rt::test(shared)]
    async fn test_the_starts_are_read_whole_over_several_replies() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let hs_id = random_pubky_id().to_string();
        let mut expected = HashMap::new();
        for start in 0..5 {
            let user_id = random_pubky_id().to_string();
            PerUserRead::leave(&hs_id, &user_id, start).await?;
            expected.insert(user_id, start);
        }

        for page in [1, 2, 5, 6] {
            let starts = PerUserRead::starts_by_page(&hs_id, page).await?;
            assert_eq!(starts, expected, "{page} per reply");
        }
        Ok(())
    }
}
