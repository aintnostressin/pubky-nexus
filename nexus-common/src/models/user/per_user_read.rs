use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::db::kv::{RedisResult, SortOrder};
use crate::db::RedisOps;

const PER_USER_READ: [&str; 2] = ["Homeservers", "PerUserRead"];

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

    /// The users left to the per-user read of `hs_id`, each with the cursor they start after.
    pub async fn starts(hs_id: &str) -> RedisResult<HashMap<String, u64>> {
        // Redis takes the count as a signed integer, so this is "every member".
        let every = Some(isize::MAX as usize);
        let starts = Self::try_from_index_sorted_set(
            &key(hs_id),
            None,
            None,
            None,
            every,
            SortOrder::Ascending,
            None,
        )
        .await?;
        Ok(starts
            .unwrap_or_default()
            .into_iter()
            .map(|(user_id, start)| (user_id, start as u64))
            .collect())
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

        PerUserRead::leave(&hs_id, &first, 42).await?;
        PerUserRead::leave(&hs_id, &second, 0).await?;
        PerUserRead::leave(&hs_id, &first, 100).await?;

        assert_eq!(PerUserRead::start(&hs_id, &first).await?, Some(42));
        assert_eq!(PerUserRead::start(&hs_id, &second).await?, Some(0));
        assert_eq!(PerUserRead::start(&other_hs_id, &first).await?, None);
        let expected = HashMap::from([(first, 42), (second, 0)]);
        assert_eq!(PerUserRead::starts(&hs_id).await?, expected);
        assert!(PerUserRead::starts(&other_hs_id).await?.is_empty());
        Ok(())
    }
}
