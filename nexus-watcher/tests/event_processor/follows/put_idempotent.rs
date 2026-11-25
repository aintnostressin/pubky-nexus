use super::utils::find_follow_relationship;
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::{
    db::RedisOps,
    models::{
        follow::{Followers, Following, UserFollows},
        notification::Notification,
    },
    types::Pagination,
    utils::test_utils::default_ingestor_tests,
};
use nexus_watcher::events::handlers::follow;
use pubky::Keypair;
use pubky_social_specs::legacy_v0::PubkyAppUser;
use pubky_social_specs::PubkyId;

/// Test that calling sync_put twice (simulating a retry) does not double
/// counters, duplicate index entries, or create extra notifications.
#[tokio_shared_rt::test(shared)]
async fn test_follow_put_idempotent() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create follower
    let follower_kp = Keypair::random();
    let follower_user = PubkyAppUser {
        bio: Some("test_follow_put_idempotent".to_string()),
        image: None,
        links: None,
        name: "Watcher:IdempotentPut:Follower".to_string(),
        status: None,
    };
    let follower_id = test.create_user(&follower_kp, &follower_user).await?;

    // Create followee
    let followee_kp = Keypair::random();
    let followee_user = PubkyAppUser {
        bio: Some("test_follow_put_idempotent".to_string()),
        image: None,
        links: None,
        name: "Watcher:IdempotentPut:Followee".to_string(),
        status: None,
    };
    let followee_id = test.create_user(&followee_kp, &followee_user).await?;

    // First follow (normal flow through event processing)
    test.create_follow(&follower_kp, &followee_id).await?;

    // Verify initial state: counts = 1
    let followee_counts = find_user_counts(&followee_id).await;
    assert_eq!(
        followee_counts.followers, 1,
        "Followee should have 1 follower"
    );
    let follower_counts = find_user_counts(&follower_id).await;
    assert_eq!(
        follower_counts.following, 1,
        "Follower should be following 1"
    );

    // Verify index membership
    let (_, is_follower) = Followers::check_set_member(&[&followee_id], &follower_id).await?;
    assert!(is_follower, "Follower should be in followee's follower set");
    let (_, is_following) = Following::check_set_member(&[&follower_id], &followee_id).await?;
    assert!(
        is_following,
        "Followee should be in follower's following set"
    );

    // Count notifications before retry
    let notifications_before = Notification::get_by_id(&followee_id, Pagination::default()).await?;
    let notification_count_before = notifications_before.len();

    // Simulate retry: call sync_put directly with the same follower/followee
    let follower_pubky = PubkyId::from(follower_kp.clone());
    let followee_pubky = PubkyId::from(followee_kp.clone());
    follow::sync_put(follower_pubky, followee_pubky, &default_ingestor_tests()).await?;

    // Verify counts are unchanged (not doubled)
    let followee_counts = find_user_counts(&followee_id).await;
    assert_eq!(
        followee_counts.followers, 1,
        "Followee should still have 1 follower after retry"
    );
    let follower_counts = find_user_counts(&follower_id).await;
    assert_eq!(
        follower_counts.following, 1,
        "Follower should still be following 1 after retry"
    );

    // Verify index membership is unchanged
    let (_, is_follower) = Followers::check_set_member(&[&followee_id], &follower_id).await?;
    assert!(
        is_follower,
        "Follower should still be in followee's follower set after retry"
    );
    let (_, is_following) = Following::check_set_member(&[&follower_id], &followee_id).await?;
    assert!(
        is_following,
        "Followee should still be in follower's following set after retry"
    );

    // Verify no duplicate notifications
    let notifications_after = Notification::get_by_id(&followee_id, Pagination::default()).await?;
    assert_eq!(
        notifications_after.len(),
        notification_count_before,
        "No new notifications should be created on retry"
    );

    // Verify graph relationship still exists
    let exists = find_follow_relationship(&follower_id, &followee_id).await?;
    assert!(exists, "Follow relationship should still exist in graph");

    // Cleanup
    test.cleanup_user(&follower_kp).await?;
    test.cleanup_user(&followee_kp).await?;

    Ok(())
}

/// Test partial failure recovery: graph edge was created on a previous attempt
/// but index writes failed. On retry, sync_put should recover both indexes
/// without duplicating counters or notifications.
#[tokio_shared_rt::test(shared)]
async fn test_follow_put_recovers_missing_indexes() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create follower
    let follower_kp = Keypair::random();
    let follower_user = PubkyAppUser {
        bio: Some("test_follow_put_recovers_missing_indexes".to_string()),
        image: None,
        links: None,
        name: "Watcher:RecoverPut:Follower".to_string(),
        status: None,
    };
    let follower_id = test.create_user(&follower_kp, &follower_user).await?;

    // Create followee
    let followee_kp = Keypair::random();
    let followee_user = PubkyAppUser {
        bio: Some("test_follow_put_recovers_missing_indexes".to_string()),
        image: None,
        links: None,
        name: "Watcher:RecoverPut:Followee".to_string(),
        status: None,
    };
    let followee_id = test.create_user(&followee_kp, &followee_user).await?;

    // Normal follow (graph + indexes + counters all complete)
    test.create_follow(&follower_kp, &followee_id).await?;

    // Simulate partial failure: remove both index entries as if they were never written
    let followers = Followers(vec![follower_id.to_string()]);
    let following = Following(vec![followee_id.to_string()]);
    followers.del_from_index(&followee_id).await?;
    following.del_from_index(&follower_id).await?;

    // Verify indexes are gone
    let (_, is_follower) = Followers::check_set_member(&[&followee_id], &follower_id).await?;
    assert!(!is_follower, "Follower index should be missing");
    let (_, is_following) = Following::check_set_member(&[&follower_id], &followee_id).await?;
    assert!(!is_following, "Following index should be missing");

    // Record counters and notifications before recovery
    let counts_before = find_user_counts(&followee_id).await;
    let notifications_before = Notification::get_by_id(&followee_id, Pagination::default()).await?;

    // Simulate retry: sync_put hits Updated (graph edge exists) and runs recovery
    let follower_pubky = PubkyId::from(follower_kp.clone());
    let followee_pubky = PubkyId::from(followee_kp.clone());
    follow::sync_put(follower_pubky, followee_pubky, &default_ingestor_tests()).await?;

    // Verify both indexes are recovered
    let (_, is_follower) = Followers::check_set_member(&[&followee_id], &follower_id).await?;
    assert!(
        is_follower,
        "Follower index should be recovered after retry"
    );
    let (_, is_following) = Following::check_set_member(&[&follower_id], &followee_id).await?;
    assert!(
        is_following,
        "Following index should be recovered after retry"
    );

    // Verify counters were NOT incremented again
    let counts_after = find_user_counts(&followee_id).await;
    assert_eq!(
        counts_before.followers, counts_after.followers,
        "Follower count should not change on recovery"
    );

    // Verify no duplicate notifications
    let notifications_after = Notification::get_by_id(&followee_id, Pagination::default()).await?;
    assert_eq!(
        notifications_before.len(),
        notifications_after.len(),
        "No new notifications should be created on recovery"
    );

    // Cleanup
    test.cleanup_user(&follower_kp).await?;
    test.cleanup_user(&followee_kp).await?;

    Ok(())
}

/// A follow list is ordered by the time each follow was last indexed, and a PUT
/// of a follow that already exists (a retry, a replayed batch) indexes it again:
/// that follow moves to the newest end of the list. This pins the limit the
/// follow page query documents; it fails once `create_follow` keeps the time of
/// the first PUT, and that note then goes.
#[tokio_shared_rt::test(shared)]
async fn test_follow_put_again_moves_the_follow_to_the_end_of_the_list() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let user = |name: &str| PubkyAppUser {
        bio: Some("test_follow_put_again_moves_the_follow".to_string()),
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    };
    let follower_kp = Keypair::random();
    let follower_id = test
        .create_user(&follower_kp, &user("Watcher:PutAgain:Follower"))
        .await?;
    let first_kp = Keypair::random();
    let first_id = test
        .create_user(&first_kp, &user("Watcher:PutAgain:First"))
        .await?;
    let second_kp = Keypair::random();
    let second_id = test
        .create_user(&second_kp, &user("Watcher:PutAgain:Second"))
        .await?;

    // Two follows in different milliseconds, so the order is by time, not by id
    let apart = std::time::Duration::from_millis(5);
    test.create_follow(&follower_kp, &first_id).await?;
    tokio::time::sleep(apart).await;
    test.create_follow(&follower_kp, &second_id).await?;

    let following = Following::get_by_id(&follower_id, None, None).await?;
    assert_eq!(
        following.map(|f| f.0),
        Some(vec![first_id.clone(), second_id.clone()]),
        "the oldest follow comes first"
    );

    // The first follow is PUT again, as a retry would
    tokio::time::sleep(apart).await;
    follow::sync_put(
        PubkyId::from(follower_kp.clone()),
        PubkyId::from(first_kp.clone()),
        &default_ingestor_tests(),
    )
    .await?;

    let following = Following::get_by_id(&follower_id, None, None).await?;
    assert_eq!(
        following.map(|f| f.0),
        Some(vec![second_id.clone(), first_id.clone()]),
        "the follow indexed again is now the newest"
    );

    test.cleanup_user(&follower_kp).await?;
    test.cleanup_user(&first_kp).await?;
    test.cleanup_user(&second_kp).await?;

    Ok(())
}
