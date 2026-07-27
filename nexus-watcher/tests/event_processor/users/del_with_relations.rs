use crate::{
    event_processor::users::utils::{find_user_counts, find_user_details},
    event_processor::utils::watcher::WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::user::{UserCounts, UserStream, UserView};
use pubky::Keypair;
use pubky_app_specs::{
    traits::{HasIdPath, HashId},
    PubkyAppBlob, PubkyAppFile, PubkyAppPost, PubkyAppPostKind, PubkyAppUser, PubkyAppUserLink,
};

#[tokio_shared_rt::test(shared)]
async fn test_delete_user_with_relationships() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create a new user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_delete_user_with_relationships".to_string()),
        image: None,
        links: None,
        name: "Watcher:UserDeleteWith:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Create a post to establish a relationship
    let post = PubkyAppPost {
        content: "User's first post".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (_post_id, post_path) = test.create_post(&user_kp, &post).await?;

    // Delete the user
    test.cleanup_user(&user_kp).await?;

    // Fetch user details; profile fields must be unchanged, only the deleted flag should be set.
    let user_details = find_user_details(&user_id).await?;
    assert_eq!(
        user_details.name, "Watcher:UserDeleteWith:User",
        "User name should be unchanged after deletion (del() no longer overwrites name)"
    );
    assert!(
        user_details.deleted,
        "User should be marked as deleted after deletion with relationships"
    );
    // NOTE: bio, status, image, links are unchanged because del() no longer clears fields.
    assert_eq!(
        user_details.bio,
        Some("test_delete_user_with_relationships".to_string()),
        "User bio should be unchanged after deletion"
    );

    // User counts should still exist
    let user_counts = find_user_counts(&user_id).await;
    assert_eq!(
        user_counts.posts, 1,
        "User should still have posts count after deletion"
    );

    // User view should reflect the updated details
    let user_view = UserView::get_by_id(&user_id, None, None).await.unwrap();
    assert!(
        user_view.is_some(),
        "User view should be present after deletion"
    );
    let user_view = user_view.unwrap();
    assert_eq!(
        user_view.details.name, "Watcher:UserDeleteWith:User",
        "User view name should be unchanged after deletion (del() no longer overwrites name)"
    );
    assert!(
        user_view.details.deleted,
        "User view should report deleted: true after tombstoning"
    );

    // Now delete the user's post
    test.cleanup_post(&user_kp, &post_path).await?;

    // Write and delete the user again; this time it should be fully removed
    test.create_profile(&user_kp, &user).await?;
    test.cleanup_user(&user_kp).await?;

    // Attempt to find user details; should not exist
    let user_details_result = find_user_details(&user_id).await;
    assert!(
        user_details_result.is_err(),
        "User details should not be found after final deletion"
    );

    // Attempt to find user counts; should not exist
    let user_counts_result = UserCounts::get_by_id(&user_id).await.unwrap();
    assert!(
        user_counts_result.is_none(),
        "User counts should not be found after deletion"
    );

    // User view should not be found
    let user_view = UserView::get_by_id(&user_id, None, None).await.unwrap();
    assert!(
        user_view.is_none(),
        "User view should not be found after final deletion"
    );

    // Create a User with image and links
    let user_with_kp = Keypair::random();
    let mut user_with = PubkyAppUser {
        bio: Some("test_delete_user_with_relationships".to_string()),
        image: None,
        links: None,
        name: "Watcher:UserDeleteWith:UserWith".to_string(),
        status: None,
    };
    let user_with_id = test.create_user(&user_with_kp, &user_with).await?;

    // Add image to the user
    let blob_data = "Image bytes blob".to_string();
    let blob = PubkyAppBlob::new(blob_data.as_bytes().to_vec());
    let blob_id = blob.create_id();
    let blob_relative_url = PubkyAppBlob::create_path(&blob_id);

    test.create_file_from_body(&user_with_kp, &blob_relative_url, blob.0.clone())
        .await?;

    // Act
    let file = PubkyAppFile {
        name: "Watcher:UserDeleteWith:UserWith:Image".to_string(),
        content_type: "image/png".to_string(),
        src: blob_relative_url.clone(),
        size: blob.0.len(),
        created_at: Utc::now().timestamp_millis(),
    };

    let (_file_id, file_path) = test.create_file(&user_with_kp, &file).await?;

    user_with = PubkyAppUser {
        bio: Some("test_delete_user_with_relationships".to_string()),
        image: Some(file_path.to_string()),
        links: Some(vec![PubkyAppUserLink {
            title: "Heaven".to_string(),
            url: "pubky://rest.now".to_string(),
        }]),
        name: "Watcher:UserDeleteWith:UserWith".to_string(),
        status: Some("Zombie soon".to_string()),
    };
    let _ = test.create_profile(&user_with_kp, &user_with).await?;

    // Create a post to establish a relationship
    let post_b = PubkyAppPost {
        content: "User's Second post".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (_post_b_id, post_b_path) = test.create_post(&user_with_kp, &post_b).await?;

    // Delete the user
    test.cleanup_user(&user_with_kp).await?;

    // Fetch user details; profile fields must be unchanged, only the deleted flag should be set.
    let user_details = find_user_details(&user_with_id).await?;
    assert_eq!(
        user_details.name, "Watcher:UserDeleteWith:UserWith",
        "User name should be unchanged after deletion (del() no longer overwrites name)"
    );
    assert!(
        user_details.deleted,
        "User should be marked as deleted after deletion with relationships"
    );
    // All profile fields preserved — del() no longer clears them.
    assert_eq!(
        user_details.bio,
        Some("test_delete_user_with_relationships".to_string()),
        "User bio should be unchanged after deletion"
    );
    assert_eq!(
        user_details.status,
        Some("Zombie soon".to_string()),
        "User status should be unchanged after deletion"
    );
    assert_eq!(
        user_details.image,
        Some(file_path.to_string()),
        "User image should be unchanged after deletion"
    );
    assert!(
        user_details.links.is_some(),
        "User links should be unchanged after deletion"
    );

    // User counts should still exist
    let user_counts = find_user_counts(&user_with_id).await;
    assert_eq!(
        user_counts.posts, 1,
        "User should still have posts count after deletion"
    );

    // User view should reflect the updated details
    let user_view = UserView::get_by_id(&user_with_id, None, None)
        .await
        .unwrap();
    assert!(
        user_view.is_some(),
        "User view should be present after deletion"
    );
    let user_view = user_view.unwrap();
    assert_eq!(
        user_view.details.name, "Watcher:UserDeleteWith:UserWith",
        "User view name should be unchanged after deletion"
    );
    assert!(
        user_view.details.deleted,
        "User view should report deleted: true after tombstoning"
    );

    // Now delete the user's post
    test.cleanup_post(&user_with_kp, &post_b_path).await?;

    // Write and delete the user again; this time it should be fully removed
    test.create_profile(&user_with_kp, &user_with).await?;
    test.cleanup_user(&user_with_kp).await?;
    // Delete the file
    test.cleanup_file(&user_with_kp, &file_path).await?;

    // Attempt to find user details; should not exist
    let user_details_result = find_user_details(&user_with_id).await;
    assert!(
        user_details_result.is_err(),
        "User details should not be found after final deletion"
    );

    // Attempt to find user counts; should not exist
    let user_counts_result = UserCounts::get_by_id(&user_with_id).await.unwrap();
    assert!(
        user_counts_result.is_none(),
        "User counts should not be found after deletion"
    );

    // User view should not be found
    let user_view = UserView::get_by_id(&user_with_id, None, None)
        .await
        .unwrap();
    assert!(
        user_view.is_none(),
        "User view should not be found after final deletion"
    );

    Ok(())
}

/// Scenario:
/// - ensure a user is recommended in the Who To Follow list
/// - delete that user
/// - ensure that user is not recommended anymore in that list
#[tokio_shared_rt::test(shared)]
async fn test_delete_recommended_user() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let fn_create_user =
        async |test: &mut WatcherTest, keypair: &Keypair, name: &str| -> Result<String> {
            let user = PubkyAppUser {
                bio: Some("test_delete_user_with_relationships".to_string()),
                image: None,
                links: None,
                name: format!("Watcher:UserDeleteWith:User:{name}"),
                status: None,
            };
            test.create_user(keypair, &user).await
        };

    // Create 3 users: Alice, Bob, Carol
    let alice_kp = Keypair::random();
    let bob_kp = Keypair::random();
    let carol_kp = Keypair::random();
    let alice_id = fn_create_user(&mut test, &alice_kp, "Alice").await?;
    let bob_id = fn_create_user(&mut test, &bob_kp, "Bob").await?;
    let carol_id = fn_create_user(&mut test, &carol_kp, "Carol").await?;

    // Alice follows Bob, Bob follows Carol
    // Carol is in Alice's social graph, but not directly followed
    test.create_follow(&alice_kp, &bob_id).await?;
    test.create_follow(&bob_kp, &carol_id).await?;

    // Carol is an active user (has at least 5 posts)
    for i in 0..5 {
        let post = PubkyAppPost {
            content: format!("Carol's post {i}"),
            kind: PubkyAppPostKind::Short,
            parent: None,
            embed: None,
            attachments: None,
            lock: None,
        };
        test.create_post(&carol_kp, &post).await?;
    }

    // Check if Carol is recommended to Alice
    let alice_recommended_ids_res_1 = UserStream::get_recommended_ids(&alice_id, None).await;
    let alice_recommended_ids_1 = alice_recommended_ids_res_1.unwrap().unwrap();
    assert_eq!(alice_recommended_ids_1.len(), 1);
    assert_eq!(alice_recommended_ids_1.first(), Some(&carol_id));

    // Carol deletes her user
    test.cleanup_user(&carol_kp).await?;

    // Check if Carol is not recommended anymore to Alice
    let alice_recommended_ids_res_2 = UserStream::get_recommended_ids(&alice_id, None).await;
    let alice_recommended_ids_2 = alice_recommended_ids_res_2.unwrap();
    assert_eq!(alice_recommended_ids_2, None);

    Ok(())
}

/// Resurrection: delete a user with relationships, re-PUT a profile, assert deleted == false
/// and that the user is visible again in streams.
#[tokio_shared_rt::test(shared)]
async fn test_user_resurrection() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("Resurrection test user".to_string()),
        image: None,
        links: None,
        name: "Watcher:UserResurrection:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Create a post to establish a relationship
    let post = PubkyAppPost {
        content: "User's post before deletion".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    test.create_post(&user_kp, &post).await?;

    // Delete the user (tombstone with relationships)
    test.cleanup_user(&user_kp).await?;

    // Verify deleted flag is set
    let user_details = find_user_details(&user_id).await?;
    assert!(
        user_details.deleted,
        "User should be marked as deleted after tombstoning"
    );

    // Re-PUT a profile (resurrection)
    let revived_user = PubkyAppUser {
        bio: Some("Revived!".to_string()),
        image: None,
        links: None,
        name: "Watcher:UserResurrection:User".to_string(),
        status: None,
    };
    test.create_profile(&user_kp, &revived_user).await?;

    // Verify deleted flag is cleared — create_user always sets deleted: false
    let user_details = find_user_details(&user_id).await?;
    assert!(
        !user_details.deleted,
        "User should NOT be deleted after re-PUTting their profile (resurrection)"
    );
    assert_eq!(user_details.name, "Watcher:UserResurrection:User");
    assert_eq!(user_details.bio, Some("Revived!".to_string()));

    // User should be visible in search
    let search_result =
        nexus_common::models::user::UserSearch::get_by_name("Watcher:UserResurrection", None, None)
            .await?;
    assert!(
        search_result.is_some(),
        "Resurrected user should be visible in name search"
    );
    let search_ids = search_result.unwrap().0;
    assert!(
        search_ids.contains(&user_id),
        "Resurrected user ID should appear in search results"
    );

    Ok(())
}

/// A deleted user with relationships must still appear in name and ID search results.
/// (Requirement 3: removed the sentinel filter from put_to_index outright.)
#[tokio_shared_rt::test(shared)]
async fn test_deleted_user_remains_in_search() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Watcher:DeletedInSearch:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Create a post so the user has relationships and will be tombstoned
    let post = PubkyAppPost {
        content: "A post".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    test.create_post(&user_kp, &post).await?;

    // Delete the user
    test.cleanup_user(&user_kp).await?;

    // Verify deleted flag
    let user_details = find_user_details(&user_id).await?;
    assert!(user_details.deleted, "User should be marked as deleted");

    // Verify the user is still visible in name search
    let search_result =
        nexus_common::models::user::UserSearch::get_by_name("Watcher:DeletedInSearch", None, None)
            .await?;
    assert!(
        search_result.is_some(),
        "Deleted user should still appear in name search"
    );
    let search_ids = search_result.unwrap().0;
    assert!(
        search_ids.contains(&user_id),
        "Deleted user ID should appear in search results"
    );

    // Verify the user is still visible in ID search
    let id_search = nexus_common::models::user::UserSearch::get_by_id(
        user_id.chars().take(10).collect::<String>().as_str(),
        None,
        None,
    )
    .await?;
    assert!(
        id_search.is_some(),
        "Deleted user should still appear in ID search"
    );

    Ok(())
}

/// Regression: a live user who sets their name to the literal "[DELETED]" must still
/// appear in streams and recommendations and report deleted: false.
/// This is the bug that motivated the entire flag-based approach.
#[tokio_shared_rt::test(shared)]
async fn test_live_user_with_deleted_name_is_not_tombstoned() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    // Create a user whose name is exactly "[DELETED]"
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("I chose this name intentionally".to_string()),
        image: None,
        links: None,
        name: "[DELETED]".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Verify the user is NOT marked as deleted
    let user_details = find_user_details(&user_id).await?;
    assert_eq!(user_details.name, "[DELETED]");
    assert!(
        !user_details.deleted,
        "A live user with name '[DELETED]' should NOT be treated as deleted"
    );

    // User should be visible in search
    let search_result =
        nexus_common::models::user::UserSearch::get_by_name("DELETED", None, None).await?;
    assert!(
        search_result.is_some(),
        "User with name '[DELETED]' should be visible in name search"
    );

    Ok(())
}
