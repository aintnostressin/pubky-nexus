use super::utils::{
    check_member_total_engagement_user_posts, find_post_counts, short_post, short_reply, test_user,
};
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::RedisOps;
use nexus_common::models::post::{PostCounts, PostStream};
use pubky::Keypair;
use pubky_app_specs::{
    post_uri_builder, PubkyAppPost, PubkyAppPostEmbed, PubkyAppPostKind, PubkyAppTag, PubkyAppUser,
};

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_post_engagement() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let alice_user_kp = Keypair::random();

    let alice = PubkyAppUser {
        bio: Some("test_homeserver_post_engagement".to_string()),
        image: None,
        links: None,
        name: "Watcher:PostEngagement:Alice".to_string(),
        status: None,
    };
    let alice_id = test.create_user(&alice_user_kp, &alice).await?;

    // Alice creates a new post
    let alice_post = PubkyAppPost {
        content: "Watcher:PostEngagement:Alice:Post".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };

    let (alice_post_id, _alice_post_path) = test.create_post(&alice_user_kp, &alice_post).await?;

    let alice_post_key: [&str; 2] = [&alice_id, &alice_post_id];

    // CACHE_OP: Assert cache index exist
    // post engagement: Sorted:Posts:Global:TotalEngagement:user_id:post_id
    let total_engagement = check_member_total_engagement_user_posts(&alice_post_key)
        .await
        .unwrap();
    assert!(total_engagement.is_some());
    assert_eq!(total_engagement.unwrap(), 0);

    // Create new user
    let bob_user_kp = Keypair::random();

    let bob_user = PubkyAppUser {
        bio: Some("test_homeserver_post_engagement".to_string()),
        image: None,
        links: None,
        name: "Watcher:PostEngagement:Bob".to_string(),
        status: None,
    };
    let _bob_id = test.create_user(&bob_user_kp, &bob_user).await?;

    // Bob replies to popular alice post
    let alice_post_absolute_uri = post_uri_builder(alice_id.clone(), alice_post_id.clone());

    let reply = PubkyAppPost {
        content: "Watcher:PostInfluencer:Bob:Reply".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: Some(alice_post_absolute_uri.clone()),
        embed: None,
        attachments: None,
        lock: None,
    };

    let (_reply_id, _reply_path) = test.create_post(&bob_user_kp, &reply).await?;

    // Create repost of alice post
    let repost = PubkyAppPost {
        content: "Watcher:PostInfluencer:Bob:Repost".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: Some(PubkyAppPostEmbed {
            kind: PubkyAppPostKind::Short,
            uri: alice_post_absolute_uri.clone(),
        }),
        attachments: None,
        lock: None,
    };

    test.create_post(&bob_user_kp, &repost).await?;

    let total_engagement = check_member_total_engagement_user_posts(&alice_post_key)
        .await
        .unwrap();
    assert!(total_engagement.is_some());
    assert_eq!(total_engagement.unwrap(), 2);

    // // // TODO: Impl DEL post. Assert the reply does not exist in Nexus
    // test.cleanup_post(&user_id, &reply_id).await?;

    // Cleanup
    test.cleanup_user(&alice_user_kp).await?;
    test.cleanup_user(&bob_user_kp).await?;
    //test.cleanup_post(&user_id, &parent_post_id).await?;

    Ok(())
}

/// Read-through must not seed the engagement feed. `get_by_id` recomputes a
/// cache miss via `cache_json` (JSON only), so a read must not resurrect a post
/// into `POST_TOTAL_ENGAGEMENT`.
#[tokio_shared_rt::test(shared)]
async fn test_post_counts_read_does_not_seed_engagement() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Watcher:CountsReadEngagement".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let post = PubkyAppPost {
        content: "root".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (post_id, _post_path) = test.create_post(&user_kp, &post).await?;

    let post_key: [&str; 2] = [&user_id, &post_id];
    assert!(
        check_member_total_engagement_user_posts(&post_key)
            .await?
            .is_some(),
        "root post is seeded into the engagement feed at creation"
    );

    // Remove it from the feed, then force a read-through cache miss.
    PostStream::delete_from_engagement_sorted_set(&user_id, &post_id).await?;
    PostCounts::remove_from_index_multiple_json(&[&post_key]).await?;
    PostCounts::get_by_id(&user_id, &post_id).await?;

    assert!(
        check_member_total_engagement_user_posts(&post_key)
            .await?
            .is_none(),
        "read-through recompute must not seed POST_TOTAL_ENGAGEMENT"
    );

    test.cleanup_user(&user_kp).await?;
    Ok(())
}

/// The watcher keeps the engagement feed current but never writes or drops
/// `Post:Counts` JSON: a cached entry outlives replies, tags and deletes until
/// its TTL, and replies stay out of the feed.
#[tokio_shared_rt::test(shared)]
async fn test_watcher_leaves_post_counts_cache_alone() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let alice_kp = Keypair::random();
    let alice_id = test
        .create_user(&alice_kp, &test_user("Watcher:CountsCache:Alice", "counts"))
        .await?;
    let bob_kp = Keypair::random();
    let bob_id = test
        .create_user(&bob_kp, &test_user("Watcher:CountsCache:Bob", "counts"))
        .await?;

    let (post_id, _) = test.create_post(&alice_kp, &short_post("root")).await?;
    let post_key: [&str; 2] = [&alice_id, &post_id];
    assert!(
        PostCounts::get_from_index(&alice_id, &post_id)
            .await?
            .is_none(),
        "a post put writes no counts JSON"
    );
    assert_eq!(
        check_member_total_engagement_user_posts(&post_key).await?,
        Some(0)
    );
    let cached = PostCounts::get_by_id(&alice_id, &post_id)
        .await?
        .expect("counts of an indexed post");

    let post_uri = post_uri_builder(alice_id.clone(), post_id.clone());
    let (reply_id, reply_path) = test
        .create_post(&bob_kp, &short_reply("reply", post_uri.clone()))
        .await?;
    let tag = PubkyAppTag {
        uri: post_uri,
        label: "counts_cache".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&bob_kp, &tag.hs_path(), tag).await?;

    assert_eq!(
        check_member_total_engagement_user_posts(&post_key).await?,
        Some(2)
    );
    let reply_key: [&str; 2] = [&bob_id, &reply_id];
    assert!(check_member_total_engagement_user_posts(&reply_key)
        .await?
        .is_none());
    assert!(PostCounts::get_from_index(&bob_id, &reply_id)
        .await?
        .is_none());
    let graph = find_post_counts(&alice_id, &post_id).await;
    assert_eq!((graph.replies, graph.tags), (1, 1));
    let index = PostCounts::get_from_index(&alice_id, &post_id)
        .await?
        .expect("the read-through entry is still cached");
    assert_eq!(
        (index.replies, index.tags),
        (cached.replies, cached.tags),
        "reply and tag events leave the cached counts as they were"
    );

    test.cleanup_post(&bob_kp, &reply_path).await?;
    assert_eq!(
        check_member_total_engagement_user_posts(&post_key).await?,
        Some(1)
    );

    let (lone_id, lone_path) = test.create_post(&alice_kp, &short_post("lone")).await?;
    PostCounts::get_by_id(&alice_id, &lone_id).await?;
    test.cleanup_post(&alice_kp, &lone_path).await?;
    assert!(
        check_member_total_engagement_user_posts(&[&alice_id, &lone_id])
            .await?
            .is_none()
    );
    assert!(
        PostCounts::get_from_index(&alice_id, &lone_id)
            .await?
            .is_some(),
        "a post delete leaves its cached counts to expire"
    );

    test.cleanup_user(&alice_kp).await?;
    test.cleanup_user(&bob_kp).await?;
    Ok(())
}
