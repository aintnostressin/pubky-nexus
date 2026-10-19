use crate::{
    event_processor::users::utils::assert_no_influencer_score,
    event_processor::utils::watcher::WatcherTest,
};
use anyhow::Result;
use pubky::Keypair;
use pubky_app_specs::{
    post_uri_builder, PubkyAppPost, PubkyAppPostEmbed, PubkyAppPostKind, PubkyAppUser,
};

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_post_influencer() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let alice_user_kp = Keypair::random();

    let alice_user = PubkyAppUser {
        bio: Some("test_homeserver_post_influencer".to_string()),
        image: None,
        links: None,
        name: "Watcher:PostInfluencer:Alice".to_string(),
        status: None,
    };
    let alice_id = test.create_user(&alice_user_kp, &alice_user).await?;

    // Alice creates a new post
    let alice_post = PubkyAppPost {
        content: "Watcher:PostInfluencer:Alice:Post".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };

    let (alice_post_id, _alice_post_path) = test.create_post(&alice_user_kp, &alice_post).await?;

    // Posts, follows, replies and reposts change influencer scores, but the watcher
    // leaves Sorted:Users:Influencers to the all-time job: no event may write it.
    assert_no_influencer_score(&alice_id).await;

    // Create new user
    let bob_user_kp = Keypair::random();

    let bob_user = PubkyAppUser {
        bio: Some("test_homeserver_post_influencer".to_string()),
        image: None,
        links: None,
        name: "Watcher:PostInfluencer:Bob".to_string(),
        status: None,
    };
    let bob_id = test.create_user(&bob_user_kp, &bob_user).await?;

    // Follow Alice
    test.create_follow(&bob_user_kp, &alice_id).await?;

    assert_no_influencer_score(&alice_id).await;

    // Bob replies to popular alice post
    let alice_post_uri = post_uri_builder(alice_id.clone(), alice_post_id.clone());

    let reply = PubkyAppPost {
        content: "Watcher:PostInfluencer:Bob:Reply".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: Some(alice_post_uri.clone()),
        attachments: None,
        embed: None,
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
            uri: alice_post_uri.clone(),
        }),
        attachments: None,
        lock: None,
    };

    test.create_post(&bob_user_kp, &repost).await?;

    assert_no_influencer_score(&alice_id).await;
    assert_no_influencer_score(&bob_id).await;

    // Follow Bob
    test.create_follow(&alice_user_kp, &bob_id).await?;

    assert_no_influencer_score(&bob_id).await;

    // TODO: Impl DEL post. Assert the reply does not exist in Nexus
    // test.cleanup_post(&user_id, &reply_id).await?;

    // TODO: Cleanup
    test.cleanup_user(&alice_user_kp).await?;
    test.cleanup_user(&bob_user_kp).await?;
    //test.cleanup_post(&user_id, &parent_post_id).await?;

    Ok(())
}
