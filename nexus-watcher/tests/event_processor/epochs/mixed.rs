use super::utils::{assert_no_trace, create_v0_user, event_uri};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::post::PostDetails;
use nexus_common::models::user::UserCounts;
use pubky_social_specs::legacy_v0::{PubkyAppPost, PubkyAppPostKind};
use pubky_social_specs::{PubkySocialPost, PubkySocialPostKind};

/// One user writes in both epochs: the v0 post is indexed and the v1 post is not.
#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v0_and_v1_posts_same_user() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (user_kp, user_id) = create_v0_user(&mut test, "Watcher:Epochs:Mixed").await?;

    let v1_post = PubkySocialPost::new(
        "test_homeserver_v0_and_v1_posts_same_user v1".to_string(),
        PubkySocialPostKind::Note,
        None,
        None,
        vec![],
    );
    let (v1_post_id, v1_post_path) = test.create_v1_post(&user_kp, &v1_post).await?;

    let v0_post = PubkyAppPost {
        content: "test_homeserver_v0_and_v1_posts_same_user v0".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (v0_post_id, _) = test.create_post(&user_kp, &v0_post).await?;

    let v0_details = PostDetails::get_by_id(&user_id, &v0_post_id)
        .await?
        .expect("The v0 post should be indexed");
    assert_eq!(v0_details.content, v0_post.content);
    assert!(PostDetails::get_by_id(&user_id, &v1_post_id)
        .await?
        .is_none());

    let counts = UserCounts::get_by_id(&user_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(counts.posts, 1);

    assert_no_trace(&event_uri(&user_id, &v1_post_path)).await
}
