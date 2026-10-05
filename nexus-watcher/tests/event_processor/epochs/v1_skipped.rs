use super::utils::{assert_no_trace, create_v0_user, event_uri};
use crate::event_processor::follows::utils::find_follow_relationship;
use crate::event_processor::tags::utils::find_post_tag;
use crate::event_processor::users::utils::find_user_details;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::file::FileDetails;
use nexus_common::models::post::{PostCounts, PostDetails};
use nexus_common::models::traits::Collection;
use nexus_common::models::user::{UserCounts, UserDetails};
use pubky::{Keypair, ResourcePath};
use pubky_social_specs::legacy_v0::{post_uri_builder, PubkyAppPost, PubkyAppPostKind};
use pubky_social_specs::traits::HasPath;
use pubky_social_specs::{PubkySocialPost, PubkySocialPostKind, PubkySocialTag, PubkySocialUser};

/// A user who only ever wrote a v1 profile stays unknown to Nexus.
#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v1_profile_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let user_kp = Keypair::random();
    let user = PubkySocialUser::new(
        "Watcher:Epochs:V1Profile".to_string(),
        Some("test_homeserver_v1_profile_skipped".to_string()),
        None,
        None,
        None,
    );
    let user_id = test.create_v1_user(&user_kp, &user).await?;

    assert!(find_user_details(&user_id).await.is_err());
    assert!(UserDetails::get_by_id(&user_id).await?.is_none());

    let user_path: ResourcePath = PubkySocialUser::create_path().parse()?;
    assert_no_trace(&event_uri(&user_id, &user_path)).await
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v1_post_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (user_kp, user_id) = create_v0_user(&mut test, "Watcher:Epochs:V1Post").await?;

    let post = PubkySocialPost::new(
        "test_homeserver_v1_post_skipped".to_string(),
        PubkySocialPostKind::Note,
        None,
        None,
        vec![],
    );
    let (post_id, post_path) = test.create_v1_post(&user_kp, &post).await?;

    assert!(PostDetails::get_by_id(&user_id, &post_id).await?.is_none());
    let counts = UserCounts::get_by_id(&user_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(counts.posts, 0);

    assert_no_trace(&event_uri(&user_id, &post_path)).await
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v1_tag_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (author_kp, author_id) = create_v0_user(&mut test, "Watcher:Epochs:V1Tag:Author").await?;
    let (tagger_kp, tagger_id) = create_v0_user(&mut test, "Watcher:Epochs:V1Tag:Tagger").await?;

    let post = PubkyAppPost {
        content: "test_homeserver_v1_tag_skipped".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (post_id, _) = test.create_post(&author_kp, &post).await?;

    let label = "v1_only";
    let tag = PubkySocialTag::new(
        post_uri_builder(author_id.clone(), post_id.clone()),
        label.into(),
    );
    let (_, tag_path) = test.create_v1_tag(&tagger_kp, &tag).await?;

    assert!(find_post_tag(&author_id, &post_id, label).await?.is_none());
    let post_counts = PostCounts::get_by_id(&author_id, &post_id)
        .await?
        .expect("v0 post counts");
    assert_eq!(post_counts.tags, 0);
    let tagger_counts = UserCounts::get_by_id(&tagger_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(tagger_counts.tagged, 0);

    assert_no_trace(&event_uri(&tagger_id, &tag_path)).await
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v1_follow_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (follower_kp, follower_id) =
        create_v0_user(&mut test, "Watcher:Epochs:V1Follow:Follower").await?;
    let (_, followee_id) = create_v0_user(&mut test, "Watcher:Epochs:V1Follow:Followee").await?;

    let follow_path = test.create_v1_follow(&follower_kp, &followee_id).await?;

    assert!(!find_follow_relationship(&follower_id, &followee_id).await?);
    let follower = UserCounts::get_by_id(&follower_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(follower.following, 0);
    let followee = UserCounts::get_by_id(&followee_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(followee.followers, 0);

    assert_no_trace(&event_uri(&follower_id, &follow_path)).await
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v1_file_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (user_kp, user_id) = create_v0_user(&mut test, "Watcher:Epochs:V1File").await?;

    let bytes = format!("test_homeserver_v1_file_skipped {user_id}").into_bytes();
    let (file_id, file_path) = test.create_v1_file(&user_kp, bytes, "text/plain").await?;

    let files = FileDetails::get_by_ids(&[&[user_id.as_str(), file_id.as_str()]]).await?;
    assert!(files[0].is_none(), "The v1 file was indexed");

    assert_no_trace(&event_uri(&user_id, &file_path)).await
}
