use super::utils::{assert_no_trace, create_v0_user, event_uri};
use crate::event_processor::utils::watcher::{generate_post_id, WatcherTest};
use anyhow::Result;
use nexus_common::models::post::PostDetails;
use nexus_common::models::user::UserCounts;
use pubky::ResourcePath;
use pubky_social_specs::{PubkySocialPost, PubkySocialPostKind};

/// A `social` epoch this build does not speak is skipped like a v1 object, with a warning.
#[tokio_shared_rt::test(shared)]
async fn test_homeserver_v2_post_skipped() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (user_kp, user_id) = create_v0_user(&mut test, "Watcher:Epochs:V2Post").await?;

    let post_id = generate_post_id();
    let post_path: ResourcePath = format!("/pub/social/v2/posts/{post_id}").parse()?;
    let post = PubkySocialPost::new(
        "test_homeserver_v2_post_skipped".to_string(),
        PubkySocialPostKind::Note,
        None,
        None,
        vec![],
    );
    test.put(&user_kp, &post_path, &post).await?;

    assert!(PostDetails::get_by_id(&user_id, &post_id).await?.is_none());
    let counts = UserCounts::get_by_id(&user_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(counts.posts, 0);

    assert_no_trace(&event_uri(&user_id, &post_path)).await
}
