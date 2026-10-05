use super::utils::{assert_no_trace, create_v0_user, event_uri};
use crate::event_processor::tags::resource_utils::{
    check_resource_in_sorted_set, compute_resource_id, find_resource_tag, resource_exists_in_graph,
};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::user::UserCounts;
use pubky::ResourcePath;
use pubky_social_specs::legacy_v0::traits::HashId;
use pubky_social_specs::legacy_v0::PubkyAppTag;

/// An epoch-less `social/tags/` write used to land as a universal tag of an app named `social`.
/// The namespace is reserved for the spec now, so it is rejected instead.
#[tokio_shared_rt::test(shared)]
async fn test_homeserver_epoch_less_social_tag_not_indexed() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (tagger_kp, tagger_id) = create_v0_user(&mut test, "Watcher:Epochs:Reserved").await?;

    let target_uri = format!("https://example.com/epoch-less/{tagger_id}");
    let label = "reserved";
    let tag = PubkyAppTag {
        uri: target_uri.clone(),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_path: ResourcePath = format!("/pub/social/tags/{}", tag.create_id()).parse()?;
    test.put(&tagger_kp, &tag_path, &tag).await?;

    let resource_id = compute_resource_id(&target_uri);
    assert!(find_resource_tag(&resource_id, label).await?.is_none());
    assert!(!resource_exists_in_graph(&resource_id).await?);
    let app_timeline =
        check_resource_in_sorted_set(&["Resources", "App", "social", "Timeline"], &resource_id)
            .await?;
    assert!(app_timeline.is_none());
    let counts = UserCounts::get_by_id(&tagger_id)
        .await?
        .expect("v0 user counts");
    assert_eq!(counts.tagged, 0);

    assert_no_trace(&event_uri(&tagger_id, &tag_path)).await
}
