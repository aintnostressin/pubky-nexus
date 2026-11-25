//! The primary homeserver is read in bulk, with one cursor for every user, so an event its
//! bulk read passes over is never delivered again. A user it passed over is read per user
//! there instead, from their own cursor.

use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::post::PostDetails;
use nexus_common::models::user::{set_user_homeserver, set_user_homeserver_stale, UserHsCursor};
use pubky::Keypair;
use pubky_social_specs::legacy_v0::{PubkyAppPost, PubkyAppPostKind, PubkyAppUser};

fn profile(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}

fn post(content: &str) -> PubkyAppPost {
    PubkyAppPost {
        content: content.to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    }
}

/// A post written while its author's mapping to the primary was stale is passed over by the
/// bulk read. Once the mapping realigns, the per-user read of the primary indexes it, with
/// whatever the author wrote since. A user the bulk read never passed over stays on it.
#[tokio_shared_rt::test(shared)]
async fn test_a_post_the_bulk_read_passed_over_is_indexed_by_the_per_user_read() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let primary = test.homeserver_id.clone();
    let author_kp = Keypair::random();
    let author = test.create_user(&author_kp, &profile("author")).await?;
    set_user_homeserver(&author, &primary).await?;
    let bystander_kp = Keypair::random();
    let bystander = test
        .create_user(&bystander_kp, &profile("bystander"))
        .await?;
    set_user_homeserver(&bystander, &primary).await?;

    // The author's record names another homeserver for a while, as the resolver records it.
    set_user_homeserver_stale(&author, true, None).await?;
    let (passed_over, passed_over_path) =
        test.create_post(&author_kp, &post("while stale")).await?;
    assert!(
        PostDetails::get_by_id(&author, &passed_over)
            .await?
            .is_none(),
        "the bulk read applies no event of a stale mapping"
    );
    set_user_homeserver_stale(&author, false, None).await?;
    let (later, later_path) = test.create_post(&author_kp, &post("realigned")).await?;

    test.process_per_user_read_of_primary().await?;

    for post_id in [&passed_over, &later] {
        assert!(
            PostDetails::get_by_id(&author, post_id).await?.is_some(),
            "post {post_id} is not indexed"
        );
    }
    assert_eq!(
        UserHsCursor::read(&[bystander.as_str()], &primary).await?,
        vec![0],
        "a user the bulk read never passed over is not read per user"
    );

    // The author's deletes reach the index through the per-user read as well.
    test.cleanup_post(&author_kp, &later_path).await?;
    test.cleanup_post(&author_kp, &passed_over_path).await?;
    test.cleanup_user(&author_kp).await?;
    test.process_per_user_read_of_primary().await?;
    test.cleanup_user(&bystander_kp).await?;
    Ok(())
}
