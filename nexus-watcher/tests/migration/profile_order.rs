//! Copies that write a user's objects ahead of their profile. Nexus reads a
//! user through `social/v1` only once it has indexed their
//! `social/v1/profile.json`, and until then ignores what they write there, so
//! the order of a copy decides what the read of the new homeserver keeps.

use anyhow::Result;
use nexus_common::models::user::RecheckOutcome;
use pubky::Keypair;

use crate::harness::{Layout, MigrationTest};
use crate::layout::in_each_layout;

/// Nexus has read the mover on A, profile included, and then they post once
/// more. They move to B with a copy in path order, which writes their posts
/// ahead of the profile. Nexus already reads them through the layout they
/// write, so the read of B indexes the post it had not seen yet, wherever the
/// profile comes in the copy.
async fn a_copy_that_writes_the_profile_after_the_posts_is_indexed_whole(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let mover = Keypair::random();

    let (device_a, session_a, _) = t
        .resident(&mover, &t.hs_a, "mover", &["first", "second"])
        .await?;
    t.post(&session_a, "third, not read on A", None).await?;

    let device_b = t.device()?;
    let session_b = t.open_account(&device_b, &mover, &t.hs_b).await?;
    let tree = MigrationTest::in_path_order(t.public_tree(&device_a, &mover).await?);
    t.write_tree(&session_b, &tree).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;

    assert_eq!(
        (
            t.bound_name(&mover).await?,
            t.live_posts(&mover).await?,
            t.profile_state(&mover).await?
        ),
        ("hs_b", 3, (false, "mover".to_string())),
        "(binding, live posts, (tombstoned, name))"
    );
    Ok(())
}

/// Nexus knows the mover only as a stub bound to A: someone it indexes named
/// them, and the read of A has not come round yet. The mover moves to B with
/// a copy in path order: an upload, a follow and two posts, then the profile.
/// The recheck follows them and B is read from its first event.
///
/// In `pubky.app` every object is indexed as it comes. In `social/v1` the
/// upload, the follow and the posts reach nexus while it holds no
/// `social/v1` profile for the mover, so the gate ignores them and B's cursor
/// moves past them; the profile then flips the mover to `social/v1`, and
/// nothing reads the earlier events again. The mover is indexed with a
/// profile and nothing else, though B holds everything. Demonstrated.
///
/// The same holds without a move for any user whose first `social/v1` profile
/// nexus reads after their other objects (reasoned): the ADR accepts it for a
/// new user, who writes the profile first. A copy does not have to.
async fn objects_copied_ahead_of_the_profile_of_a_user_nexus_has_not_read_are_indexed(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_scout = t.device()?;
    t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    t.index_primary().await?;

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    t.upload(&session_a, &mover, b"pixels").await?;
    t.follow(&session_a, &scout.public_key().to_z32()).await?;
    t.post(&session_a, "first", None).await?;
    t.post(&session_a, "second", None).await?;
    t.ingest(&mover).await?;

    let device_b = t.device()?;
    let session_b = t.open_account(&device_b, &mover, &t.hs_b).await?;
    let tree = MigrationTest::in_path_order(t.public_tree(&device_a, &mover).await?);
    t.write_tree(&session_b, &tree).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;
    t.retry().await?;

    assert_eq!(
        (
            t.profile_state(&mover).await?,
            t.live_posts(&mover).await?,
            t.edge_times(&mover, "FOLLOWS").await?.len(),
            t.files(&mover).await?
        ),
        (
            (false, "mover".to_string()),
            2,
            1,
            layout.indexed_uploads(1)
        ),
        "((tombstoned, name), live posts, follows, (file records, stored files))"
    );
    Ok(())
}

in_each_layout! {
    a_copy_that_writes_the_profile_after_the_posts_is_indexed_whole,
    #[ignore_in_social_v1 = "social/v1 objects read ahead of a user's first social/v1 profile \
                             are ignored for good; needs them read again once that profile is \
                             indexed"]
    objects_copied_ahead_of_the_profile_of_a_user_nexus_has_not_read_are_indexed,
}
