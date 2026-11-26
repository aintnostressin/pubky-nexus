//! Switches to and from the primary homeserver, whose stream is bulk-indexed
//! with one global cursor.

use anyhow::Result;
use nexus_common::models::user::RecheckOutcome;
use pubky::Keypair;

use crate::harness::{Layout, MigrationTest};
use crate::layout::in_each_layout;

/// External to primary. The bulk stream skips the user's events while the
/// edge still names the old homeserver, which leaves the user to the primary's
/// per-user read: after the switch it reads their own history on the primary,
/// and everything they write there later. The bulk stream never takes them
/// back.
async fn external_to_primary_is_read_per_user_from_then_on(layout: Layout) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let mover = Keypair::random();

    let (device_a, _, _) = t
        .resident(&mover, &t.hs_a, "mover", &["first", "second"])
        .await?;
    assert_eq!(t.live_posts(&mover).await?, 2);

    // Sign up on the primary and copy. The bulk stream runs before nexus
    // notices the switch and passes over the copy as another homeserver's.
    let (_, session_p) = t
        .signup_and_copy(&mover, &t.primary, "mover", &device_a)
        .await?;
    t.index_primary().await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_a.as_ref()));
    assert_eq!(t.cursor(&mover, &t.primary).await?, 0);

    t.recheck(&mover).await?;
    assert_eq!(
        t.bound_to(&mover).await?.as_deref(),
        Some(t.primary.as_ref())
    );

    // The bulk stream has nothing left for the user; the per-user read does.
    t.index_primary().await?;
    assert_eq!(t.cursor(&mover, &t.primary).await?, 0);
    t.catch_up().await?;
    assert!(t.cursor(&mover, &t.primary).await? > 0);
    assert_eq!(
        t.live_posts(&mover).await?,
        2,
        "the copy is not indexed twice"
    );

    // The bulk stream passes the user over from here on; the per-user read
    // covers them.
    t.post(&session_p, "third, on the primary", None).await?;
    t.index_primary().await?;
    assert_eq!(t.live_posts(&mover).await?, 2, "left to the per-user read");
    t.catch_up().await?;
    assert_eq!(t.live_posts(&mover).await?, 3);
    Ok(())
}

/// Primary to external and back. Once the user has left, the bulk stream
/// passes their events over and leaves them to the primary's per-user read,
/// which starts after the cursor the bulk stream had reached: what it applied
/// before is not read again. The return is read per user from there, and so
/// is everything they write on the primary afterwards.
async fn primary_to_external_and_back(layout: Layout) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let mover = Keypair::random();

    let device_p = t.device()?;
    let session_p = t.signup(&device_p, &mover, &t.primary, "mover").await?;
    t.post(&session_p, "first", None).await?;
    t.post(&session_p, "second", None).await?;
    t.index_primary().await?;
    t.recheck(&mover).await?;
    assert_eq!(
        t.bound_to(&mover).await?.as_deref(),
        Some(t.primary.as_ref())
    );
    assert_eq!(t.live_posts(&mover).await?, 2);

    // Leave for A.
    let (device_a, session_a) = t
        .signup_and_copy(&mover, &t.hs_a, "mover", &device_p)
        .await?;
    let bulk_cursor = t.bulk_cursor().await?;
    assert!(bulk_cursor > 0);
    t.recheck(&mover).await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_a.as_ref()));
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    t.post(&session_a, "third, on A", None).await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.live_posts(&mover).await?, 3);

    // Return: copy the new post to the primary through the old device, whose
    // cache still names the primary, and publish the record.
    let session_p = t.session(&device_p, &mover).await?;
    t.write_tree(&session_p, &t.public_tree(&device_a, &mover).await?)
        .await?;
    t.switch_record(&device_p, &mover, &t.primary).await?;
    t.index_primary().await?;
    assert_eq!(
        t.live_posts(&mover).await?,
        3,
        "skipped as another homeserver's"
    );

    t.recheck(&mover).await?;
    assert_eq!(
        t.bound_to(&mover).await?.as_deref(),
        Some(t.primary.as_ref())
    );
    t.catch_up().await?;
    assert!(t.cursor(&mover, &t.primary).await? > bulk_cursor);
    assert_eq!(t.live_posts(&mover).await?, 3);

    t.post(&session_p, "fourth, home again", None).await?;
    t.index_primary().await?;
    assert_eq!(t.live_posts(&mover).await?, 3, "left to the per-user read");
    t.catch_up().await?;
    assert_eq!(t.live_posts(&mover).await?, 4);
    Ok(())
}

/// The scout replies to a post the mover wrote on the primary before nexus
/// noticed the switch. The bulk stream passes the mover's post over and reads
/// the scout's reply, whose parent it does not know: the reply waits in the
/// retry queue. The per-user read indexes the post after the switch and the
/// retry then applies the reply, once.
async fn a_reply_to_a_post_the_bulk_stream_passed_over_waits_for_the_per_user_read(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    let (device_a, _, _) = t.resident(&mover, &t.hs_a, "mover", &["first"]).await?;

    let (_, session_p) = t
        .signup_and_copy(&mover, &t.primary, "mover", &device_a)
        .await?;
    let only_p = t
        .post(&session_p, "second, only on the primary", None)
        .await?;
    t.post(&session_scout, "hi", Some(t.post_uri(&mover, &only_p)))
        .await?;
    t.index_primary().await?;
    assert_eq!(
        (t.live_posts(&scout).await?, t.queued_retries().await?),
        (0, 1),
        "the reply waits for its parent"
    );

    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.catch_up().await?;
    t.retry().await?;
    let indexed = (
        t.live_posts(&mover).await?,
        t.live_posts(&scout).await?,
        t.notification_times(&mover).await?,
    );
    assert_eq!(
        (indexed.0, indexed.1, indexed.2.len()),
        (2, 1, 1),
        "(the mover's posts, the scout's reply, the mover's notifications)"
    );

    t.retry().await?;
    t.index_primary().await?;
    t.catch_up().await?;
    assert_eq!(
        (
            t.live_posts(&mover).await?,
            t.live_posts(&scout).await?,
            t.notification_times(&mover).await?
        ),
        indexed
    );
    Ok(())
}

/// External to primary, noticed before the bulk stream reads the copy: the
/// mover is bound to the primary by then, so the bulk stream applies the copy
/// itself and the mover is never left to the per-user read. A read of A that
/// had listed the mover before the switch reaches them after A was wiped and
/// leaves the wipe unread. Nothing is counted or notified twice.
async fn a_copy_the_bulk_stream_reads_after_the_switch_changes_nothing(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    let (device_a, session_a, posts) = t
        .resident(&mover, &t.hs_a, "mover", &["first", "second"])
        .await?;
    t.post(&session_scout, "hi", Some(t.post_uri(&mover, &posts[0])))
        .await?;
    t.index_primary().await?;
    let before = (
        t.post_times(&mover).await?,
        t.counts(&mover).await?,
        t.notification_times(&mover).await?,
    );

    let read_a = t.index_external_paused(&t.hs_a, &mover).await?;
    let device_p = t.device()?;
    let session_p = t.signup(&device_p, &mover, &t.primary, "mover").await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_p, &tree).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_primary().await?;
    t.delete_tree(&session_a, &tree).await?;
    read_a.resume().await?;
    t.catch_up().await?;

    assert_eq!(
        (
            t.bound_name(&mover).await?,
            t.cursor(&mover, &t.primary).await?
        ),
        ("primary", 0),
        "read by the bulk stream, not per user"
    );
    assert_eq!(
        (
            t.post_times(&mover).await?,
            t.counts(&mover).await?,
            t.notification_times(&mover).await?
        ),
        before
    );
    Ok(())
}

/// A user nexus has never met, living on A, mirrors to the primary while
/// their record still names A, then switches. The bulk stream passes the
/// mirror over, profile included, so nexus holds no user at all: nothing to
/// bind, nothing for the per-user read to list, nothing for the sweep or a
/// recheck to find. What they write on the primary after the switch has no
/// author to attach to, so none of it is indexed.
///
/// An accepted limit: nexus cannot tell a stranger's mirror from the copy of
/// a user who lives elsewhere, and skipping the second is the point. The
/// user's next profile write on the primary, or anyone who follows, tags or
/// mentions them, introduces them.
#[tokio_shared_rt::test(shared)]
async fn a_stranger_who_mirrors_to_the_primary_stays_unknown_after_the_switch() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    t.post(&session_a, "first", None).await?;

    // Mirror: an account on the primary, the record back on A, then the copy.
    let device_p = t.device()?;
    let session_p = t.signup(&device_p, &mover, &t.primary, "mover").await?;
    t.switch_record(&device_a, &mover, &t.hs_a).await?;
    t.write_tree(&session_p, &t.public_tree(&device_a, &mover).await?)
        .await?;
    t.index_primary().await?;

    t.switch_record(&device_p, &mover, &t.primary).await?;
    t.post(&session_p, "second, on the primary", None).await?;
    t.index_primary().await?;
    t.catch_up().await?;
    t.retry().await?;

    assert_eq!(t.live_posts(&mover).await?, 0);
    Ok(())
}

/// A user signs up on the primary and nexus reads their posts before any
/// sweep has bound them. They move to A with a copy and wipe the primary. The
/// bulk stream decides for a user without a mapping from the record it looks
/// up, cache first, and its cache still names the primary: the wipe is
/// applied. With no mapping and nothing left, the user is removed outright,
/// so a recheck has nobody to move to A. Demonstrated.
///
/// The external twin heals with the switch because the user is bound and
/// only tombstoned. The window here is the time between a signup and the
/// first sweep that binds the user.
#[tokio_shared_rt::test(shared)]
#[ignore = "the wipe of a primary user not bound yet is applied from a cached record after \
            they moved away; needs a fresh lookup before deletes of an unbound user are applied"]
async fn a_wipe_of_the_primary_by_a_user_not_bound_yet_is_left_unread_once_they_moved() -> Result<()>
{
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let device_p = t.device()?;
    let session_p = t.signup(&device_p, &mover, &t.primary, "mover").await?;
    t.post(&session_p, "first", None).await?;
    t.post(&session_p, "second", None).await?;
    t.index_primary().await?;
    assert_eq!(
        (t.bound_name(&mover).await?, t.live_posts(&mover).await?),
        ("nowhere", 2)
    );

    let tree = t.public_tree(&device_p, &mover).await?;
    t.signup_and_copy(&mover, &t.hs_a, "mover", &device_p)
        .await?;
    t.delete_tree(&session_p, &tree).await?;
    t.index_primary().await?;

    assert_eq!(
        t.live_posts(&mover).await?,
        2,
        "the record names A: the wipe is not the primary's to apply"
    );
    Ok(())
}

in_each_layout! {
    external_to_primary_is_read_per_user_from_then_on,
    primary_to_external_and_back,
    a_reply_to_a_post_the_bulk_stream_passed_over_waits_for_the_per_user_read,
    a_copy_the_bulk_stream_reads_after_the_switch_changes_nothing,
}
