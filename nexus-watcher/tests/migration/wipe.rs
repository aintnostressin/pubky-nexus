//! Migrations that remove the data from the old homeserver.

use anyhow::Result;
use pubky::Keypair;

use crate::harness::{post_uri, MigrationTest};

/// Mirror first, switch later, wipe last: the record keeps naming A while the
/// data is copied to B, so nexus keeps reading A; once the record names B, a
/// wipe of A is not read at all.
#[tokio_shared_rt::test(shared)]
async fn mirror_then_switch_then_wipe_the_old_homeserver() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let (device_a, session_a, _) = t
        .resident(&mover, &t.hs_a, "mover", &["first", "second"])
        .await?;
    t.recheck(&mover).await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_a.as_ref()));

    // Mirror: the signup on B publishes B, the old device publishes A again
    // right away. B's device keeps writing to B from its own cache.
    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    t.switch_record(&device_a, &mover, &t.hs_a.to_public_key())
        .await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_b, &tree).await?;
    t.recheck(&mover).await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_a.as_ref()));
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    assert_eq!(t.cursor(&mover, &t.hs_b).await?, 0, "B is not read");

    // Switch.
    t.switch_record(&device_b, &mover, &t.hs_b.to_public_key())
        .await?;
    t.recheck(&mover).await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_b.as_ref()));
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 2);

    // Wipe A. Nothing reads the user there any more.
    t.delete_tree(&session_a, &tree).await?;
    let cursor_a = t.cursor(&mover, &t.hs_a).await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.cursor(&mover, &t.hs_a).await?, cursor_a);
    assert_eq!(t.live_posts(&mover).await?, 2);
    assert_eq!(t.profile_state(&mover).await?, (false, "mover".to_string()));
    Ok(())
}

/// Switch, then wipe A before nexus has noticed: the deletes are indexed and
/// the user tombstoned, the switch is still followed, and the read of B
/// brings everything back — a tombstoned user stays on their homeserver's
/// read list, so the copy on B heals them. Counterparts see a delete and a
/// re-create.
#[tokio_shared_rt::test(shared)]
async fn wipe_before_nexus_notices_is_healed_by_the_switch() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    let first = t.post(&session_a, "first", None).await?;
    t.post(&session_a, "second", None).await?;
    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    t.index_primary().await?;
    t.follow(&session_a, &scout.public_key().to_z32()).await?;
    t.ingest(&mover).await?;
    t.index_external(&t.hs_a).await?;
    t.follow(&session_scout, &mover.public_key().to_z32())
        .await?;
    t.post(&session_scout, "hi", Some(post_uri(&mover, &first)))
        .await?;
    t.index_primary().await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    let scout_notifications = t.notifications(&scout).await?;

    // Sign up on B, copy, wipe A, all before nexus looks again.
    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_b, &tree).await?;
    t.delete_tree(&session_a, &tree).await?;

    // Nexus reads A first: the wipe looks like a deletion.
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.live_posts(&mover).await?, 0);
    assert!(t.profile_state(&mover).await?.0, "tombstoned");

    // The tombstoned user still follows the record, and the read of B
    // restores the profile and the posts.
    t.recheck(&mover).await?;
    assert_eq!(t.bound_to(&mover).await?.as_deref(), Some(t.hs_b.as_ref()));
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    assert_eq!(t.profile_state(&mover).await?, (false, "mover".to_string()));

    // The known cost: the counterpart was told about the deletion.
    assert!(t.notifications(&scout).await?.len() >= scout_notifications.len());

    t.post(&session_b, "third, from B", None).await?;
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 3);
    Ok(())
}

/// The mover leaves A for B and, once nexus has followed, wipes A. Later they
/// come back: the record names A again and B's tree is copied back to A.
/// Nexus reads A from the cursor where it left off, so it reads the wipe
/// before the copy. A read can end between the wipe's profile delete and the
/// copy's profile write, leaving the user tombstoned at the end of that read;
/// a tombstoned user stays on the read list, so the next read indexes the
/// copy and the profile write clears the tombstone. The 99 posts make the
/// wipe fill one read of 100 events so the read ends exactly on the profile
/// delete — the narrowest window there is, since the copy writes the profile
/// first and the wipe deletes it last; a copy in listing order writes the
/// profile after the posts, and then any read that ends inside the copy
/// tombstones the same way (reasoned).
///
/// Unlike the wipe before nexus notices, the tombstone comes from a read made
/// after the switch, so a one-off read of a deleted user after a switch would
/// not have covered it: a tombstoned user has to keep being read on the
/// homeserver they are bound to, since a profile write is the one thing that
/// clears the tombstone.
#[tokio_shared_rt::test(shared)]
async fn returning_to_a_homeserver_wiped_after_leaving_it_replays_the_wipe() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    for i in 0..99 {
        t.post(&session_a, &format!("post {i}"), None).await?;
    }
    t.ingest(&mover).await?;
    t.index_external(&t.hs_a).await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.live_posts(&mover).await?, 99);

    // Leave for B, then wipe A: 99 post deletes and the profile delete.
    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_b, &tree).await?;
    t.recheck(&mover).await?;
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.bound_name(&mover).await?, "hs_b");
    t.delete_tree(&session_a, &tree).await?;

    // Back to A with B's tree.
    t.switch_record(&device_a, &mover, &t.hs_a.to_public_key())
        .await?;
    t.write_tree(&session_a, &t.public_tree(&device_b, &mover).await?)
        .await?;
    t.recheck(&mover).await?;
    for _ in 0..3 {
        t.index_external(&t.hs_a).await?;
    }

    let end = (
        t.bound_name(&mover).await?,
        t.live_posts(&mover).await?,
        t.profile_state(&mover).await?,
    );
    assert_eq!(
        end,
        ("hs_a", 99, (false, "mover".to_string())),
        "(binding, live posts, (tombstoned, name))"
    );
    Ok(())
}

/// No migration at all: a user of an external homeserver deletes their
/// profile and writes it again after nexus has read the delete. The delete
/// tombstones them, the tombstoned user stays on the per-user read list
/// (`get_active_users_by_homeserver` keeps deleted users), so the new profile
/// is read and clears the tombstone. This is what the two scenarios above
/// come down to. The sweep keeps deleted users too, so it still re-resolves
/// their record: the default sweep marks a changed mapping stale, and a
/// recheck, or a sweep with stale mapping disabled, follows it (reasoned).
/// The tombstone still hides the
/// user from readers: every feed and search query filters on `u.deleted`;
/// only the user's own record stops the watcher reading them.
#[tokio_shared_rt::test(shared)]
async fn a_profile_deleted_then_written_again_is_read_again() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let user = Keypair::random();

    let device = t.device()?;
    let session = t.signup(&device, &user, &t.hs_a, "user").await?;
    t.post(&session, "first", None).await?;
    t.ingest(&user).await?;
    t.index_external(&t.hs_a).await?;

    t.delete_profile(&session).await?;
    t.index_external(&t.hs_a).await?;
    assert!(t.profile_state(&user).await?.0, "tombstoned");

    t.write_profile(&session, "user, again").await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(
        t.profile_state(&user).await?,
        (false, "user, again".to_string())
    );
    Ok(())
}
