//! Reads, retries and lookups that start before a switch and land after nexus
//! has followed the user: what they bring from the old homeserver, or about
//! it, must not be applied on top of the new one.

use anyhow::{ensure, Result};
use nexus_common::models::user::RecheckOutcome;
use pubky::{Keypair, Pubky};

use crate::harness::{post_uri, MigrationTest};

/// A per-user read of A lists its users when it starts and reaches each one
/// in turn, minutes apart on a homeserver with many users. In between, the
/// mover signs up on B and copies, the recheck moves them to B, and the client
/// wipes A once the recheck has confirmed the switch. The read of A still
/// fetches the mover's events when it reaches them and finds the wipe. Like
/// the bulk stream, it checks the user's binding before applying anything,
/// sees the mover is bound to B and leaves the fetched events unread. B's read
/// follows and finds everything already indexed: the posts and the profile
/// stay, and the scout who replied is told nothing.
///
/// Without that check the wipe was applied: the posts were deleted, the
/// profile tombstoned, the scout told the post was deleted and, after B's
/// read restored it, that it was edited.
#[tokio_shared_rt::test(shared)]
async fn a_read_of_the_old_homeserver_started_before_the_switch_leaves_the_wipe_unread(
) -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    let first = t.post(&session_a, "first", None).await?;
    t.post(&session_a, "second", None).await?;
    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    t.index_primary().await?;
    t.ingest(&mover).await?;
    t.index_external(&t.hs_a).await?;
    t.post(&session_scout, "hi", Some(post_uri(&mover, &first)))
        .await?;
    t.index_primary().await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    let scout_notifications = t.notifications(&scout).await?;

    // A read of A starts and lists the mover.
    let read_a = t.index_external_paused(&t.hs_a, &mover).await?;

    // Meanwhile: sign up on B, copy, recheck, and wipe A once it says Switched.
    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_b, &tree).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.delete_tree(&session_a, &tree).await?;

    // The read of A reaches the mover; B's read follows in the same cycle.
    read_a.resume().await?;
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;

    let end = (
        t.bound_name(&mover).await?,
        t.live_posts(&mover).await?,
        t.profile_state(&mover).await?,
        t.notifications(&scout).await?,
    );
    assert_eq!(
        end,
        ("hs_b", 2, (false, "mover".to_string()), scout_notifications),
        "(binding, live posts, (tombstoned, name), scout's notifications)"
    );
    Ok(())
}

/// The scout posts on the primary and nexus has not read it yet. The mover
/// replies from A: the parent is unknown, so the reply waits in the retry
/// queue with A as the homeserver it came from, and A's cursor moves past it.
/// Returns the mover's device and the reply's id.
async fn reply_waiting_for_its_parent(
    t: &MigrationTest,
    mover: &Keypair,
    scout: &Keypair,
) -> Result<(Pubky, String)> {
    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, scout, &t.primary, "scout").await?;
    let topic = t.post(&session_scout, "topic", None).await?;

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, mover, &t.hs_a, "mover").await?;
    let reply = t
        .post(&session_a, "a reply", Some(post_uri(scout, &topic)))
        .await?;
    t.ingest(mover).await?;
    t.index_external(&t.hs_a).await?;
    ensure!(t.live_posts(mover).await? == 0, "waiting for its parent");
    ensure!(t.queued_retries().await? == 1, "queued for a retry");
    Ok((device_a, reply))
}

/// The mover copies to B while the reply still waits for its parent. B's read
/// meets the reply too and queues it again: the queue holds one entry for an
/// address, so it now names B as the homeserver it came from. The parent
/// arrives, the retry runs for a user bound to B and applies the reply once.
/// A second retry and a second read of B add nothing.
#[tokio_shared_rt::test(shared)]
async fn a_retry_queued_again_on_the_new_homeserver_is_applied_there_once() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());
    let (device_a, _) = reply_waiting_for_its_parent(&t, &mover, &scout).await?;

    t.signup_and_copy(&mover, &t.hs_b, "mover", &device_a)
        .await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.queued_retries().await?, 1, "one entry for the address");

    t.index_primary().await?;
    t.retry().await?;
    let applied = (
        t.live_posts(&mover).await?,
        t.notification_times(&scout).await?,
    );
    assert_eq!(
        (applied.0, applied.1.len()),
        (1, 1),
        "(live posts, replies)"
    );

    t.retry().await?;
    t.index_external(&t.hs_b).await?;
    assert_eq!(
        (
            t.live_posts(&mover).await?,
            t.notification_times(&scout).await?
        ),
        applied
    );
    Ok(())
}

/// The mover tries B without copying anything and nexus follows them there.
/// The retry runs while they are away: the mover is bound to another
/// homeserver than the one the entry came from, so it is dropped. Then the
/// mover returns to A, which still holds the reply. A's cursor passed the
/// reply when it was queued, so the read of A resumes after it, and with the
/// entry gone nothing delivers the reply again: it is never indexed.
///
/// An accepted limit of the drop, which assumes the user's new homeserver
/// delivers what it holds. It takes an event still waiting in the retry
/// queue, a move away and a return to the same homeserver.
#[tokio_shared_rt::test(shared)]
async fn a_retry_dropped_while_the_user_was_away_stays_dropped_when_they_return() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());
    let (device_a, _) = reply_waiting_for_its_parent(&t, &mover, &scout).await?;
    t.index_primary().await?;

    // Away: a signup on B, nothing copied. The retry runs meanwhile.
    let device_b = t.device()?;
    t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.retry().await?;

    // Back to A.
    t.switch_record(&device_a, &mover, &t.hs_a).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_a).await?;
    t.retry().await?;

    assert_eq!(
        (
            t.bound_name(&mover).await?,
            t.live_posts(&mover).await?,
            t.notifications(&scout).await?
        ),
        ("hs_a", 0, Vec::new()),
        "(binding, live posts, scout's notifications)"
    );
    Ok(())
}

/// The same limit without a retry run in between. The mover copies to B
/// while the reply waits for its parent, B's read queues it again, and the
/// entry now names B. The mover returns to A before the parent arrives. When
/// the retry runs, the mover is bound to A and the entry came from B, so it
/// is dropped, and A's cursor is past the reply.
#[tokio_shared_rt::test(shared)]
async fn a_retry_queued_again_on_a_homeserver_the_user_left_is_dropped_when_they_return(
) -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());
    let (device_a, _) = reply_waiting_for_its_parent(&t, &mover, &scout).await?;

    t.signup_and_copy(&mover, &t.hs_b, "mover", &device_a)
        .await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;

    t.switch_record(&device_a, &mover, &t.hs_a).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_a).await?;
    t.index_primary().await?;
    t.retry().await?;

    assert_eq!(
        (
            t.bound_name(&mover).await?,
            t.live_posts(&mover).await?,
            t.notifications(&scout).await?
        ),
        ("hs_a", 0, Vec::new()),
        "(binding, live posts, scout's notifications)"
    );
    Ok(())
}

/// The mover replies from A to a post nexus has not indexed yet, so the reply
/// waits in the retry queue with A as the homeserver it came from. The parent
/// is indexed, the mover moves to B with a copy, and the reply is indexed from
/// B. The mover deletes the reply on B and nexus indexes the delete. When the
/// retry runs, the mover is bound to another homeserver than the one the
/// entry came from, so the entry is dropped and nothing is fetched: the reply
/// stays deleted and the scout is told nothing.
///
/// Without the drop the retry fetched the reply from A, which still holds it,
/// and indexed it again: a reply the user deleted was back, and the scout's
/// reply notification was issued again, as the newest. The window was the
/// retry backoff, up to an hour between attempts.
#[tokio_shared_rt::test(shared)]
async fn a_retry_queued_on_the_old_homeserver_is_dropped_once_the_user_left_it() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());
    let (device_a, reply) = reply_waiting_for_its_parent(&t, &mover, &scout).await?;
    t.index_primary().await?;

    // Move to B. The copy brings the reply, whose parent is indexed by now.
    let (_, session_b) = t
        .signup_and_copy(&mover, &t.hs_b, "mover", &device_a)
        .await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 1);

    // The mover deletes the reply on B.
    session_b
        .storage()
        .delete(format!("/pub/pubky.app/posts/{reply}").as_str())
        .await?;
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 0);
    let scout_notifications = t.notifications(&scout).await?;

    // The queued retry runs.
    t.retry().await?;
    assert_eq!(
        (t.live_posts(&mover).await?, t.notifications(&scout).await?),
        (0, scout_notifications),
        "(live posts, scout's notifications)"
    );
    Ok(())
}

/// The recheck runs in the API process and the sweep in the watcher process,
/// each with its own pkarr cache. The watcher's cache still holds the record
/// its last sweep saw, and pkarr's `resolve` serves a cached packet even after
/// it has expired, refreshing it in the background. So the first sweep visit
/// after a recheck, one TTL later, reads the old record unless the entry was
/// evicted, and with stale mapping disabled it moves the user back to A, which
/// their record no longer names. The client wiped A once the recheck had
/// confirmed the switch, so the next read of A applies the wipe: the posts are
/// deleted and the user is tombstoned. Demonstrated, with a second client
/// standing for the watcher's cache; the suite runs in seconds, so that cache
/// holds the old packet unexpired, which is the same answer. With stale
/// mapping enabled, the default, the same visit marks the binding stale
/// instead, and the user is indexed nowhere until the visit after.
///
/// The sweep does not move a binding on a record older than the one the
/// binding was set from: the recheck keeps the record's timestamp on the
/// binding and the sweep ignores anything older (`MappingUpdate::IgnoredOlder`).
#[tokio_shared_rt::test(shared)]
async fn the_watchers_sweep_moves_the_user_back_after_a_recheck_through_the_api() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();
    let watcher = t.device()?;

    let (device_a, session_a, _) = t
        .resident(&mover, &t.hs_a, "mover", &["first", "second"])
        .await?;
    // A sweep visit before the move leaves the record naming A in the
    // watcher's cache.
    t.resolve_cached(&mover, &watcher).await?;
    assert_eq!(t.bound_name(&mover).await?, "hs_a");

    // The move, the app's recheck through the API, and the wipe once the
    // switch is confirmed.
    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    let tree = t.public_tree(&device_a, &mover).await?;
    t.write_tree(&session_b, &tree).await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.live_posts(&mover).await?, 2);
    t.delete_tree(&session_a, &tree).await?;

    // The watcher's next visit, then a read of each homeserver.
    t.resolve_cached(&mover, &watcher).await?;
    t.index_external(&t.hs_a).await?;
    t.index_external(&t.hs_b).await?;

    let end = (
        t.bound_name(&mover).await?,
        t.live_posts(&mover).await?,
        t.profile_state(&mover).await?,
    );
    assert_eq!(
        end,
        ("hs_b", 2, (false, "mover".to_string())),
        "(binding, live posts, (tombstoned, name))"
    );
    Ok(())
}
