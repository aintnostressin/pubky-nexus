//! Switches to and from the primary homeserver, whose stream is bulk-indexed
//! with one global cursor.

use anyhow::Result;
use pubky::Keypair;

use crate::harness::MigrationTest;

/// External to primary. The bulk stream skips the user's events while the
/// edge still names the old homeserver, which leaves the user to the primary's
/// per-user read: after the switch it reads their own history on the primary,
/// and everything they write there later. The bulk stream never takes them
/// back.
#[tokio_shared_rt::test(shared)]
async fn external_to_primary_is_read_per_user_from_then_on() -> Result<()> {
    let t = MigrationTest::setup().await?;
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
#[tokio_shared_rt::test(shared)]
async fn primary_to_external_and_back() -> Result<()> {
    let t = MigrationTest::setup().await?;
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
