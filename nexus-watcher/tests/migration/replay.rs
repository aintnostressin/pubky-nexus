//! A copy is read again from the new homeserver's first event, so everything
//! the user ever wrote is applied a second time. Nothing nexus derived from it
//! may change: no count, no time, no notification.

use anyhow::{ensure, Result};
use nexus_common::models::user::RecheckOutcome;
use pubky::{Keypair, Pubky};
use serde_json::{json, Value};

use crate::harness::{post_uri, MigrationTest};

/// A mover on A and a scout on the primary who know each other, everything
/// read once: they follow each other, each has replied to and tagged a post of
/// the other's, and the mover has bookmarked the scout's post.
struct Neighbours {
    mover: Keypair,
    scout: Keypair,
    /// The mover's device, which resolves A.
    device_a: Pubky,
}

impl Neighbours {
    async fn settle(t: &MigrationTest) -> Result<Self> {
        let (mover, scout) = (Keypair::random(), Keypair::random());
        let (mover_id, scout_id) = (mover.public_key().to_z32(), scout.public_key().to_z32());

        let device_scout = t.device()?;
        let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
        let topic = t.post(&session_scout, "topic", None).await?;
        t.index_primary().await?;

        let (device_a, session_a, posts) = t
            .resident(&mover, &t.hs_a, "mover", &["first", "second"])
            .await?;
        t.follow(&session_a, &scout_id).await?;
        t.post(&session_a, "re", Some(post_uri(&scout, &topic)))
            .await?;
        t.tag(&session_a, post_uri(&scout, &topic), "nice").await?;
        t.bookmark(&session_a, post_uri(&scout, &topic)).await?;
        t.index_external(&t.hs_a).await?;

        t.follow(&session_scout, &mover_id).await?;
        t.post(&session_scout, "hi", Some(post_uri(&mover, &posts[0])))
            .await?;
        t.tag(&session_scout, post_uri(&mover, &posts[0]), "first")
            .await?;
        t.index_primary().await?;
        ensure!(
            t.notifications(&scout).await?.len() == 3 && t.notifications(&mover).await?.len() == 3,
            "each was told of a follow, a reply and a tag"
        );

        Ok(Self {
            mover,
            scout,
            device_a,
        })
    }

    /// Everything nexus derived from the two that a replay could count or
    /// notify twice, or date again.
    async fn derived(&self, t: &MigrationTest) -> Result<Value> {
        Ok(json!({
            "mover": {
                "counts": t.counts(&self.mover).await?,
                "posts": t.post_times(&self.mover).await?,
                "tags": t.edge_times(&self.mover, "TAGGED").await?,
                "notifications": t.notification_times(&self.mover).await?,
            },
            "scout": {
                "counts": t.counts(&self.scout).await?,
                "posts": t.post_times(&self.scout).await?,
                "tags": t.edge_times(&self.scout, "TAGGED").await?,
                "notifications": t.notification_times(&self.scout).await?,
            },
        }))
    }
}

/// External to external. B is read twice: the copy, then nothing new.
#[tokio_shared_rt::test(shared)]
async fn a_copy_read_on_the_new_homeserver_counts_and_notifies_nothing_twice() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let pair = Neighbours::settle(&t).await?;
    let before = pair.derived(&t).await?;

    t.signup_and_copy(&pair.mover, &t.hs_b, "mover", &pair.device_a)
        .await?;
    assert_eq!(
        t.recheck(&pair.mover).await?.outcome,
        RecheckOutcome::Switched
    );
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;

    assert!(
        t.cursor(&pair.mover, &t.hs_b).await? > 0,
        "the copy was read"
    );
    assert_eq!(pair.derived(&t).await?, before);
    Ok(())
}

/// External to the primary. The bulk stream passes the copy over, and the
/// per-user read of the primary reads it after the switch.
#[tokio_shared_rt::test(shared)]
async fn a_copy_read_per_user_on_the_primary_counts_and_notifies_nothing_twice() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let pair = Neighbours::settle(&t).await?;
    let before = pair.derived(&t).await?;

    t.signup_and_copy(&pair.mover, &t.primary, "mover", &pair.device_a)
        .await?;
    t.index_primary().await?;
    assert_eq!(
        t.recheck(&pair.mover).await?.outcome,
        RecheckOutcome::Switched
    );
    t.index_primary().await?;
    t.catch_up().await?;
    t.catch_up().await?;

    assert!(
        t.cursor(&pair.mover, &t.primary).await? > 0,
        "the copy was read"
    );
    assert_eq!(pair.derived(&t).await?, before);
    Ok(())
}

/// The copied follow is the follow nexus already holds, but its time becomes
/// the time of the read: `create_follow` sets `indexed_at` on every PUT, not
/// only when the relationship is new. The influencer rankings count followers
/// by that time, so after a move everyone the mover follows gains a follower
/// "today". A tag keeps its time, which is set only when it is created.
/// Demonstrated.
#[tokio_shared_rt::test(shared)]
#[ignore = "a replayed follow is dated again; needs the follow's time set only when it is created"]
async fn a_copied_follow_keeps_its_time() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let pair = Neighbours::settle(&t).await?;
    let before = t.edge_times(&pair.mover, "FOLLOWS").await?;
    assert_eq!(before.len(), 1);

    t.signup_and_copy(&pair.mover, &t.hs_b, "mover", &pair.device_a)
        .await?;
    t.recheck(&pair.mover).await?;
    t.index_external(&t.hs_b).await?;

    assert_eq!(t.edge_times(&pair.mover, "FOLLOWS").await?, before);
    Ok(())
}

/// The copied bookmark is the bookmark nexus already holds, but its time
/// becomes the time of the read: the handler stamps every PUT and writes the
/// stream's score again. A copy writes bookmarks in path order, which is the
/// order of their hashes, so after a move the user's bookmark list is dated
/// today and shuffled. Demonstrated.
#[tokio_shared_rt::test(shared)]
#[ignore = "a replayed bookmark is dated again; needs the bookmark's time kept when it exists"]
async fn a_copied_bookmark_keeps_its_time() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let pair = Neighbours::settle(&t).await?;
    let before = t.edge_times(&pair.mover, "BOOKMARKED").await?;
    assert_eq!(before.len(), 1);

    t.signup_and_copy(&pair.mover, &t.hs_b, "mover", &pair.device_a)
        .await?;
    t.recheck(&pair.mover).await?;
    t.index_external(&t.hs_b).await?;

    assert_eq!(t.edge_times(&pair.mover, "BOOKMARKED").await?, before);
    Ok(())
}
