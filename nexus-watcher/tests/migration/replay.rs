//! A copy is read again from the new homeserver's first event, so everything
//! the user ever wrote is applied a second time. Nothing nexus derived from it
//! may change: no count, no time, no notification.

use anyhow::{ensure, Result};
use nexus_common::models::user::RecheckOutcome;
use pubky::{Keypair, Pubky};
use serde_json::{json, Value};

use crate::harness::{Layout, MigrationTest};
use crate::layout::in_each_layout;

/// A mover on A and a scout on the primary who know each other, everything
/// read once: they follow each other, each has replied to and tagged a post of
/// the other's, and in `pubky.app` the mover has bookmarked the scout's post.
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
        t.post(&session_a, "re", Some(t.post_uri(&scout, &topic)))
            .await?;
        t.tag(&session_a, t.post_uri(&scout, &topic), "nice")
            .await?;
        // A `social/v1` bookmark is private: nexus never sees one.
        if t.layout == Layout::PubkyApp {
            t.bookmark(&session_a, t.post_uri(&scout, &topic)).await?;
        }
        t.index_external(&t.hs_a).await?;

        t.follow(&session_scout, &mover_id).await?;
        t.post(&session_scout, "hi", Some(t.post_uri(&mover, &posts[0])))
            .await?;
        t.tag(&session_scout, t.post_uri(&mover, &posts[0]), "first")
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
async fn a_copy_read_on_the_new_homeserver_counts_and_notifies_nothing_twice(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
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
async fn a_copy_read_per_user_on_the_primary_counts_and_notifies_nothing_twice(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
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

/// The scout replied to a post of the mover's, and the mover then edited the
/// post: nexus read the edit on A and told the scout once. The mover copies to
/// B and nexus reads the copy. The post keeps its content and its place, and
/// the scout is told nothing more.
///
/// In `pubky.app` the copy holds the post as it is now, which is what nexus
/// holds. In `social/v1` an edit is a new version next to the first, the copy
/// holds both, and nexus applies every version it reads as an edit. The post
/// ends as it was, but the scout's notification of the edit is issued again,
/// as the newest (demonstrated). Reasoned from the handler: the first version
/// puts the old content back and tells the scout, the second restores the
/// content and tells them once more, and a read that ends between the two
/// serves the content the mover had replaced until the next one.
async fn a_copied_post_that_was_edited_tells_its_repliers_nothing_again(
    layout: Layout,
) -> Result<()> {
    let t = MigrationTest::setup_in(layout).await?;
    let (mover, scout) = (Keypair::random(), Keypair::random());

    let device_scout = t.device()?;
    let session_scout = t.signup(&device_scout, &scout, &t.primary, "scout").await?;
    let (device_a, session_a, posts) = t.resident(&mover, &t.hs_a, "mover", &["first"]).await?;
    t.post(&session_scout, "hi", Some(t.post_uri(&mover, &posts[0])))
        .await?;
    t.index_primary().await?;
    t.edit_post(&session_a, &posts[0], "first, edited").await?;
    t.index_external(&t.hs_a).await?;
    let before = (
        t.post_content(&mover, &posts[0]).await?,
        t.post_times(&mover).await?,
        t.notification_times(&scout).await?,
    );
    ensure!(
        before.0 == "first, edited" && before.2.len() == 1,
        "the edit was read and the scout told of it"
    );

    t.signup_and_copy(&mover, &t.hs_b, "mover", &device_a)
        .await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;

    assert_eq!(
        (
            t.post_content(&mover, &posts[0]).await?,
            t.post_times(&mover).await?,
            t.notification_times(&scout).await?,
        ),
        before,
        "(content, (id, deleted, time) of each post, the scout's notifications)"
    );
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
    let (before, after) = times_around_a_copy(Layout::PubkyApp, "FOLLOWS").await?;
    assert_eq!(after, before);
    Ok(())
}

/// The same follow written in `social/v1`: it is dated again as well, by the
/// same handler. Demonstrated.
#[tokio_shared_rt::test(shared)]
#[ignore = "a replayed follow is dated again; needs the follow's time set only when it is created"]
async fn a_copied_social_v1_follow_keeps_its_time() -> Result<()> {
    let (before, after) = times_around_a_copy(Layout::SocialV1, "FOLLOWS").await?;
    assert_eq!(after, before);
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
    let (before, after) = times_around_a_copy(Layout::PubkyApp, "BOOKMARKED").await?;
    assert_eq!(after, before);
    Ok(())
}

/// When nexus indexed the mover's one relationship of `kind`, before their
/// copy is read on B and after.
async fn times_around_a_copy(
    layout: Layout,
    kind: &str,
) -> Result<(Vec<(String, i64)>, Vec<(String, i64)>)> {
    let t = MigrationTest::setup_in(layout).await?;
    let pair = Neighbours::settle(&t).await?;
    let before = t.edge_times(&pair.mover, kind).await?;
    ensure!(before.len() == 1, "the mover has one such relationship");

    t.signup_and_copy(&pair.mover, &t.hs_b, "mover", &pair.device_a)
        .await?;
    t.recheck(&pair.mover).await?;
    t.index_external(&t.hs_b).await?;

    Ok((before, t.edge_times(&pair.mover, kind).await?))
}

in_each_layout! {
    a_copy_read_on_the_new_homeserver_counts_and_notifies_nothing_twice,
    a_copy_read_per_user_on_the_primary_counts_and_notifies_nothing_twice,
    #[ignore_in_social_v1 = "the older versions of a copied post are applied as edits and its \
                             repliers told again; needs only the newest version of a post \
                             applied (ADR 0003, E1)"]
    a_copied_post_that_was_edited_tells_its_repliers_nothing_again,
}
