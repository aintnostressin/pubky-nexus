//! Writes to the sorted sets every viewer shares go through the trust gate: an
//! unranked author's posts stay out of the global timeline and engagement sets,
//! the tag sets and other people's threads. Their own threads and the sets
//! scoped to them are untouched.
//!
//! `WatcherTest::create_user` ranks the users it creates; each test here
//! unranks the authors it needs hidden. No other watcher test rebuilds the
//! ranking, so the change holds for the test's duration.
use super::utils::{
    check_member_global_timeline_user_post, check_member_post_replies,
    check_member_total_engagement_user_posts, check_member_user_post_timeline,
    check_member_user_replies_timeline, short_post, test_user,
};
use crate::event_processor::tags::utils::{
    check_member_post_tag_global_timeline, check_member_total_engagement_post_tag,
};
use crate::event_processor::utils::watcher::{unrank_user, HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use pubky::Keypair;
use pubky_app_specs::{post_uri_builder, PubkyAppPost, PubkyAppTag};

const BIO: &str = "trust gate";

#[tokio_shared_rt::test(shared)]
async fn test_unranked_posts_stay_out_of_shared_sets() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let label = "watchergatelabel";

    let ranked_kp = Keypair::random();
    let ranked_id = test
        .create_user(&ranked_kp, &test_user("Watcher:Gate:Ranked", BIO))
        .await?;
    let unranked_kp = Keypair::random();
    let unranked_id = test
        .create_user(&unranked_kp, &test_user("Watcher:Gate:Unranked", BIO))
        .await?;
    unrank_user(&unranked_id).await?;
    let fan_kp = Keypair::random();
    test.create_user(&fan_kp, &test_user("Watcher:Gate:Fan", BIO))
        .await?;

    let (ranked_post, _) = test
        .create_post(&ranked_kp, &short_post("Watcher:Gate:RankedPost"))
        .await?;
    let (unranked_post, _) = test
        .create_post(&unranked_kp, &short_post("Watcher:Gate:UnrankedPost"))
        .await?;

    // A tag and a reply on each post: the engagement writes must not create a
    // hidden post either.
    for (author_id, post_id) in [(&ranked_id, &ranked_post), (&unranked_id, &unranked_post)] {
        let uri = post_uri_builder(author_id.clone(), post_id.clone());
        let tag = PubkyAppTag {
            uri: uri.clone(),
            label: label.to_string(),
            created_at: Utc::now().timestamp_millis(),
        };
        test.put(&fan_kp, &tag.hs_path(), tag).await?;
        let reply = PubkyAppPost {
            parent: Some(uri),
            ..short_post("Watcher:Gate:Reply")
        };
        test.create_post(&fan_kp, &reply).await?;
    }

    let ranked_key: &[&str] = &[&ranked_id, &ranked_post];
    assert!(
        check_member_global_timeline_user_post(&ranked_id, &ranked_post)
            .await?
            .is_some()
    );
    assert_eq!(
        check_member_total_engagement_user_posts(ranked_key).await?,
        Some(2),
        "one tag and one reply"
    );
    assert!(check_member_post_tag_global_timeline(ranked_key, label)
        .await?
        .is_some());
    assert_eq!(
        check_member_total_engagement_post_tag(ranked_key, label).await?,
        Some(1)
    );

    let unranked_key: &[&str] = &[&unranked_id, &unranked_post];
    assert!(
        check_member_global_timeline_user_post(&unranked_id, &unranked_post)
            .await?
            .is_none(),
        "global timeline"
    );
    assert!(
        check_member_total_engagement_user_posts(unranked_key)
            .await?
            .is_none(),
        "global engagement"
    );
    assert!(
        check_member_post_tag_global_timeline(unranked_key, label)
            .await?
            .is_none(),
        "tag timeline"
    );
    assert!(
        check_member_total_engagement_post_tag(unranked_key, label)
            .await?
            .is_none(),
        "tag engagement"
    );
    // Their own stream still has it.
    assert!(
        check_member_user_post_timeline(&unranked_id, &unranked_post)
            .await?
            .is_some()
    );
    Ok(())
}

/// A thread takes ranked repliers and the post's own author, ranked or not.
#[tokio_shared_rt::test(shared)]
async fn test_threads_keep_the_authors_own_replies() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let owner_kp = Keypair::random();
    let owner_id = test
        .create_user(&owner_kp, &test_user("Watcher:Gate:Owner", BIO))
        .await?;
    unrank_user(&owner_id).await?;
    let ranked_kp = Keypair::random();
    let ranked_id = test
        .create_user(&ranked_kp, &test_user("Watcher:Gate:RankedReplier", BIO))
        .await?;
    let stranger_kp = Keypair::random();
    let stranger_id = test
        .create_user(&stranger_kp, &test_user("Watcher:Gate:Stranger", BIO))
        .await?;
    unrank_user(&stranger_id).await?;

    let (post_id, _) = test
        .create_post(&owner_kp, &short_post("Watcher:Gate:Thread"))
        .await?;
    let reply = PubkyAppPost {
        parent: Some(post_uri_builder(owner_id.clone(), post_id.clone())),
        ..short_post("Watcher:Gate:ThreadReply")
    };
    let mut replies = Vec::new();
    for kp in [&owner_kp, &ranked_kp, &stranger_kp] {
        let (reply_id, _) = test.create_post(kp, &reply).await?;
        replies.push(reply_id);
    }

    let expected = [
        (&owner_id, &replies[0], true),
        (&ranked_id, &replies[1], true),
        (&stranger_id, &replies[2], false),
    ];
    for (author_id, reply_id, kept) in expected {
        let score = check_member_post_replies(&owner_id, &post_id, &[author_id, reply_id]).await?;
        assert_eq!(score.is_some(), kept, "{author_id}'s reply");
    }
    // The stranger's own replies stream still has it.
    assert!(
        check_member_user_replies_timeline(&stranger_id, &replies[2])
            .await?
            .is_some()
    );
    Ok(())
}
