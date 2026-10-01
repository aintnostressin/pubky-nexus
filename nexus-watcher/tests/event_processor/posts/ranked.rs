//! The ranked timeline sets (`source=all` for viewers inside the trust
//! ranking) mirror the global and per-label timelines as posts and tags are
//! written: only authors in the ranking get in, and deletes come out of both.
//!
//! Each test ranks its own fresh author by adding it to the shared ranking
//! (`Sorted:Users:SocialGraph`) and removes it at the end. No other watcher
//! test rebuilds the ranking, so the member survives for the test's duration.
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::RedisOps;
use nexus_common::models::post::search::TAG_GLOBAL_POST_TIMELINE;
use nexus_common::models::post::{
    PostStream, POST_RANKED_TIMELINE_KEY_PARTS, POST_TIMELINE_KEY_PARTS, TAG_RANKED_POST_TIMELINE,
};
use nexus_common::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;
use pubky::Keypair;
use pubky_app_specs::{
    post_uri_builder, PubkyAppPost, PubkyAppPostKind, PubkyAppTag, PubkyAppUser,
};

async fn rank(user_id: &str) -> Result<()> {
    // Far past any fixture rank, so the fixture's positions are untouched.
    PostStream::put_index_sorted_set(&USER_SOCIAL_GRAPH_KEY_PARTS, &[(1e9, user_id)], None, None)
        .await?;
    Ok(())
}

async fn unrank(user_id: &str) -> Result<()> {
    PostStream::remove_from_index_sorted_set(None, &USER_SOCIAL_GRAPH_KEY_PARTS, &[user_id])
        .await?;
    Ok(())
}

async fn score_in(key_parts: &[&str], author_id: &str, post_id: &str) -> Result<Option<isize>> {
    Ok(PostStream::check_sorted_set_member(None, key_parts, &[author_id, post_id]).await?)
}

fn user(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: Some("ranked timeline mirror".to_string()),
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}

fn root_post(content: &str) -> PubkyAppPost {
    PubkyAppPost {
        content: content.to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    }
}

#[tokio_shared_rt::test(shared)]
async fn test_ranked_timeline_mirrors_root_posts() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let ranked_kp = Keypair::random();
    let ranked_id = test
        .create_user(&ranked_kp, &user("Watcher:Ranked:Author"))
        .await?;
    let unranked_kp = Keypair::random();
    let unranked_id = test
        .create_user(&unranked_kp, &user("Watcher:Unranked:Author"))
        .await?;
    rank(&ranked_id).await?;

    let (ranked_post, ranked_path) = test
        .create_post(&ranked_kp, &root_post("Watcher:Ranked:Post"))
        .await?;
    let (unranked_post, _) = test
        .create_post(&unranked_kp, &root_post("Watcher:Unranked:Post"))
        .await?;

    let global = score_in(&POST_TIMELINE_KEY_PARTS, &ranked_id, &ranked_post).await?;
    let ranked = score_in(&POST_RANKED_TIMELINE_KEY_PARTS, &ranked_id, &ranked_post).await?;
    assert!(global.is_some(), "the post is in the global timeline");
    assert_eq!(ranked, global, "mirrored at the global score");

    assert!(
        score_in(&POST_TIMELINE_KEY_PARTS, &unranked_id, &unranked_post)
            .await?
            .is_some()
    );
    assert!(
        score_in(
            &POST_RANKED_TIMELINE_KEY_PARTS,
            &unranked_id,
            &unranked_post
        )
        .await?
        .is_none(),
        "an unranked author's post stays out of the ranked timeline"
    );

    test.cleanup_post(&ranked_kp, &ranked_path).await?;
    assert!(
        score_in(&POST_RANKED_TIMELINE_KEY_PARTS, &ranked_id, &ranked_post)
            .await?
            .is_none(),
        "a deleted post leaves the ranked timeline"
    );

    unrank(&ranked_id).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_ranked_tag_timeline_mirrors_tags() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let label = "watcherrankedtag";
    let source_set = [&TAG_GLOBAL_POST_TIMELINE[..], &[label]].concat();
    let ranked_set = [&TAG_RANKED_POST_TIMELINE[..], &[label]].concat();

    let ranked_kp = Keypair::random();
    let ranked_id = test
        .create_user(&ranked_kp, &user("Watcher:RankedTag:Author"))
        .await?;
    let unranked_kp = Keypair::random();
    let unranked_id = test
        .create_user(&unranked_kp, &user("Watcher:UnrankedTag:Author"))
        .await?;
    let tagger_kp = Keypair::random();
    test.create_user(&tagger_kp, &user("Watcher:RankedTag:Tagger"))
        .await?;
    rank(&ranked_id).await?;

    let (ranked_post, _) = test
        .create_post(&ranked_kp, &root_post("Watcher:RankedTag:Post"))
        .await?;
    let (unranked_post, _) = test
        .create_post(&unranked_kp, &root_post("Watcher:UnrankedTag:Post"))
        .await?;

    let mut tag_paths = Vec::new();
    for (author_id, post_id) in [(&ranked_id, &ranked_post), (&unranked_id, &unranked_post)] {
        let tag = PubkyAppTag {
            uri: post_uri_builder(author_id.clone(), post_id.clone()),
            label: label.to_string(),
            created_at: Utc::now().timestamp_millis(),
        };
        let tag_path = tag.hs_path();
        test.put(&tagger_kp, &tag_path, tag).await?;
        tag_paths.push(tag_path);
    }

    let source = score_in(&source_set, &ranked_id, &ranked_post).await?;
    assert!(source.is_some(), "the post is in the label's timeline");
    assert_eq!(
        score_in(&ranked_set, &ranked_id, &ranked_post).await?,
        source,
        "mirrored at the label timeline's score"
    );
    assert!(score_in(&source_set, &unranked_id, &unranked_post)
        .await?
        .is_some());
    assert!(
        score_in(&ranked_set, &unranked_id, &unranked_post)
            .await?
            .is_none(),
        "an unranked author's post stays out of the label's ranked timeline"
    );

    // The only tagger removes the label: the post leaves both timelines.
    test.del(&tagger_kp, &tag_paths[0]).await?;
    assert!(score_in(&source_set, &ranked_id, &ranked_post)
        .await?
        .is_none());
    assert!(
        score_in(&ranked_set, &ranked_id, &ranked_post)
            .await?
            .is_none(),
        "an untagged post leaves the label's ranked timeline"
    );

    unrank(&ranked_id).await?;
    Ok(())
}
