use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::RedisOps;
use nexus_common::models::user::StarterPackRank;
use pubky::Keypair;
use pubky_app_specs::{post_uri_builder, PubkyAppPost, PubkyAppTag, PubkyAppUser};

/// No other test uses it, so nothing else caches or drops this ranking meanwhile.
const LABEL: &str = "packinvalidation";

/// A label's starter pack ranking is cached, so a tag with the label coming or going must drop
/// it, on a profile and on a post alike.
#[tokio_shared_rt::test(shared)]
async fn test_homeserver_tag_put_and_del_drop_the_starter_pack_ranking() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let tagger_kp = Keypair::random();
    let tagger = PubkyAppUser {
        bio: Some("test_homeserver_tag_put_and_del_drop_the_starter_pack_ranking".to_string()),
        image: None,
        links: None,
        name: "Watcher:StarterPackInvalidation:Tagger".to_string(),
        status: None,
    };
    test.create_user(&tagger_kp, &tagger).await?;

    let author_kp = Keypair::random();
    let author = PubkyAppUser {
        bio: Some("test_homeserver_tag_put_and_del_drop_the_starter_pack_ranking".to_string()),
        image: None,
        links: None,
        name: "Watcher:StarterPackInvalidation:Author".to_string(),
        status: None,
    };
    let author_id = test.create_user(&author_kp, &author).await?;

    let post = PubkyAppPost {
        content: "Watcher:StarterPackInvalidation:Post".to_string(),
        kind: PubkyAppPost::default().kind,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (post_id, post_path) = test.create_post(&author_kp, &post).await?;

    let profile_uri = format!("pubky://{author_id}/pub/pubky.app/profile.json");
    for uri in [profile_uri, post_uri_builder(author_id.clone(), post_id)] {
        let tag = PubkyAppTag {
            uri,
            label: LABEL.to_string(),
            created_at: Utc::now().timestamp_millis(),
        };
        let tag_path = tag.hs_path();

        StarterPackRank::compute_and_cache(LABEL).await?;
        assert!(cached().await?.is_some(), "the ranking should start cached");
        test.put(&tagger_kp, &tag_path, tag).await?;
        assert!(
            cached().await?.is_none(),
            "indexing a tag with the label should drop its ranking"
        );

        StarterPackRank::compute_and_cache(LABEL).await?;
        test.del(&tagger_kp, &tag_path).await?;
        assert!(
            cached().await?.is_none(),
            "deleting a tag with the label should drop its ranking"
        );
    }

    test.cleanup_post(&author_kp, &post_path).await?;
    test.cleanup_user(&author_kp).await?;
    test.cleanup_user(&tagger_kp).await?;

    Ok(())
}

async fn cached() -> Result<Option<StarterPackRank>> {
    Ok(StarterPackRank::try_from_index_json(&[LABEL], None).await?)
}
