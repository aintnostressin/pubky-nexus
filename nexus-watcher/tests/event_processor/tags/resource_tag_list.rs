use super::resource_utils::{compute_resource_id, resource_label_scores_exist};
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::{get_redis_conn, RedisOps};
use nexus_common::models::resource::tag::{TagResource, RESOURCE_TAGS_KEY_PARTS};
use nexus_common::models::tag::post::{TagPost, POST_TAGS_KEY_PARTS};
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::{TagUser, USER_TAGS_KEY_PARTS};
use nexus_common::models::tag::TagDetails;
use pubky::Keypair;
use pubky::ResourcePath;
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{post_uri_builder, PubkyAppPost, PubkyAppTag, PubkyAppUser};

const APP: &str = "mapky";

/// Resource tag lists are read from the graph: they follow every put and del,
/// rank labels by tagger count (ties by label), page them, cap each label's
/// tagger preview, and flag the viewer against all of a label's taggers, not
/// only the previewed ones.
#[tokio_shared_rt::test(shared)]
async fn test_resource_tag_list_follows_puts_and_dels() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let (alice_kp, alice_id) = create_tagger(&mut test, "Alice").await?;
    let (bob_kp, bob_id) = create_tagger(&mut test, "Bob").await?;
    let (carol_kp, carol_id) = create_tagger(&mut test, "Carol").await?;

    // A fresh user id in the URI keeps the resource unique to this run
    let target_uri = format!("https://example.com/tag-list/{alice_id}");
    let resource_id = compute_resource_id(&target_uri);

    // Alice and Bob tag "rust", Alice also tags "nostr"
    let alice_rust = put_resource_tag(&mut test, &alice_kp, &target_uri, "rust").await?;
    let bob_rust = put_resource_tag(&mut test, &bob_kp, &target_uri, "rust").await?;
    let alice_nostr = put_resource_tag(&mut test, &alice_kp, &target_uri, "nostr").await?;

    // Ranked by tagger count; without a viewer nobody is flagged
    let tags = tag_list(&resource_id, None, None, None, None).await?;
    assert_eq!(labels(&tags), ["rust", "nostr"]);
    assert_eq!(tags[0].taggers_count, 2);
    assert_eq!(
        sorted(&tags[0].taggers),
        sorted(&[alice_id.clone(), bob_id.clone()])
    );
    assert_eq!(tags[1].taggers, [alice_id.as_str()]);
    assert_eq!(relationships(&tags), [false, false]);

    // The viewer is flagged on the labels they applied, and only there
    let tags = tag_list(&resource_id, None, None, None, Some(&bob_id)).await?;
    assert_eq!(relationships(&tags), [true, false]);
    let tags = tag_list(&resource_id, None, None, None, Some(&carol_id)).await?;
    assert_eq!(relationships(&tags), [false, false]);

    // The preview is capped, the count is not, and the flag checks every
    // tagger: one of Alice and Bob is left out of a one-tagger preview
    for viewer in [&alice_id, &bob_id] {
        let tags = tag_list(&resource_id, None, None, Some(1), Some(viewer)).await?;
        assert_eq!(tags[0].taggers.len(), 1);
        assert_eq!(tags[0].taggers_count, 2);
        assert!(tags[0].relationship, "{viewer} tagged rust");
    }

    // Labels are paginated
    let tags = tag_list(&resource_id, None, Some(1), None, None).await?;
    assert_eq!(labels(&tags), ["rust"]);
    let tags = tag_list(&resource_id, Some(1), None, None, None).await?;
    assert_eq!(labels(&tags), ["nostr"]);

    // Bob removes "rust": both labels have one tagger, so the label breaks the tie
    test.del(&bob_kp, &bob_rust).await?;
    let tags = tag_list(&resource_id, None, None, None, Some(&bob_id)).await?;
    assert_eq!(labels(&tags), ["nostr", "rust"]);
    for tag in &tags {
        assert_eq!(tag.taggers, [alice_id.as_str()]);
        assert_eq!(tag.taggers_count, 1);
        assert!(!tag.relationship, "Bob no longer tags {}", tag.label);
    }

    // Alice removes "nostr", then "rust", which takes the resource with it
    test.del(&alice_kp, &alice_nostr).await?;
    let tags = tag_list(&resource_id, None, None, None, None).await?;
    assert_eq!(labels(&tags), ["rust"]);
    test.del(&alice_kp, &alice_rust).await?;
    let tags = TagResource::get_by_id(&resource_id, None, None, None, None).await?;
    assert!(tags.is_none(), "The untagged resource should be gone");

    for user_kp in [&alice_kp, &bob_kp, &carol_kp] {
        test.cleanup_user(user_kp).await?;
    }

    Ok(())
}

/// Resource tag puts and dels leave the retired label-score sorted set alone,
/// and tag-list reads only consult the graph: a stale sorted set is ignored,
/// and a read rebuilds neither it nor an evicted tagger set.
#[tokio_shared_rt::test(shared)]
async fn test_resource_tag_list_ignores_and_never_rebuilds_redis_indexes() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let (user_kp, user_id) = create_tagger(&mut test, "Indexes").await?;
    let target_uri = format!("https://example.com/tag-list-indexes/{user_id}");
    let resource_id = compute_resource_id(&target_uri);
    let label = "rust";

    // The put indexes both tagger sets, and no label score
    let tag_path = put_resource_tag(&mut test, &user_kp, &target_uri, label).await?;
    for key_parts in [
        vec![resource_id.as_str(), label],
        vec![resource_id.as_str(), APP, label],
    ] {
        let (_, is_member) = TagResource::check_set_member(&key_parts, &user_id).await?;
        assert!(
            is_member,
            "Tagger should be in the {key_parts:?} tagger set"
        );
    }
    assert!(
        !resource_label_scores_exist(&resource_id).await?,
        "The put must not write label scores"
    );

    // A stale label-score set is ignored. Read through it, the list would hide
    // "rust" (score 0) and rank "stale" (score 9), which has no taggers
    let scores_key_parts: Vec<&str> =
        [&RESOURCE_TAGS_KEY_PARTS[..], &[resource_id.as_str()]].concat();
    TagResource::put_index_sorted_set(
        &scores_key_parts,
        &[(0.0, label), (9.0, "stale")],
        None,
        None,
    )
    .await?;
    let tags = tag_list(&resource_id, None, None, None, Some(&user_id)).await?;
    assert_eq!(labels(&tags), [label]);
    assert_eq!(tags[0].taggers, [user_id.as_str()]);
    assert!(tags[0].relationship);

    // Evicted indexes stay evicted: the read lists the tagger from the graph
    // and writes nothing back
    let mut redis_conn = get_redis_conn().await?;
    let evicted_keys = [
        format!("Sorted:{}", scores_key_parts.join(":")),
        format!("{}:{resource_id}:{label}", TagResource::prefix().await),
    ];
    let _: () = redis_conn.del(&evicted_keys).await?;
    let tags = tag_list(&resource_id, None, None, None, Some(&user_id)).await?;
    assert_eq!(tags[0].taggers, [user_id.as_str()]);
    assert!(tags[0].relationship);
    for key in &evicted_keys {
        let exists: bool = redis_conn.exists(key).await?;
        assert!(!exists, "The read must not rebuild {key}");
    }

    // The del does not write label scores either
    test.del(&user_kp, &tag_path).await?;
    assert!(
        !resource_label_scores_exist(&resource_id).await?,
        "The del must not write label scores"
    );
    let tags = TagResource::get_by_id(&resource_id, None, None, None, None).await?;
    assert!(tags.is_none(), "The untagged resource should be gone");

    test.cleanup_user(&user_kp).await?;

    Ok(())
}

/// Only the resource path reads from the graph directly: user and post tag
/// lists keep their Redis indexes and rebuild them from the graph after an
/// eviction.
#[tokio_shared_rt::test(shared)]
async fn test_user_and_post_tag_lists_still_rebuild_their_indexes() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let (author_kp, author_id) = create_tagger(&mut test, "Author").await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "Tagger").await?;
    let post = PubkyAppPost {
        content: "Watcher:ResourceTagList:Post".to_string(),
        kind: PubkyAppPost::default().kind,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (post_id, post_path) = test.create_post(&author_kp, &post).await?;

    // Tag the author's profile and post with the same label
    let label = "rebuilt";
    let mut tag_paths = Vec::new();
    for uri in [
        format!("pubky://{author_id}/pub/pubky.app/profile.json"),
        post_uri_builder(author_id.clone(), post_id.clone()),
    ] {
        let tag = PubkyAppTag {
            uri,
            label: label.to_string(),
            created_at: Utc::now().timestamp_millis(),
        };
        let tag_path = tag.hs_path();
        test.put(&tagger_kp, &tag_path, tag).await?;
        tag_paths.push(tag_path);
    }

    // Evict both label-score sets and both tagger sets
    let index_keys = [
        format!("Sorted:{}:{author_id}", USER_TAGS_KEY_PARTS.join(":")),
        format!("{}:{author_id}:{label}", TagUser::prefix().await),
        format!(
            "Sorted:{}:{author_id}:{post_id}",
            POST_TAGS_KEY_PARTS.join(":")
        ),
        format!("{}:{author_id}:{post_id}:{label}", TagPost::prefix().await),
    ];
    let mut redis_conn = get_redis_conn().await?;
    let _: () = redis_conn.del(&index_keys).await?;

    // Both reads still list the tag and rebuild every evicted key
    let user_tags = TagUser::get_by_id(&author_id, None, None, None, None, None, None)
        .await?
        .expect("User should have tags");
    let post_tags = TagPost::get_by_id(&author_id, Some(&post_id), None, None, None, None, None)
        .await?
        .expect("Post should have tags");
    for tags in [&user_tags, &post_tags] {
        assert_eq!(labels(tags), [label]);
        assert_eq!(tags[0].taggers, [tagger_id.as_str()]);
    }
    for key in &index_keys {
        let exists: bool = redis_conn.exists(key).await?;
        assert!(exists, "The read should rebuild {key}");
    }

    for tag_path in &tag_paths {
        test.del(&tagger_kp, tag_path).await?;
    }
    test.cleanup_post(&author_kp, &post_path).await?;
    test.cleanup_user(&author_kp).await?;
    test.cleanup_user(&tagger_kp).await?;

    Ok(())
}

async fn create_tagger(test: &mut WatcherTest, name: &str) -> Result<(Keypair, String)> {
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_resource_tag_list".to_string()),
        image: None,
        links: None,
        name: format!("Watcher:ResourceTagList:{name}"),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;
    Ok((user_kp, user_id))
}

/// PUTs a `label` tag on `uri` under the app's tag path, so the watcher indexes
/// it as a Resource tag
async fn put_resource_tag(
    test: &mut WatcherTest,
    user_kp: &Keypair,
    uri: &str,
    label: &str,
) -> Result<ResourcePath> {
    let tag = PubkyAppTag {
        uri: uri.to_string(),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_path: ResourcePath = format!("/pub/{APP}/tags/{}", tag.create_id()).parse()?;
    test.put(user_kp, &tag_path, &tag).await?;
    Ok(tag_path)
}

async fn tag_list(
    resource_id: &str,
    skip_tags: Option<usize>,
    limit_tags: Option<usize>,
    limit_taggers: Option<usize>,
    viewer_id: Option<&str>,
) -> Result<Vec<TagDetails>> {
    TagResource::get_by_id(resource_id, skip_tags, limit_tags, limit_taggers, viewer_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Resource {resource_id} should have a tag list"))
}

fn labels(tags: &[TagDetails]) -> Vec<&str> {
    tags.iter().map(|tag| tag.label.as_str()).collect()
}

fn relationships(tags: &[TagDetails]) -> Vec<bool> {
    tags.iter().map(|tag| tag.relationship).collect()
}

fn sorted(ids: &[String]) -> Vec<String> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids
}
