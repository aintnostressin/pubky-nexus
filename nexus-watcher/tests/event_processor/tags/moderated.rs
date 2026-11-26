use super::resource_utils::{compute_resource_id, find_resource_tag, resource_exists_in_graph};
use super::utils::find_post_tag;
use crate::event_processor::posts::utils::find_post_details;
use crate::event_processor::users::utils::{find_user_counts, set_user_epoch};
use crate::event_processor::utils::watcher::{
    retrieve_and_handle_event_line, HomeserverHashIdPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::{exec_single_row, graph::Query};
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::takedown::{is_taken_down, record_takedown};
use nexus_common::types::Pagination;
use nexus_common::utils::test_utils::default_ingestor_tests;
use nexus_watcher::events::handlers::tag::remove_resource_if_taken_down;
use nexus_watcher::events::inputs::{Reference, ReferenceTarget};
use nexus_watcher::events::retry::RedisRetryStore;
use nexus_watcher::events::{DefaultEventHandler, Moderation};
use pubky::{recovery_file, Keypair};
use pubky_social_specs::legacy_v0::traits::HashId;
use pubky_social_specs::legacy_v0::{
    post_uri_builder, tag_uri_builder, PubkyAppPost, PubkyAppTag, PubkyAppUser,
};
use pubky_social_specs::{PubkyId, PubkySocialTag};
use std::sync::Arc;
use tokio::fs;

const MODERATION_LABEL: &str = "label_to_moderate";

#[tokio_shared_rt::test(shared)]
async fn test_moderation_deletes_pubky_app_tag() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "pubky-app").await?;

    let label = "mod_pubky_app";
    let tag_uri = put_pubky_app_post_tag(
        &mut test, &tagger_kp, &tagger_id, &author_id, &post_id, label,
    )
    .await?;

    let post_tag = find_post_tag(&author_id, &post_id, label)
        .await?
        .expect("post tag should exist before moderation");
    assert_eq!(post_tag.taggers_count, 1);

    moderate_tag(&mut test, &moderator_kp, &tag_uri).await?;

    let post_tag = find_post_tag(&author_id, &post_id, label).await?;
    assert!(post_tag.is_none(), "pubky.app tag should be moderated");

    Ok(())
}

/// Nexus no longer indexes another app's tag folder, but the tags indexed from one before stay,
/// keyed by tagger and tag id like their `pubky.app` copy. A moderator tag naming such a tag's
/// app-folder address still takes it down, a resource tag and a post tag alike.
#[tokio_shared_rt::test(shared)]
async fn test_moderation_through_an_app_folder_address_deletes_the_tag() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "app-folder").await?;

    let target_uri = format!("https://example.com/moderate-app-folder/{tagger_id}");
    let resource_label = "mod_app_folder_res";
    let (resource_tag_uri, resource_id) = put_pubky_app_resource_tag(
        &mut test,
        &tagger_kp,
        &tagger_id,
        &target_uri,
        resource_label,
    )
    .await?;
    let post_label = "mod_app_folder_post";
    let post_tag_uri = put_pubky_app_post_tag(
        &mut test, &tagger_kp, &tagger_id, &author_id, &post_id, post_label,
    )
    .await?;
    assert!(
        find_resource_tag(&resource_id, resource_label)
            .await?
            .is_some(),
        "resource tag should exist before moderation"
    );
    assert!(
        find_post_tag(&author_id, &post_id, post_label)
            .await?
            .is_some(),
        "post tag should exist before moderation"
    );

    moderate_tag(
        &mut test,
        &moderator_kp,
        &app_folder_address(&resource_tag_uri),
    )
    .await?;
    moderate_tag(&mut test, &moderator_kp, &app_folder_address(&post_tag_uri)).await?;

    assert!(
        find_resource_tag(&resource_id, resource_label)
            .await?
            .is_none(),
        "resource tag should be moderated through its app-folder address"
    );
    assert!(
        find_post_tag(&author_id, &post_id, post_label)
            .await?
            .is_none(),
        "post tag should be moderated through its app-folder address"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_moderation_keeps_same_label_resource_tag_from_same_user() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "same-user").await?;

    let label = "mod_same_user";
    let pubky_app_tag_uri = put_pubky_app_post_tag(
        &mut test, &tagger_kp, &tagger_id, &author_id, &post_id, label,
    )
    .await?;
    let target_uri = format!("https://example.com/same-user/{tagger_id}");
    let (_resource_tag_uri, resource_id) =
        put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &target_uri, label).await?;

    moderate_tag(&mut test, &moderator_kp, &pubky_app_tag_uri).await?;

    assert!(
        find_post_tag(&author_id, &post_id, label).await?.is_none(),
        "targeted pubky.app tag should be moderated"
    );
    assert!(
        find_resource_tag(&resource_id, label).await?.is_some(),
        "same-label resource tag from same user should remain"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_moderation_keeps_same_label_pubky_app_tag_from_different_user() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let (pubky_tagger_kp, pubky_tagger_id) =
        create_tagger(&mut test, "different-user-pubky").await?;
    let (resource_tagger_kp, resource_tagger_id) =
        create_tagger(&mut test, "different-user-resource").await?;

    let label = "mod_diff_user";
    put_pubky_app_post_tag(
        &mut test,
        &pubky_tagger_kp,
        &pubky_tagger_id,
        &author_id,
        &post_id,
        label,
    )
    .await?;
    let target_uri = format!("https://example.com/different-user/{resource_tagger_id}");
    let (resource_tag_uri, resource_id) = put_pubky_app_resource_tag(
        &mut test,
        &resource_tagger_kp,
        &resource_tagger_id,
        &target_uri,
        label,
    )
    .await?;

    moderate_tag(&mut test, &moderator_kp, &resource_tag_uri).await?;

    assert!(
        find_resource_tag(&resource_id, label).await?.is_none(),
        "targeted resource tag should be moderated"
    );
    let post_tag = find_post_tag(&author_id, &post_id, label)
        .await?
        .expect("same-label pubky.app tag from another user should remain");
    assert_eq!(post_tag.taggers_count, 1);
    assert!(post_tag.taggers.contains(&pubky_tagger_id));

    Ok(())
}

/// A moderator tag whose target names nothing Nexus deletes (a follow) is a no-op that
/// succeeds: nothing is deleted or indexed.
#[tokio_shared_rt::test(shared)]
async fn test_moderation_of_an_unresolved_target_is_a_no_op() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let moderator_id = moderator_kp.public_key().to_z32();
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let tagged_before = find_user_counts(&moderator_id).await.tagged;

    // Switch OFF event processing so the tag is handled only by the call below
    test = test.remove_event_processing().await;

    let follow = format!("pubky://{author_id}/pub/pubky.app/follows/{moderator_id}");
    let tag = PubkyAppTag {
        uri: follow.clone(),
        label: MODERATION_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;

    let tag_event = format!("PUT pubky://{moderator_id}/pub/pubky.app/tags/{tag_id}");
    let event_handler = test.event_processor_runner.event_handler.clone();
    retrieve_and_handle_event_line(&tag_event, &test.homeserver_id, event_handler)
        .await
        .unwrap_or_else(|err| panic!("moderating {follow} should succeed: {err:?}"));

    let post = find_post_details(&author_id, &post_id).await?;
    assert!(!post.deleted, "the v0 post must not be moderated");
    assert!(
        !is_taken_down(&follow).await?,
        "nothing is recorded for a target Nexus never deletes"
    );
    assert_eq!(
        find_user_counts(&moderator_id).await.tagged,
        tagged_before,
        "the moderator tag must not be indexed"
    );

    Ok(())
}

/// A moderator tag on a foreign URI, written with a fragment that normalizes away, takes its
/// `Resource` down with every tag on it, whoever wrote them, and leaves another URI's
/// `Resource` alone. A later tag on the URI is dropped before it is written, so
/// its label doesn't reach the tag search either.
#[tokio_shared_rt::test(shared)]
async fn test_moderation_takes_down_a_foreign_resource_with_every_tag_on_it() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "resource-takedown").await?;
    let (other_kp, other_id) = create_tagger(&mut test, "resource-takedown-other").await?;

    let url = format!("https://example.com/takedown/{tagger_id}");
    let kept_url = format!("{url}/kept");
    let run = Utc::now().timestamp_millis() % 100_000_000;
    let (label, other_label, later_label) = (
        format!("restd{run}a"),
        format!("restd{run}b"),
        format!("restd{run}c"),
    );
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, &label).await?;
    put_pubky_app_resource_tag(&mut test, &other_kp, &other_id, &url, &other_label).await?;
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &kept_url, &label).await?;
    let resource_id = compute_resource_id(&url);
    assert!(resource_exists_in_graph(&resource_id).await?);

    moderate_tag(&mut test, &moderator_kp, &format!("{url}#comments")).await?;

    assert!(
        !resource_exists_in_graph(&resource_id).await?,
        "the Resource goes with every tag on it"
    );
    assert!(
        is_taken_down(&resource_id).await?,
        "recorded under the Resource's id"
    );
    assert!(
        find_resource_tag(&compute_resource_id(&kept_url), &label)
            .await?
            .is_some(),
        "another URI's Resource stays"
    );

    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, &later_label).await?;
    assert!(
        !resource_exists_in_graph(&resource_id).await?,
        "a later tag on the URI stays out"
    );
    assert!(
        TagSearch::get_by_label(&later_label, &Pagination::default())
            .await?
            .is_none_or(|labels| labels.is_empty()),
        "the dropped tag's label is not indexed"
    );

    for label in [label, other_label] {
        TagSearch::del_from_index(&label).await?;
    }
    test.cleanup_user(&tagger_kp).await?;
    test.cleanup_user(&other_kp).await?;
    Ok(())
}

/// A moderator tag on a URL longer than a `social/v1` tag's target may be is refused like any
/// such tag, so no takedown is recorded, and the moderator's next tag still applies.
#[tokio_shared_rt::test(shared)]
async fn test_moderation_of_a_url_over_the_cap_is_refused() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;

    let run = Utc::now().timestamp_millis();
    let long_url = format!(
        "https://example.com/takedown-long/{run}?q={}",
        "a".repeat(9_000)
    );
    let short_url = format!("https://example.com/takedown-long/{run}");
    moderate_tag(&mut test, &moderator_kp, &long_url).await?;
    moderate_tag(&mut test, &moderator_kp, &short_url).await?;

    assert!(
        !is_taken_down(&compute_resource_id(&long_url)).await?,
        "no takedown is recorded"
    );
    assert!(
        is_taken_down(&compute_resource_id(&short_url)).await?,
        "the moderator's next tag applies"
    );

    Ok(())
}

/// A `social/v1` moderator tag on a URL of 1,000 CJK characters, which `social/v1` accepts and
/// which normalizes to over 8 kB, takes down the `Resource` a `social/v1` tag created on it, and
/// its event processes: the takedown is keyed by the `Resource`'s id, which Neo4j's index on
/// `Takedown(uri)` holds whatever the URL's length. Read again by a Nexus whose moderator it
/// isn't, the same tag stays out.
#[tokio_shared_rt::test(shared)]
async fn test_social_v1_moderation_of_a_long_cjk_url_takes_its_resource_down() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    // A moderator of its own, at `v1` so its `social/v1` tag is read: the shared moderator
    // never flips.
    let (moderator_kp, moderator_id) = create_tagger(&mut test, "cjk-takedown-moderator").await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "cjk-takedown").await?;
    for user_id in [&moderator_id, &tagger_id] {
        set_user_epoch(user_id, "v1").await?;
    }

    // Unique per run, in the characters themselves: a prefix would take the URL over the cap.
    let run = Utc::now().timestamp_millis().to_string();
    let cjk: String = run
        .bytes()
        .map(|digit| char::from_u32(0x4E00 + u32::from(digit - b'0')).expect("a CJK character"))
        .chain(std::iter::repeat('中'))
        .take(1_000)
        .collect();
    let url = format!("https://example.com/{cjk}");
    assert!(url.chars().count() <= 1_024, "social/v1 accepts the URL");
    let resource_id = compute_resource_id(&url);

    let label = format!("cjktd{}", &run[run.len() - 8..]);
    test.put_v1_tag(&tagger_kp, &PubkySocialTag::new(url.clone(), label.clone()))
        .await?;
    assert!(
        find_resource_tag(&resource_id, &label).await?.is_some(),
        "the social/v1 tag creates the Resource"
    );

    // Read only by the handler below, whose moderator is the one above.
    test.ensure_event_processing = false;
    let moderator_tag = PubkySocialTag::new(url.clone(), MODERATION_LABEL.to_string());
    let tag_path = test.put_v1_tag(&moderator_kp, &moderator_tag).await?;
    let moderation = Arc::new(Moderation {
        id: PubkyId::try_from(moderator_id.as_str()).expect("a valid user id"),
        tags: vec![MODERATION_LABEL.to_string()],
    });
    let handler = Arc::new(DefaultEventHandler::new(
        moderation,
        default_ingestor_tests(),
        test.event_processor_runner.client.clone(),
        test.temp_dir.path().to_path_buf(),
        Arc::new(RedisRetryStore::new()),
    ));
    let tag_event = format!("PUT pubky://{moderator_id}{tag_path}");
    retrieve_and_handle_event_line(&tag_event, &test.homeserver_id, handler)
        .await
        .unwrap_or_else(|err| panic!("the moderator's tag should process: {err:?}"));

    assert!(
        !resource_exists_in_graph(&resource_id).await?,
        "the Resource goes with every tag on it"
    );
    assert!(
        is_taken_down(&resource_id).await?,
        "recorded under the Resource's id"
    );

    // The watcher's own processor reads the same tag as an ordinary one, and drops it.
    test.ensure_event_processing = true;
    test.ensure_event_processing_complete().await?;
    assert!(
        !resource_exists_in_graph(&resource_id).await?,
        "the takedown holds against a tag on the URL"
    );

    let delete = Query::new(
        "test_delete_takedown",
        "MATCH (t:Takedown {uri: $uri}) DELETE t",
    )
    .param("uri", resource_id);
    exec_single_row(delete).await?;
    TagSearch::del_from_index(&label).await?;
    // Back to `v0`, so the gate lets the `pubky.app` cleanup through.
    for (user_kp, user_id) in [(&moderator_kp, &moderator_id), (&tagger_kp, &tagger_id)] {
        set_user_epoch(user_id, "v0").await?;
        test.cleanup_user(user_kp).await?;
    }
    Ok(())
}

/// A takedown of a foreign URI that nobody tagged yet is recorded, so the tags read after it
/// never create its `Resource`.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_foreign_resource_read_before_its_tags_holds() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "resource-takedown-first").await?;

    let url = format!("https://example.com/takedown-first/{tagger_id}");
    moderate_tag(&mut test, &moderator_kp, &url).await?;
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, "mod_first").await?;

    assert!(
        !resource_exists_in_graph(&compute_resource_id(&url)).await?,
        "the tag read after the takedown is dropped"
    );
    test.cleanup_user(&tagger_kp).await?;
    Ok(())
}

/// A tag written while the moderator's tag was read elsewhere doesn't stay: once written, a
/// recorded takedown of its `Resource` deletes it. A `Resource` not taken down is left alone.
#[tokio_shared_rt::test(shared)]
async fn test_resource_tagged_after_its_takedown_was_recorded_is_removed() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "resource-takedown-race").await?;

    let url = format!("https://example.com/takedown-race/{tagger_id}");
    let kept_url = format!("{url}/kept");
    // The writes that passed the first check before the takedown was recorded.
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, "mod_race").await?;
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &kept_url, "mod_race").await?;

    // The takedown, recorded by another loop that found nothing to delete yet.
    let tag_uri = tag_uri_builder(tagger_id.clone(), "MODERATORTAG".to_string());
    record_takedown(&compute_resource_id(&url), &tag_uri).await?;

    let foreign = |uri: &str| Reference {
        uri: uri.to_string(),
        target: ReferenceTarget::Foreign,
    };
    assert!(remove_resource_if_taken_down(&foreign(&url)).await?);
    assert!(!remove_resource_if_taken_down(&foreign(&kept_url)).await?);

    assert!(
        !resource_exists_in_graph(&compute_resource_id(&url)).await?,
        "the Resource written before the takedown was recorded doesn't stay"
    );
    assert!(resource_exists_in_graph(&compute_resource_id(&kept_url)).await?);
    test.cleanup_user(&tagger_kp).await?;
    Ok(())
}

/// A takedown whose removal was left unfinished (its delete failed, or the watcher stopped
/// before it) is finished by the next tag on the URI, which is skipped.
#[tokio_shared_rt::test(shared)]
async fn test_a_later_tag_finishes_an_unfinished_resource_takedown() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "resource-takedown-finish").await?;

    let url = format!("https://example.com/takedown-finish/{tagger_id}");
    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, "mod_finish").await?;
    // Recorded, but the `Resource` was never deleted.
    let tag_uri = tag_uri_builder(tagger_id.clone(), "MODERATORTAG".to_string());
    record_takedown(&compute_resource_id(&url), &tag_uri).await?;
    assert!(resource_exists_in_graph(&compute_resource_id(&url)).await?);

    put_pubky_app_resource_tag(&mut test, &tagger_kp, &tagger_id, &url, "mod_finish_later").await?;

    assert!(
        !resource_exists_in_graph(&compute_resource_id(&url)).await?,
        "the next tag on the URI finishes the removal"
    );
    test.cleanup_user(&tagger_kp).await?;
    Ok(())
}

/// A post has one node whichever epoch's address names it: a moderator tag written as the
/// `social/v1` address of an indexed `pubky.app` post takes that post down.
#[tokio_shared_rt::test(shared)]
async fn test_moderation_through_the_social_v1_address_of_a_post_takes_it_down() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;

    let social_v1_post = format!("pubky://{author_id}/pub/social/v1/posts/{post_id}");
    moderate_tag(&mut test, &moderator_kp, &social_v1_post).await?;

    // Edge-free, so hard-deleted rather than tombstoned.
    assert!(
        find_post_details(&author_id, &post_id).await.is_err(),
        "the post must be taken down through its v1 address"
    );

    Ok(())
}

/// A takedown of a tag is recorded under its address, so it holds when the same tag is replayed.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_tag_holds_on_replay() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (_author_kp, author_id, post_id) = create_author_and_post(&mut test).await?;
    let (tagger_kp, tagger_id) = create_tagger(&mut test, "tag-replay").await?;

    let label = "mod_tag_replay";
    let tag_uri = put_pubky_app_post_tag(
        &mut test, &tagger_kp, &tagger_id, &author_id, &post_id, label,
    )
    .await?;
    assert!(
        find_post_tag(&author_id, &post_id, label).await?.is_some(),
        "the tag should exist before moderation"
    );

    moderate_tag(&mut test, &moderator_kp, &tag_uri).await?;
    assert!(
        find_post_tag(&author_id, &post_id, label).await?.is_none(),
        "the tag should be moderated"
    );

    let replayed_uri = put_pubky_app_post_tag(
        &mut test, &tagger_kp, &tagger_id, &author_id, &post_id, label,
    )
    .await?;
    assert_eq!(replayed_uri, tag_uri, "the same tag, replayed");
    assert!(
        find_post_tag(&author_id, &post_id, label).await?.is_none(),
        "the takedown holds when the tag is replayed"
    );

    Ok(())
}

async fn create_moderator(test: &mut WatcherTest) -> Result<Keypair> {
    let moderator_recovery_file =
        fs::read("./tests/event_processor/utils/moderator_key.pkarr").await?;
    let moderator_kp = recovery_file::decrypt_recovery_file(&moderator_recovery_file, "password")?;
    let moderator = test_user("Mod:Moderator", "moderator");
    test.create_user(&moderator_kp, &moderator).await?;
    Ok(moderator_kp)
}

async fn create_author_and_post(test: &mut WatcherTest) -> Result<(Keypair, String, String)> {
    let author_kp = Keypair::random();
    let author = test_user("Mod:Author", "author");
    let author_id = test.create_user(&author_kp, &author).await?;
    let post = PubkyAppPost {
        content: format!("Watcher:TagModerate:Post:{author_id}"),
        ..Default::default()
    };
    let (post_id, _) = test.create_post(&author_kp, &post).await?;
    Ok((author_kp, author_id, post_id))
}

async fn create_tagger(test: &mut WatcherTest, suffix: &str) -> Result<(Keypair, String)> {
    let tagger_kp = Keypair::random();
    let tagger = test_user("Mod:Tagger", suffix);
    let tagger_id = test.create_user(&tagger_kp, &tagger).await?;
    Ok((tagger_kp, tagger_id))
}

async fn put_pubky_app_post_tag(
    test: &mut WatcherTest,
    tagger_kp: &Keypair,
    tagger_id: &str,
    author_id: &str,
    post_id: &str,
    label: &str,
) -> Result<String> {
    let tag = PubkyAppTag {
        uri: post_uri_builder(author_id.to_string(), post_id.to_string()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    let tag_uri = tag_uri_builder(tagger_id.to_string(), tag_id);
    let tag_path = tag.hs_path();
    test.put(tagger_kp, &tag_path, tag).await?;
    Ok(tag_uri)
}

async fn put_pubky_app_resource_tag(
    test: &mut WatcherTest,
    tagger_kp: &Keypair,
    tagger_id: &str,
    target_uri: &str,
    label: &str,
) -> Result<(String, String)> {
    let tag = PubkyAppTag {
        uri: target_uri.to_string(),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    let resource_id = compute_resource_id(target_uri);
    let tag_uri = tag_uri_builder(tagger_id.to_string(), tag_id);
    let tag_path = tag.hs_path();
    test.put(tagger_kp, &tag_path, tag).await?;
    Ok((tag_uri, resource_id))
}

/// The address the same tag file had in another app's folder (`mapky`).
fn app_folder_address(pubky_app_tag_uri: &str) -> String {
    pubky_app_tag_uri.replace("/pub/pubky.app/tags/", "/pub/mapky/tags/")
}

async fn moderate_tag(
    test: &mut WatcherTest,
    moderator_kp: &Keypair,
    target_tag_uri: &str,
) -> Result<()> {
    let tag = PubkyAppTag {
        uri: target_tag_uri.to_string(),
        label: MODERATION_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_path = tag.hs_path();
    test.put(moderator_kp, &tag_path, tag).await
}

fn test_user(name: &str, bio: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: Some(bio.to_string()),
        name: name.to_string(),
        ..Default::default()
    }
}
