//! A moderator tag on a `{hash}.{ext}` address removes that v1 folder and nothing else, and a
//! later PUT at the address stays down; a tag on a `{tsid}` address removes that legacy folder
//! and its `File` node, and takes down the address the flip copies it to, nothing else. Prior art:
//! `tags/moderated.rs` and `delete.rs`.

use super::social_v1::{cleanup_v1_user, handler_over_mock, v1_user, V1File};
use crate::event_processor::posts::utils::test_user;
use crate::event_processor::tags::resource_utils::{compute_resource_id, resource_exists_in_graph};
use crate::event_processor::users::utils::set_user_epoch;
use crate::event_processor::utils::watcher::{
    assert_file_details, retrieve_and_handle_event_line, HomeserverHashIdPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::file::FileDetails;
use nexus_common::models::takedown::is_taken_down;
use nexus_common::models::traits::Collection;
use nexus_watcher::events::handlers::file::blob_takedown_key;
use pubky::{recovery_file, Keypair, ResourcePath};
use pubky_social_specs::legacy_v0::traits::{HasIdPath, HashId as _};
use pubky_social_specs::legacy_v0::{
    blob_uri_builder, file_uri_builder as v0_file_uri_builder, PubkyAppBlob, PubkyAppFile,
    PubkyAppTag,
};
use std::path::PathBuf;
use tokio::fs;

const MODERATION_LABEL: &str = "label_to_moderate";

/// The moderator the test handler trusts, at `v0`, so their `pubky.app` tags are handled.
async fn create_moderator(test: &mut WatcherTest) -> Result<Keypair> {
    let recovery = fs::read("./tests/event_processor/utils/moderator_key.pkarr").await?;
    let moderator_kp = recovery_file::decrypt_recovery_file(&recovery, "password")?;
    test.create_user(&moderator_kp, &test_user("Mod:Moderator", "moderator"))
        .await?;
    Ok(moderator_kp)
}

/// The moderator tags `target_uri` with the moderated label, and the processor reads it.
async fn moderate(test: &mut WatcherTest, moderator_kp: &Keypair, target_uri: &str) -> Result<()> {
    let tag = PubkyAppTag {
        uri: target_uri.to_string(),
        label: MODERATION_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(moderator_kp, &tag.hs_path(), tag).await
}

/// A `pubky.app` file of `user_kp`, indexed: its id, its address and its `main` on disk.
async fn create_v0_file(
    test: &mut WatcherTest,
    user_kp: &Keypair,
    user_id: &str,
    content: &str,
) -> Result<(String, String, PathBuf)> {
    let blob = PubkyAppBlob::new(content.as_bytes().to_vec());
    let blob_id = blob.create_id();
    let blob_uri = blob_uri_builder(user_id.to_string(), blob_id.clone());
    test.create_file_from_body(
        user_kp,
        &PubkyAppBlob::create_path(&blob_id),
        blob.0.clone(),
    )
    .await?;
    let file = PubkyAppFile {
        name: "takedown".to_string(),
        content_type: "text/plain".to_string(),
        src: blob_uri.clone(),
        size: blob.0.len(),
        created_at: Utc::now().timestamp_millis(),
    };
    let (file_id, _) = test.create_file(user_kp, &file).await?;
    let details = assert_file_details(user_id, &file_id, &blob_uri, &file).await;
    let main = test.temp_dir.path().join(&details.urls.main);
    assert!(main.exists(), "the v0 file's main must be on disk");
    Ok((
        file_id.clone(),
        v0_file_uri_builder(user_id.to_string(), file_id),
        main,
    ))
}

/// A `social/v1` file of `user_kp`, who must be at `v1`, indexed: the file, its address and
/// its `main` on disk.
async fn create_v1_file(
    test: &mut WatcherTest,
    user_kp: &Keypair,
    user_id: &str,
    content: &str,
) -> Result<(V1File, String, PathBuf)> {
    let file = V1File::new(content.as_bytes().to_vec(), "png");
    file.put(test, user_kp).await?;
    let main = file.main_path(test.temp_dir.path(), user_id);
    assert!(main.exists(), "the v1 file's main must be on disk");
    let uri = file.uri(user_id);
    Ok((file, uri, main))
}

/// `tagger_kp` tags `target_uri`, which Nexus indexes as a `Resource` tag; returns the
/// `Resource` id.
async fn tag_resource(
    test: &mut WatcherTest,
    tagger_kp: &Keypair,
    target_uri: &str,
) -> Result<String> {
    let tag = PubkyAppTag {
        uri: target_uri.to_string(),
        label: "takedown_resource".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(tagger_kp, &tag.hs_path(), tag).await?;
    Ok(compute_resource_id(target_uri))
}

/// Whether the owner's `File` node for `file_id` exists.
async fn v0_file_node_exists(user_id: &str, file_id: &str) -> Result<bool> {
    let nodes = FileDetails::get_by_ids(&[&[user_id, file_id]]).await?;
    Ok(!nodes.is_empty() && nodes[0].is_some())
}

/// A tag on a v1 file's address removes its folder and records the takedown under the blob's
/// key, so a later PUT at the address is skipped before any fetch and the file stays down, and
/// so does a PUT of the same bytes under another extension.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_v1_file_removes_its_folder_and_holds_against_a_later_put() -> Result<()>
{
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (author_kp, author_id) = v1_user(&mut test, "Watcher:V1File:Takedown").await?;
    let (file, uri, main) =
        create_v1_file(&mut test, &author_kp, &author_id, "Watcher:V1File:Takedown").await?;

    moderate(&mut test, &moderator_kp, &uri).await?;

    assert!(!main.exists(), "the takedown must remove the v1 folder");
    assert!(
        is_taken_down(&blob_takedown_key(&author_id, file.hash())).await?,
        "the takedown must be recorded under the blob's key"
    );

    // A later PUT at the address, over a homeserver that serves nothing: skipped before any
    // fetch, so it succeeds and leaves the file down.
    let (handler, client) = handler_over_mock(test.temp_dir.path());
    retrieve_and_handle_event_line(&format!("PUT {uri}"), &test.homeserver_id, handler)
        .await
        .expect("a PUT of a taken-down file is skipped, not failed");
    assert!(client.blob_fetches().await.is_empty(), "no fetch");
    assert!(!main.exists(), "the file must stay down");

    // And through the processor, written again by its author.
    file.put(&mut test, &author_kp).await?;
    assert!(
        !main.exists(),
        "the file must stay down when uploaded again"
    );

    // And the same bytes under another extension: the takedown is the blob's.
    let as_bin = V1File::new(b"Watcher:V1File:Takedown".to_vec(), "bin");
    as_bin.put(&mut test, &author_kp).await?;
    assert!(
        !as_bin.main_path(test.temp_dir.path(), &author_id).exists(),
        "the same bytes under another extension must stay down"
    );

    as_bin.del(&mut test, &author_kp).await?;
    file.del(&mut test, &author_kp).await?;
    cleanup_v1_user(&mut test, &author_kp, &author_id).await
}

/// A tag on a `{tsid}` address removes that legacy folder and its node, and takes down the blob
/// the flip would copy it to, under its key: the owner's own v1 upload of the same bytes under
/// another extension, `H.png` for this `text/plain` file, goes with it, and a PUT of it again is
/// skipped. The bytes are the content taken down, whichever extension names them.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_tsid_address_takes_the_owners_v1_upload_of_its_bytes_down() -> Result<()>
{
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let author_kp = Keypair::random();
    let author_id = test
        .create_user(
            &author_kp,
            &test_user("Watcher:V1File:TsidTakedown", "takedown"),
        )
        .await?;
    let (file_id, v0_uri, v0_main) = create_v0_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V1File:TsidTakedown",
    )
    .await?;
    set_user_epoch(&author_id, "v1").await?;
    let (v1_file, _, v1_main) = create_v1_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V1File:TsidTakedown",
    )
    .await?;

    moderate(&mut test, &moderator_kp, &v0_uri).await?;

    assert!(
        !v0_main.exists(),
        "the takedown must remove the legacy folder"
    );
    assert!(
        !v0_file_node_exists(&author_id, &file_id).await?,
        "the takedown must delete the File node"
    );
    assert!(
        !v1_main.exists(),
        "the owner's v1 upload of the same bytes must go with the takedown"
    );
    assert!(
        is_taken_down(&blob_takedown_key(&author_id, v1_file.hash())).await?,
        "the blob the flip would copy the file to is taken down"
    );

    // Uploaded again at `H.png`: the key carries no extension, so the PUT is skipped.
    v1_file.put(&mut test, &author_kp).await?;
    assert!(!v1_main.exists(), "the v1 upload must stay down");

    v1_file.del(&mut test, &author_kp).await?;
    cleanup_v1_user(&mut test, &author_kp, &author_id).await
}

/// A tag on a v1 file's address removes that folder and leaves the owner's legacy file alone:
/// its folder and its node stay.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_v1_address_leaves_the_legacy_file_alone() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let author_kp = Keypair::random();
    let author_id = test
        .create_user(
            &author_kp,
            &test_user("Watcher:V1File:V1Takedown", "takedown"),
        )
        .await?;
    let (file_id, v0_uri, v0_main) = create_v0_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V1File:V1Takedown",
    )
    .await?;
    set_user_epoch(&author_id, "v1").await?;
    let (_, v1_uri, v1_main) = create_v1_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V1File:V1Takedown",
    )
    .await?;

    moderate(&mut test, &moderator_kp, &v1_uri).await?;

    assert!(!v1_main.exists(), "the takedown must remove the v1 folder");
    assert!(v0_main.exists(), "the legacy folder must be left alone");
    assert!(
        v0_file_node_exists(&author_id, &file_id).await?,
        "the File node must be left alone"
    );
    assert!(!is_taken_down(&v0_uri).await?);

    // Back to `v0`, so the gate lets the `pubky.app` cleanup through.
    set_user_epoch(&author_id, "v0").await?;
    let v0_path: ResourcePath = PubkyAppFile::create_path(&file_id).parse()?;
    test.cleanup_file(&author_kp, &v0_path).await?;
    test.cleanup_user(&author_kp).await
}

/// A takedown of a v1 file takes down the tags on every address of its bytes, whatever the
/// extension, and a later tag on one of them stays out. Another blob's address keeps its tags.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_v1_file_takes_down_the_tags_on_its_addresses() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let (author_kp, author_id) = v1_user(&mut test, "Watcher:V1File:TagTakedown").await?;
    let tagger_kp = Keypair::random();
    test.create_user(&tagger_kp, &test_user("Watcher:V1File:Tagger", "tagger"))
        .await?;
    let (file, uri, _) = create_v1_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V1File:TagTakedown",
    )
    .await?;
    let as_bin = uri.replace(".png", ".bin");
    let other_blob =
        V1File::new(b"Watcher:V1File:TagTakedown:Other".to_vec(), "png").uri(&author_id);

    let on_png = tag_resource(&mut test, &tagger_kp, &uri).await?;
    let on_bin = tag_resource(&mut test, &tagger_kp, &as_bin).await?;
    let on_other = tag_resource(&mut test, &tagger_kp, &other_blob).await?;
    for id in [&on_png, &on_bin, &on_other] {
        assert!(resource_exists_in_graph(id).await?);
    }

    moderate(&mut test, &moderator_kp, &uri).await?;

    assert!(
        !resource_exists_in_graph(&on_png).await?,
        "the tagged address goes"
    );
    assert!(
        !resource_exists_in_graph(&on_bin).await?,
        "the same bytes under another extension go"
    );
    assert!(
        resource_exists_in_graph(&on_other).await?,
        "another blob's tags stay"
    );

    let as_jpg = tag_resource(&mut test, &tagger_kp, &uri.replace(".png", ".jpg")).await?;
    assert!(
        !resource_exists_in_graph(&as_jpg).await?,
        "a later tag stays out"
    );

    test.cleanup_user(&tagger_kp).await?;
    file.del(&mut test, &author_kp).await?;
    cleanup_v1_user(&mut test, &author_kp, &author_id).await
}

/// A takedown of a v0 file takes down the tags on its address, in any spelling its reader
/// accepts (a trailing slash), and on the `social/v1` addresses of its bytes, where the flip
/// copies it. Another file's address keeps its tags.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_of_a_v0_file_takes_down_the_tags_on_its_addresses() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let author_kp = Keypair::random();
    let author_id = test
        .create_user(
            &author_kp,
            &test_user("Watcher:V0File:TagTakedown", "takedown"),
        )
        .await?;
    let tagger_kp = Keypair::random();
    test.create_user(&tagger_kp, &test_user("Watcher:V0File:Tagger", "tagger"))
        .await?;
    let content = "Watcher:V0File:TagTakedown";
    let (_, v0_uri, _) = create_v0_file(&mut test, &author_kp, &author_id, content).await?;
    let (kept_id, kept_uri, _) = create_v0_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V0File:TagTakedown:Kept",
    )
    .await?;
    let flip_copy = V1File::new(content.as_bytes().to_vec(), "txt").uri(&author_id);

    let on_v0 = tag_resource(&mut test, &tagger_kp, &v0_uri).await?;
    let on_slash = tag_resource(&mut test, &tagger_kp, &format!("{v0_uri}/")).await?;
    let on_copy = tag_resource(&mut test, &tagger_kp, &flip_copy).await?;
    let on_kept = tag_resource(&mut test, &tagger_kp, &kept_uri).await?;
    for id in [&on_v0, &on_slash, &on_copy, &on_kept] {
        assert!(resource_exists_in_graph(id).await?);
    }

    moderate(&mut test, &moderator_kp, &v0_uri).await?;

    assert!(
        !resource_exists_in_graph(&on_v0).await?,
        "the file's address goes"
    );
    assert!(
        !resource_exists_in_graph(&on_slash).await?,
        "its address with a trailing slash goes"
    );
    assert!(
        !resource_exists_in_graph(&on_copy).await?,
        "the address of its flip copy goes"
    );
    assert!(
        resource_exists_in_graph(&on_kept).await?,
        "another file's tags stay"
    );

    let later = tag_resource(&mut test, &tagger_kp, &v0_uri).await?;
    assert!(
        !resource_exists_in_graph(&later).await?,
        "a later tag stays out"
    );

    test.cleanup_user(&tagger_kp).await?;
    let kept_path: ResourcePath = PubkyAppFile::create_path(&kept_id).parse()?;
    test.cleanup_file(&author_kp, &kept_path).await?;
    test.cleanup_user(&author_kp).await
}

/// A moderator tag on a spelling of a v0 file address that its prefix read misses (a `:port`)
/// still takes down the `Resource` at that spelling.
#[tokio_shared_rt::test(shared)]
async fn test_takedown_through_a_spelling_with_a_port_removes_its_resource() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;
    let moderator_kp = create_moderator(&mut test).await?;
    let author_kp = Keypair::random();
    let author_id = test
        .create_user(
            &author_kp,
            &test_user("Watcher:V0File:PortTakedown", "takedown"),
        )
        .await?;
    let tagger_kp = Keypair::random();
    test.create_user(
        &tagger_kp,
        &test_user("Watcher:V0File:PortTagger", "tagger"),
    )
    .await?;
    let (_, v0_uri, _) = create_v0_file(
        &mut test,
        &author_kp,
        &author_id,
        "Watcher:V0File:PortTakedown",
    )
    .await?;
    let with_port = v0_uri.replacen(&author_id, &format!("{author_id}:1"), 1);

    let on_port = tag_resource(&mut test, &tagger_kp, &with_port).await?;
    assert!(resource_exists_in_graph(&on_port).await?);

    moderate(&mut test, &moderator_kp, &with_port).await?;

    assert!(
        !resource_exists_in_graph(&on_port).await?,
        "the tagged spelling goes"
    );
    test.cleanup_user(&tagger_kp).await?;
    test.cleanup_user(&author_kp).await
}
