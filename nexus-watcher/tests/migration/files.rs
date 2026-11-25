//! Uploads across a switch: a file record names a blob, and nexus downloads
//! the blob from the homeserver that delivered the record.

use anyhow::Result;
use nexus_common::models::user::RecheckOutcome;
use pubky::Keypair;

use crate::harness::{MigrationTest, Tree};

/// The mover wipes A after the copy and nexus reads the wipe before it
/// notices the switch: the file record and the stored file are removed. The
/// switch reads B, whose copy brings the record again, and the blob is
/// downloaded from B. One record, one stored file, before and after.
#[tokio_shared_rt::test(shared)]
async fn a_file_wiped_before_nexus_notices_is_downloaded_again_from_the_new_homeserver(
) -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let device_a = t.device()?;
    let session_a = t.signup(&device_a, &mover, &t.hs_a, "mover").await?;
    t.upload(&session_a, &mover, b"pixels").await?;
    t.ingest(&mover).await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.files(&mover).await?, (1, 1));

    let tree = t.public_tree(&device_a, &mover).await?;
    t.signup_and_copy(&mover, &t.hs_b, "mover", &device_a)
        .await?;
    t.delete_tree(&session_a, &tree).await?;
    t.index_external(&t.hs_a).await?;
    assert_eq!(t.files(&mover).await?, (0, 0), "the wipe was applied");

    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    t.index_external(&t.hs_b).await?;
    t.index_external(&t.hs_b).await?;
    assert_eq!(t.files(&mover).await?, (1, 1));
    Ok(())
}

/// A copy that writes the file records before the blobs, read by nexus in
/// between. The record arrives on B while its blob is not there yet, the
/// download answers 404, and a 404 is skipped, not retried: B's cursor moves
/// past the record. The blob arrives a moment later and nothing reads the
/// record again, so an upload the user's homeserver holds is never indexed.
/// Nexus had not read the upload from A either, or the record would already
/// be there. Demonstrated.
///
/// A record whose blob is missing on the homeserver that delivered it should
/// wait for the blob, as a reply waits for its parent.
#[tokio_shared_rt::test(shared)]
#[ignore = "a file record read before its blob is on the homeserver is skipped for good; \
            needs the record retried until the blob is there"]
async fn a_file_record_read_before_its_blob_is_indexed_once_the_blob_arrives() -> Result<()> {
    let t = MigrationTest::setup().await?;
    let mover = Keypair::random();

    let (device_a, session_a, _) = t.resident(&mover, &t.hs_a, "mover", &[]).await?;
    t.upload(&session_a, &mover, b"pixels").await?;

    let device_b = t.device()?;
    let session_b = t.signup(&device_b, &mover, &t.hs_b, "mover").await?;
    assert_eq!(t.recheck(&mover).await?.outcome, RecheckOutcome::Switched);
    let (records, rest): (Tree, Tree) = t
        .public_tree(&device_a, &mover)
        .await?
        .into_iter()
        .partition(|(path, _)| path.contains("/files/"));
    assert_eq!(records.len(), 1);
    t.write_tree(&session_b, &records).await?;
    t.index_external(&t.hs_b).await?;
    t.write_tree(&session_b, &rest).await?;
    t.index_external(&t.hs_b).await?;
    t.retry().await?;

    assert_eq!(t.files(&mover).await?, (1, 1));
    Ok(())
}
