use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::event::EventLine;
use nexus_watcher::events::retry::{IndexKey, RetryEvent};
use pubky::{Keypair, ResourcePath};
use pubky_social_specs::legacy_v0::PubkyAppUser;

/// The URI the homeserver's event line carries for a write at `path`.
pub fn event_uri(user_id: &str, path: &ResourcePath) -> String {
    format!("pubky://{user_id}{path}")
}

/// A user Nexus knows from a v0 profile, so the v0 counterpart of a skipped object would index.
pub async fn create_v0_user(test: &mut WatcherTest, name: &str) -> Result<(Keypair, String)> {
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;
    Ok((user_kp, user_id))
}

/// Asserts that the event for `uri` left no trace: no retry entry and no stored event line.
pub async fn assert_no_trace(uri: &str) -> Result<()> {
    let queued = RetryEvent::check_index_key(&IndexKey::for_uri(uri)).await?;
    assert!(!queued, "A retry entry exists for {uri}");

    // Uncapped: the shared event log is append-only and unbounded.
    let (lines, _) = EventLine::get_from_index(None, usize::MAX).await?;
    let suffix = format!(" {uri}");
    assert!(
        !lines.iter().any(|line| line.ends_with(&suffix)),
        "An event line is stored for {uri}"
    );
    Ok(())
}
