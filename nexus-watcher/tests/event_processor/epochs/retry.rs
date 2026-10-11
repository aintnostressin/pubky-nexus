use crate::service::utils::{create_mock_handler, new_in_memory_store, TEST_USER_ID};
use anyhow::Result;
use chrono::Utc;
use nexus_common::config::EventRetryConfig;
use nexus_watcher::events::retry::{IndexKey, RetryEvent, RetryProcessor};
use nexus_watcher::events::EventType;
use nexus_watcher::service::TEventProcessor;
use pubky_social_specs::legacy_v0::post_uri_builder;
use std::sync::Arc;
use tokio::sync::watch;

const POST_ID: &str = "0032SSN7Q4EVG";

fn retry_event(event_uri: String, now: i64) -> RetryEvent {
    RetryEvent {
        retry_count: 0,
        event_type: EventType::Put,
        event_uri,
        next_retry_at: now - 1_000,
        origin_homeserver_id: "test_hs".to_string(),
        nonce: 1,
    }
}

/// A v1 event line in the retry store is re-parsed, skipped and removed: never handed to the
/// handler, never rescheduled, and no error. The v0 entry next to it shows the processor ran.
#[tokio_shared_rt::test(shared)]
async fn test_retry_v1_event_line_dropped() -> Result<()> {
    let v1_uri = format!("pubky://{TEST_USER_ID}/pub/social/v1/posts/{POST_ID}/{POST_ID}.json");
    let v0_uri = post_uri_builder(TEST_USER_ID.to_string(), POST_ID.to_string());

    let store = new_in_memory_store();
    let now = Utc::now().timestamp_millis();
    store.put(&retry_event(v1_uri.clone(), now)).await?;
    store.put(&retry_event(v0_uri.clone(), now)).await?;

    let handler = create_mock_handler(Ok(()), None);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let processor = Arc::new(RetryProcessor {
        event_handler: handler.clone(),
        shutdown_rx,
        config: EventRetryConfig::default(),
        store: store.clone(),
    });

    processor.run_internal().await?;

    assert_eq!(handler.get_handled_uris(), vec![v0_uri.clone()]);
    assert!(store.get(&IndexKey::for_uri(&v1_uri)).await?.is_none());
    assert!(store.get(&IndexKey::for_uri(&v0_uri)).await?.is_none());

    Ok(())
}
