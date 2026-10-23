use std::sync::{Arc, Mutex};

use anyhow::Result;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use nexus_common::models::error::{ModelError, ModelResult};
use nexus_common::models::user::UserIngestion;
use nexus_common::utils::test_utils::random_pubky_id;
use nexus_common::RateLimitConfig;
use nexus_webapi::media::test_utils::default_subprocess_tests;
use nexus_webapi::media::MediaPermits;
use nexus_webapi::routes::{app_routes, build_app, AppState};
use pubky_app_specs::PubkyId;
use tempfile::TempDir;
use tokio::sync::watch;
use tower::ServiceExt;

/// Records the users it's asked to ingest, refusing them as blacklisted if `refuse` is set.
#[derive(Debug, Default)]
struct RecordingIngestion {
    refuse: bool,
    asked: Mutex<Vec<PubkyId>>,
}

#[async_trait::async_trait]
impl UserIngestion for RecordingIngestion {
    async fn maybe_ingest_user(&self, user_id: &PubkyId) -> ModelResult<()> {
        self.asked.lock().unwrap().push(user_id.clone());
        if self.refuse {
            return Err(ModelError::HsBlacklisted {
                hs_id: random_pubky_id().to_string(),
            });
        }
        Ok(())
    }
}

/// Sends `PUT /v0/ingest/{user_id}` to an API that ingests through `ingestion`.
async fn put_ingest(ingestion: Arc<RecordingIngestion>, user_id: &PubkyId) -> Result<StatusCode> {
    let temp_dir = TempDir::new()?;
    let state = AppState::new(
        temp_dir.path().to_path_buf(),
        ingestion,
        MediaPermits::new(1),
        default_subprocess_tests(),
    );
    let (_tx, rx) = watch::channel(false);
    let routes = app_routes(state.clone(), &RateLimitConfig::default(), rx);
    let app = build_app(routes, state, 30, 1024);

    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("/v0/ingest/{user_id}"))
        .body(Body::empty())?;
    Ok(app.oneshot(req).await?.status())
}

/// The endpoint ingests through the implementation the API was given.
#[tokio::test]
async fn test_ingest_user_goes_through_the_given_ingestion() -> Result<()> {
    let ingestion = Arc::new(RecordingIngestion::default());
    let user_id = random_pubky_id();

    assert_eq!(
        put_ingest(ingestion.clone(), &user_id).await?,
        StatusCode::OK
    );
    assert_eq!(*ingestion.asked.lock().unwrap(), [user_id]);
    Ok(())
}

/// A user the ingestion refuses for a blacklisted HS gets a 403.
#[tokio::test]
async fn test_ingest_user_on_a_blacklisted_hs_is_forbidden() -> Result<()> {
    let ingestion = Arc::new(RecordingIngestion {
        refuse: true,
        ..Default::default()
    });

    let status = put_ingest(ingestion, &random_pubky_id()).await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}
