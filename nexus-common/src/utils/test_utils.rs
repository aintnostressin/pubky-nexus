//! # Test Utilities
//!
//! Shared helpers for unit and integration tests.

use pubky::{Keypair, PublicKey};
use pubky_app_specs::PubkyId;

use crate::models::error::ModelResult;
use crate::models::user::UserIngestion;

/// Generates a random public key.
pub fn random_pk() -> PublicKey {
    Keypair::random().public_key()
}

/// Generates a random z32-encoded public key, usable as a user or HS ID.
pub fn random_pubky_id() -> PubkyId {
    PubkyId::from(random_pk())
}

/// Ingests nobody, for API tests: they don't ingest. The watcher implements ingestion and
/// tests it.
#[derive(Debug)]
pub struct NoIngestion;

#[async_trait::async_trait]
impl UserIngestion for NoIngestion {
    async fn maybe_ingest_user(&self, _user_id: &PubkyId) -> ModelResult<()> {
        Ok(())
    }
}
