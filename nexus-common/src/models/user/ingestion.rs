use pubky_app_specs::PubkyId;

use crate::models::error::ModelResult;

/// Ingests users Nexus doesn't know yet.
///
/// The watcher implements it. The API takes an implementation for its ingest-user endpoint,
/// so it ingests the way the watcher does without depending on it.
#[async_trait::async_trait]
pub trait UserIngestion: Send + Sync + std::fmt::Debug {
    /// Resolves and persists `user_id`, if unknown.
    ///
    /// Returns [`ModelError::HsBlacklisted`](crate::models::error::ModelError::HsBlacklisted)
    /// if the user's resolved HS is blacklisted.
    async fn maybe_ingest_user(&self, user_id: &PubkyId) -> ModelResult<()>;
}
