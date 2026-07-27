//! # Test Utilities
//!
//! Shared helpers for unit and integration tests.

use std::sync::Arc;

use pubky::{Keypair, PublicKey};
use pubky_app_specs::{
    PubkyAppCollectionContent, PubkyAppPost, PubkyAppPostEmbed, PubkyAppPostKind, PubkyAppUser,
    PubkyId,
};

use crate::models::user::UserIngestor;

/// Generates a random public key.
pub fn random_pk() -> PublicKey {
    Keypair::random().public_key()
}

/// Generates a random z32-encoded public key, usable as a user or HS ID.
pub fn random_pubky_id() -> PubkyId {
    PubkyId::from(random_pk())
}

/// Default user ingestor for tests: empty HS blacklist (ingest everything).
pub fn default_ingestor_tests() -> Arc<UserIngestor> {
    Arc::new(UserIngestor::default())
}

/// A `PubkyAppUser` with name and bio; image/links/status unset.
pub fn test_user(name: impl Into<String>, bio: impl Into<String>) -> PubkyAppUser {
    PubkyAppUser::new(name.into(), Some(bio.into()), None, None, None)
}

/// A `Short` root post.
pub fn short_post(content: impl Into<String>) -> PubkyAppPost {
    PubkyAppPost::new(content.into(), PubkyAppPostKind::Short, None, None, None)
}

/// A `Long` (article) root post.
pub fn long_post(content: impl Into<String>) -> PubkyAppPost {
    PubkyAppPost::new(content.into(), PubkyAppPostKind::Long, None, None, None)
}

/// A `Short` reply to `parent_uri`.
pub fn short_reply(content: impl Into<String>, parent_uri: String) -> PubkyAppPost {
    PubkyAppPost::new(content.into(), PubkyAppPostKind::Short, Some(parent_uri), None, None)
}

/// A `Short` repost: a post whose embed points at `parent_uri`.
pub fn short_repost(content: impl Into<String>, parent_uri: String) -> PubkyAppPost {
    let embed = PubkyAppPostEmbed {
        kind: PubkyAppPostKind::Short,
        uri: parent_uri,
    };
    PubkyAppPost::new(content.into(), PubkyAppPostKind::Short, None, Some(embed), None)
}

/// A `Short` post locked behind `lock_uri`, which must be a valid `pubky://` URL.
pub fn locked_post(content: impl Into<String>, lock_uri: String) -> PubkyAppPost {
    PubkyAppPost::new_with_lock(
        content.into(),
        PubkyAppPostKind::Short,
        None,
        None,
        None,
        Some(lock_uri),
    )
}

/// A `Collection` post with the given name and no items.
pub fn collection_post(name: impl Into<String>) -> PubkyAppPost {
    let content = serde_json::to_string(&PubkyAppCollectionContent {
        name: name.into(),
        ..Default::default()
    })
    .expect("collection envelope serializes");
    PubkyAppPost::new(content, PubkyAppPostKind::Collection, None, None, None)
}
