//! The `social` arm. No `social` epoch is indexed yet, so it only reads references: what a v1 URI
//! names.

use crate::events::handlers::Target;
use pubky_social_specs::{ParsedUri, Resource};

/// What a v1 URI names.
pub(super) fn target(parsed: &ParsedUri) -> Target {
    let owner_id = parsed.user_id.clone();
    match &parsed.resource {
        // A version file and the versionless reference name the same post.
        Resource::Post { id, .. } => Target::Post {
            author_id: owner_id,
            post_id: id.clone(),
        },
        Resource::User => Target::User(owner_id),
        Resource::File(_) => Target::File {
            owner_id,
            file_id: parsed.resource.id().unwrap_or_default(),
        },
        Resource::Tag(id) => Target::Tag {
            tagger_id: owner_id,
            tag_id: id.clone(),
            app: None,
        },
        Resource::Follow(_)
        | Resource::Mute(_)
        | Resource::Bookmark(_)
        | Resource::Feed(_)
        | Resource::Unknown => Target::Unkeyed { owner_id },
        // Another namespace or epoch, or a resource kind this build does not know
        _ => Target::External,
    }
}
