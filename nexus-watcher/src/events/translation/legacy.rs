//! The `pubky.app` arm: objects read with the frozen v0 reader and copied into the handler inputs,
//! ids taken from the v0 path.

use super::{SkipReason, Translated};
use crate::errors::EventProcessorError;
use pubky_social_specs::legacy_v0::{ExtendedParsedUri, PubkyAppObject, Resource};

pub(super) fn translate_put(
    parsed: &ExtendedParsedUri,
    uri: &str,
    bytes: &[u8],
) -> Result<Translated, EventProcessorError> {
    let resource = parsed.resource();

    // `from_resource` runs spec validation; failures are deterministic and must
    // not be retried (a re-run produces the same error). Classify them as
    // `SpecValidation` so the retry queue stays clean — the load-bearing
    // counterpart to the `Unknown` forwards-compat variant in the v0 reader.
    let object = PubkyAppObject::from_resource(resource, bytes)
        .map_err(|e| EventProcessorError::SpecValidation(e.to_string()))?;

    let user_id = parsed.user_id().clone();
    let translated = match (object, resource) {
        (PubkyAppObject::User(user), Resource::User) => Translated::PutUser {
            user_id,
            user: user.into(),
        },
        (PubkyAppObject::Post(post), Resource::Post(post_id)) => Translated::PutPost {
            author_id: user_id,
            post_id: post_id.clone(),
            post: post.into(),
        },
        (PubkyAppObject::Follow(_), Resource::Follow(followee_id)) => Translated::PutFollow {
            user_id,
            followee_id: followee_id.clone(),
        },
        (PubkyAppObject::Tag(tag), Resource::Tag(tag_id)) => Translated::PutTag {
            tagger_id: user_id,
            tag_id: tag_id.clone(),
            tag: tag.into(),
            app: match parsed {
                ExtendedParsedUri::UniversalTag { app, .. } => Some(app.clone()),
                ExtendedParsedUri::PubkyApp { .. } => None,
            },
        },
        (PubkyAppObject::Bookmark(bookmark), Resource::Bookmark(bookmark_id)) => {
            Translated::PutBookmark {
                user_id,
                bookmark_id: bookmark_id.clone(),
                bookmark: bookmark.into(),
            }
        }
        (PubkyAppObject::File(file), Resource::File(file_id)) => Translated::PutFile {
            user_id,
            file_id: file_id.clone(),
            file: file.into(),
            uri: uri.to_string(),
        },
        (PubkyAppObject::Mute(_), _) => skip(SkipReason::Mute),
        (PubkyAppObject::LastRead(_), _) => skip(SkipReason::LastRead),
        (PubkyAppObject::Feed(_), _) => skip(SkipReason::Feed),
        (PubkyAppObject::Blob(_), _) => skip(SkipReason::Blob),
        (_, resource) => {
            return Err(EventProcessorError::internal_error(format!(
                "The v0 reader returned another object kind for a {resource} resource: {uri}"
            )))
        }
    };
    Ok(translated)
}

pub(super) fn translate_del(
    parsed: &ExtendedParsedUri,
    uri: &str,
) -> Result<Translated, EventProcessorError> {
    let user_id = parsed.user_id().clone();
    let translated = match parsed.resource() {
        Resource::User => Translated::DelUser { user_id },
        Resource::Post(post_id) => Translated::DelPost {
            author_id: user_id,
            post_id: post_id.clone(),
        },
        Resource::Follow(followee_id) => Translated::DelFollow {
            user_id,
            followee_id: followee_id.clone(),
        },
        Resource::Tag(_) => Translated::DelTag {
            uri: uri.to_string(),
        },
        Resource::Bookmark(bookmark_id) => Translated::DelBookmark {
            user_id,
            bookmark_id: bookmark_id.clone(),
        },
        Resource::File(file_id) => Translated::DelFile {
            user_id,
            file_id: file_id.clone(),
        },
        Resource::Mute(_) => skip(SkipReason::Mute),
        Resource::LastRead => skip(SkipReason::LastRead),
        Resource::Feed(_) => skip(SkipReason::Feed),
        Resource::Blob(_) => skip(SkipReason::Blob),
        Resource::Unknown => {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "Unknown resource in URI: {uri}"
            )))
        }
    };
    Ok(translated)
}

fn skip(reason: SkipReason) -> Translated {
    Translated::Skip { reason }
}
