//! The `pubky.app` arm: objects read with the frozen v0 reader and copied into the handler inputs,
//! ids taken from the v0 path.

use super::{SkipReason, TranslatedDel, TranslatedPut};
use crate::errors::EventProcessorError;
use pubky_social_specs::legacy_v0::{ExtendedParsedUri, PubkyAppObject, Resource};

pub(super) fn translate_put(
    parsed: &ExtendedParsedUri,
    uri: &str,
    bytes: &[u8],
) -> Result<TranslatedPut, EventProcessorError> {
    let resource = parsed.resource();

    // `from_resource` runs spec validation; failures are deterministic and must
    // not be retried (a re-run produces the same error). Classify them as
    // `SpecValidation` so the retry queue stays clean — the load-bearing
    // counterpart to the `Unknown` forwards-compat variant in the v0 reader.
    let object = PubkyAppObject::from_resource(resource, bytes)
        .map_err(|e| EventProcessorError::SpecValidation(e.to_string()))?;

    let user_id = parsed.user_id().clone();
    let translated = match (object, resource) {
        (PubkyAppObject::User(user), Resource::User) => TranslatedPut::PutUser {
            user_id,
            user: user.into(),
        },
        (PubkyAppObject::Post(post), Resource::Post(post_id)) => TranslatedPut::PutPost {
            author_id: user_id,
            post_id: post_id.clone(),
            post: post.into(),
        },
        (PubkyAppObject::Follow(_), Resource::Follow(followee_id)) => TranslatedPut::PutFollow {
            user_id,
            followee_id: followee_id.clone(),
        },
        (PubkyAppObject::Tag(tag), Resource::Tag(tag_id)) => TranslatedPut::PutTag {
            tagger_id: user_id,
            tag_id: tag_id.clone(),
            tag: tag.into(),
            app: match parsed {
                ExtendedParsedUri::UniversalTag { app, .. } => Some(app.clone()),
                ExtendedParsedUri::PubkyApp { .. } => None,
            },
        },
        (PubkyAppObject::Bookmark(bookmark), Resource::Bookmark(bookmark_id)) => {
            TranslatedPut::PutBookmark {
                user_id,
                bookmark_id: bookmark_id.clone(),
                bookmark: bookmark.into(),
            }
        }
        (PubkyAppObject::File(file), Resource::File(file_id)) => TranslatedPut::PutFile {
            user_id,
            file_id: file_id.clone(),
            file: file.into(),
            uri: uri.to_string(),
        },
        (PubkyAppObject::Mute(_), _) => skip_put(SkipReason::Mute),
        (PubkyAppObject::LastRead(_), _) => skip_put(SkipReason::LastRead),
        (PubkyAppObject::Feed(_), _) => skip_put(SkipReason::Feed),
        (PubkyAppObject::Blob(_), _) => skip_put(SkipReason::Blob),
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
) -> Result<TranslatedDel, EventProcessorError> {
    let user_id = parsed.user_id().clone();
    let translated = match parsed.resource() {
        Resource::User => TranslatedDel::DelUser { user_id },
        Resource::Post(post_id) => TranslatedDel::DelPost {
            author_id: user_id,
            post_id: post_id.clone(),
        },
        Resource::Follow(followee_id) => TranslatedDel::DelFollow {
            user_id,
            followee_id: followee_id.clone(),
        },
        Resource::Tag(_) => TranslatedDel::DelTag {
            uri: uri.to_string(),
        },
        Resource::Bookmark(bookmark_id) => TranslatedDel::DelBookmark {
            user_id,
            bookmark_id: bookmark_id.clone(),
        },
        Resource::File(file_id) => TranslatedDel::DelFile {
            user_id,
            file_id: file_id.clone(),
        },
        Resource::Mute(_) => skip_del(SkipReason::Mute),
        Resource::LastRead => skip_del(SkipReason::LastRead),
        Resource::Feed(_) => skip_del(SkipReason::Feed),
        Resource::Blob(_) => skip_del(SkipReason::Blob),
        Resource::Unknown => {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "Unknown resource in URI: {uri}"
            )))
        }
    };
    Ok(translated)
}

fn skip_put(reason: SkipReason) -> TranslatedPut {
    TranslatedPut::Skip { reason }
}

fn skip_del(reason: SkipReason) -> TranslatedDel {
    TranslatedDel::Skip { reason }
}
