//! The `pubky.app` arm: the v0 resources skipped before any fetch, the canonical v0 addresses,
//! objects read with the frozen v0 reader and copied into the handler inputs by the conversions
//! below, ids taken from the v0 path.

use super::{resolve_reference, SkipReason, TranslatedDel, TranslatedPut};
use crate::errors::EventProcessorError;
use crate::events::handlers::{BookmarkInput, FileInput, PostInput, TagInput, Target, UserInput};
use pubky_social_specs::legacy_v0::{
    ExtendedParsedUri, PubkyAppBookmark, PubkyAppFile, PubkyAppObject, PubkyAppPost, PubkyAppTag,
    PubkyAppUser, Resource,
};

pub(super) fn skip_reason(
    parsed: &ExtendedParsedUri,
    uri: &str,
) -> Result<Option<SkipReason>, EventProcessorError> {
    match parsed {
        ExtendedParsedUri::PubkyApp { resource, .. } => match resource {
            Resource::Unknown => Err(unknown_resource(uri)),
            Resource::Mute(_) => Ok(Some(SkipReason::Mute)),
            Resource::LastRead => Ok(Some(SkipReason::LastRead)),
            Resource::Feed(_) => Ok(Some(SkipReason::Feed)),
            Resource::Blob(_) => Ok(Some(SkipReason::Blob)),
            _ => Ok(None),
        },
        ExtendedParsedUri::UniversalTag { .. } => Ok(None),
    }
}

/// The v0 parser ignores segments past the resource id, so `posts/ID/extra` reads as `posts/ID`.
/// Only the address the specs render for the parsed resource is canonical.
pub(super) fn is_canonical(parsed: &ExtendedParsedUri, uri: &str) -> bool {
    parsed
        .try_to_uri_str()
        .is_ok_and(|canonical| canonical == uri)
}

/// What a v0 URI names.
pub(super) fn target(parsed: &ExtendedParsedUri) -> Target {
    match parsed {
        ExtendedParsedUri::PubkyApp { user_id, resource } => {
            let owner_id = user_id.clone();
            match resource {
                Resource::Post(id) => Target::Post {
                    author_id: owner_id,
                    post_id: id.clone(),
                },
                Resource::User => Target::User(owner_id),
                Resource::File(id) => Target::File {
                    owner_id,
                    file_id: id.clone(),
                },
                Resource::Blob(id) => Target::Blob {
                    owner_id,
                    blob_id: id.clone(),
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
                | Resource::LastRead
                | Resource::Unknown => Target::Unkeyed { owner_id },
            }
        }
        ExtendedParsedUri::UniversalTag {
            user_id,
            app,
            resource,
        } => match resource {
            Resource::Tag(id) => Target::Tag {
                tagger_id: user_id.clone(),
                tag_id: id.clone(),
                app: Some(app.clone()),
            },
            _ => Target::External,
        },
    }
}

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
        (PubkyAppObject::User(user), Resource::User) => TranslatedPut::User {
            user_id,
            user: user.into(),
        },
        (PubkyAppObject::Post(post), Resource::Post(post_id)) => TranslatedPut::Post {
            author_id: user_id,
            post_id: post_id.clone(),
            post: post.into(),
        },
        (PubkyAppObject::Follow(_), Resource::Follow(followee_id)) => TranslatedPut::Follow {
            user_id,
            followee_id: followee_id.clone(),
        },
        (PubkyAppObject::Tag(tag), Resource::Tag(tag_id)) => TranslatedPut::Tag {
            tagger_id: user_id,
            tag_id: tag_id.clone(),
            tag: tag.into(),
            app: match parsed {
                ExtendedParsedUri::UniversalTag { app, .. } => Some(app.clone()),
                ExtendedParsedUri::PubkyApp { .. } => None,
            },
        },
        (PubkyAppObject::Bookmark(bookmark), Resource::Bookmark(bookmark_id)) => {
            TranslatedPut::Bookmark {
                user_id,
                bookmark_id: bookmark_id.clone(),
                bookmark: bookmark.into(),
            }
        }
        (PubkyAppObject::File(file), Resource::File(file_id)) => TranslatedPut::File {
            user_id,
            file_id: file_id.clone(),
            file: file.into(),
            uri: uri.to_string(),
        },
        (PubkyAppObject::Mute(_), _) => TranslatedPut::Skip {
            reason: SkipReason::Mute,
        },
        (PubkyAppObject::LastRead(_), _) => TranslatedPut::Skip {
            reason: SkipReason::LastRead,
        },
        (PubkyAppObject::Feed(_), _) => TranslatedPut::Skip {
            reason: SkipReason::Feed,
        },
        (PubkyAppObject::Blob(_), _) => TranslatedPut::Skip {
            reason: SkipReason::Blob,
        },
        (_, resource) => {
            return Err(EventProcessorError::SpecValidation(format!(
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
        Resource::User => TranslatedDel::User { user_id },
        Resource::Post(post_id) => TranslatedDel::Post {
            author_id: user_id,
            post_id: post_id.clone(),
        },
        Resource::Follow(followee_id) => TranslatedDel::Follow {
            user_id,
            followee_id: followee_id.clone(),
        },
        Resource::Tag(tag_id) => TranslatedDel::Tag {
            tagger_id: user_id,
            tag_id: tag_id.clone(),
            app: match parsed {
                ExtendedParsedUri::UniversalTag { app, .. } => Some(app.clone()),
                ExtendedParsedUri::PubkyApp { .. } => None,
            },
        },
        Resource::Bookmark(bookmark_id) => TranslatedDel::Bookmark {
            user_id,
            bookmark_id: bookmark_id.clone(),
        },
        Resource::File(file_id) => TranslatedDel::File {
            user_id,
            file_id: file_id.clone(),
        },
        Resource::Mute(_) => TranslatedDel::Skip {
            reason: SkipReason::Mute,
        },
        Resource::LastRead => TranslatedDel::Skip {
            reason: SkipReason::LastRead,
        },
        Resource::Feed(_) => TranslatedDel::Skip {
            reason: SkipReason::Feed,
        },
        Resource::Blob(_) => TranslatedDel::Skip {
            reason: SkipReason::Blob,
        },
        Resource::Unknown => return Err(unknown_resource(uri)),
    };
    Ok(translated)
}

/// Reads the bytes of the v0 blob `blob_id` with the frozen reader, which checks them against the
/// id and the size limit.
pub(super) fn read_blob(blob_id: &str, bytes: &[u8]) -> Result<Vec<u8>, EventProcessorError> {
    match PubkyAppObject::from_resource(&Resource::Blob(blob_id.to_string()), bytes) {
        Ok(PubkyAppObject::Blob(blob)) => Ok(blob.0),
        Ok(_) => Err(EventProcessorError::SpecValidation(format!(
            "The v0 reader returned another object kind for blob {blob_id}"
        ))),
        Err(e) => Err(EventProcessorError::SpecValidation(e.to_string())),
    }
}

fn unknown_resource(uri: &str) -> EventProcessorError {
    EventProcessorError::InvalidEventLine(format!("Unknown resource in URI: {uri}"))
}

impl From<PubkyAppPost> for PostInput {
    fn from(post: PubkyAppPost) -> Self {
        Self {
            content: post.content,
            kind: post.kind.into(),
            parent: post.parent.as_deref().map(resolve_reference),
            embed: post.embed.map(|embed| resolve_reference(&embed.uri)),
            attachments: post.attachments,
            lock: post.lock,
        }
    }
}

impl From<PubkyAppBookmark> for BookmarkInput {
    fn from(bookmark: PubkyAppBookmark) -> Self {
        Self {
            target: resolve_reference(&bookmark.uri),
        }
    }
}

impl From<PubkyAppUser> for UserInput {
    fn from(user: PubkyAppUser) -> Self {
        Self {
            name: user.name,
            bio: user.bio,
            image: user.image,
            links: user
                .links
                .map(|links| links.into_iter().map(Into::into).collect()),
            status: user.status,
        }
    }
}

impl From<PubkyAppTag> for TagInput {
    fn from(tag: PubkyAppTag) -> Self {
        Self {
            target: resolve_reference(&tag.uri),
            label: tag.label,
        }
    }
}

impl From<PubkyAppFile> for FileInput {
    fn from(file: PubkyAppFile) -> Self {
        Self {
            name: file.name,
            src: resolve_reference(&file.src),
            content_type: file.content_type,
            size: file.size,
            created_at: file.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::handlers::Reference;
    use nexus_common::models::post::PostKind;
    use pubky_social_specs::legacy_v0::{
        blob_uri_builder, file_uri_builder, post_uri_builder, PubkyAppPostEmbed, PubkyAppPostKind,
        PubkyAppUserLink,
    };
    use pubky_social_specs::PubkyId;

    const AUTHOR: &str = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    const POST_ID: &str = "003286NSMY490";

    fn uri_of_post(id: &str) -> String {
        post_uri_builder(AUTHOR.into(), id.into())
    }

    /// The parent and the embed resolve to the post they name, keeping the URI as written.
    fn post_reference(id: &str) -> Reference {
        Reference {
            uri: uri_of_post(id),
            target: Target::Post {
                author_id: PubkyId::try_from(AUTHOR).unwrap(),
                post_id: id.to_string(),
            },
        }
    }

    #[test]
    fn post_from_legacy_copies_every_field() {
        let attachment = file_uri_builder(AUTHOR.into(), "003286NSMY493".into());
        let post = PostInput::from(PubkyAppPost {
            content: "Hello".into(),
            kind: PubkyAppPostKind::Long,
            parent: Some(uri_of_post("003286NSMY491")),
            embed: Some(PubkyAppPostEmbed {
                kind: PubkyAppPostKind::Short,
                uri: uri_of_post("003286NSMY492"),
            }),
            attachments: Some(vec![attachment.clone()]),
            lock: Some("pubky://lockserver/pub/lock".into()),
        });

        assert_eq!(post.content, "Hello");
        assert_eq!(post.kind, PostKind::Long);
        assert_eq!(post.parent, Some(post_reference("003286NSMY491")));
        assert_eq!(post.embed, Some(post_reference("003286NSMY492")));
        assert_eq!(post.attachments, Some(vec![attachment]));
        assert_eq!(post.lock.as_deref(), Some("pubky://lockserver/pub/lock"));
    }

    /// The edit check compares `PostDetails.attachments`, so a missing list must stay missing.
    #[test]
    fn post_without_attachments_keeps_none() {
        let post = PostInput::from(PubkyAppPost {
            content: "x".into(),
            attachments: None,
            ..Default::default()
        });
        assert_eq!(post.attachments, None);
    }

    #[test]
    fn bookmark_from_legacy_copies_the_target() {
        let bookmark = BookmarkInput::from(PubkyAppBookmark {
            uri: uri_of_post(POST_ID),
            created_at: 1,
        });
        assert_eq!(bookmark.target, post_reference(POST_ID));
    }

    #[test]
    fn user_from_legacy_copies_every_field() {
        let user = UserInput::from(PubkyAppUser {
            name: "Alice".into(),
            bio: Some("Hi".into()),
            image: Some(file_uri_builder(AUTHOR.into(), POST_ID.into())),
            links: Some(vec![PubkyAppUserLink {
                title: "Website".into(),
                url: "https://example.com".into(),
            }]),
            status: Some("Away".into()),
        });

        assert_eq!(user.name, "Alice");
        assert_eq!(user.bio.as_deref(), Some("Hi"));
        assert_eq!(
            user.image,
            Some(file_uri_builder(AUTHOR.into(), POST_ID.into()))
        );
        let links = user.links.expect("links are copied");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].title, "Website");
        assert_eq!(links[0].url, "https://example.com");
        assert_eq!(user.status.as_deref(), Some("Away"));
    }

    #[test]
    fn tag_from_legacy_copies_target_and_label() {
        let tag = TagInput::from(PubkyAppTag {
            uri: uri_of_post(POST_ID),
            label: "rust".into(),
            created_at: 1,
        });
        assert_eq!(tag.target, post_reference(POST_ID));
        assert_eq!(tag.label, "rust");
    }

    #[test]
    fn file_from_legacy_copies_every_field() {
        let src = blob_uri_builder(AUTHOR.into(), "8Z8CWH8NVYQY39ZEBFGKQWWEKG".into());
        let file = FileInput::from(PubkyAppFile {
            name: "a.png".into(),
            created_at: 1,
            src: src.clone(),
            content_type: "image/png".into(),
            size: 3,
        });

        assert_eq!(file.name, "a.png");
        assert_eq!(
            file.src,
            Reference {
                uri: src,
                target: Target::Blob {
                    owner_id: PubkyId::try_from(AUTHOR).unwrap(),
                    blob_id: "8Z8CWH8NVYQY39ZEBFGKQWWEKG".into(),
                },
            }
        );
        assert_eq!(file.content_type, "image/png");
        assert_eq!(file.size, 3);
        assert_eq!(file.created_at, 1);
    }
}
