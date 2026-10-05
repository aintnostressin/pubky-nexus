//! The explicit layer between an event and its handler: [`route`] classifies the URI by epoch,
//! [`skip_reason`] picks the routed events parsing skips before any fetch, and [`translate_put`]
//! and [`translate_del`] turn a routed event into the handler call that indexes it. Pure
//! functions: no I/O, no database, no homeserver.

mod legacy;
mod route;
#[cfg(test)]
pub(super) mod test_support;

pub use route::{route, EventRoute, UNKNOWN_LABEL};

use crate::errors::EventProcessorError;
use crate::events::handlers::{BookmarkInput, FileInput, PostInput, TagInput, UserInput};
use pubky_social_specs::PubkyId;

/// Why an event is deliberately left unhandled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// A v0 last-read marker, which Nexus does not index.
    LastRead,
    /// A v0 feed, which Nexus does not index.
    Feed,
    /// A v0 blob, read only through the file that points at it.
    Blob,
    /// A v0 mute, no longer handled by Nexus.
    Mute,
    /// A path in a `social` epoch Nexus reads but does not index yet.
    EpochNotIndexed { epoch: u8 },
    /// A `social/vN` path in an epoch this build does not speak.
    UnsupportedEpoch { version: String },
}

/// A PUT's handler call, carrying the inputs that handler takes, whichever epoch wrote the object.
/// Apart from [`TranslatedDel`] so a PUT cannot translate to a deletion.
#[derive(Debug)]
pub enum TranslatedPut {
    User {
        user_id: PubkyId,
        user: UserInput,
    },
    Post {
        author_id: PubkyId,
        post_id: String,
        post: PostInput,
    },
    Follow {
        user_id: PubkyId,
        followee_id: PubkyId,
    },
    /// `app` is set for a universal tag written under another app's namespace.
    Tag {
        tagger_id: PubkyId,
        tag_id: String,
        tag: TagInput,
        app: Option<String>,
    },
    Bookmark {
        user_id: PubkyId,
        bookmark_id: String,
        bookmark: BookmarkInput,
    },
    File {
        user_id: PubkyId,
        file_id: String,
        file: FileInput,
        uri: String,
    },
    Skip {
        reason: SkipReason,
    },
}

/// A DEL's handler call, from the ids in the path.
/// Apart from [`TranslatedPut`] so a DEL cannot translate to a write.
#[derive(Debug)]
pub enum TranslatedDel {
    User {
        user_id: PubkyId,
    },
    Post {
        author_id: PubkyId,
        post_id: String,
    },
    Follow {
        user_id: PubkyId,
        followee_id: PubkyId,
    },
    Tag {
        uri: String,
    },
    Bookmark {
        user_id: PubkyId,
        bookmark_id: String,
    },
    File {
        user_id: PubkyId,
        file_id: String,
    },
    Skip {
        reason: SkipReason,
    },
}

/// Whether parsing skips the event at `uri` before any fetch, and why. A `pubky.app` path naming no
/// resource is an error rather than a skip.
pub fn skip_reason(
    route: &EventRoute,
    uri: &str,
) -> Result<Option<SkipReason>, EventProcessorError> {
    match route {
        EventRoute::Legacy(parsed) => legacy::skip_reason(parsed, uri),
        // The social epochs are skipped before any fetch, so no GET is spent on them and the
        // retry queue never holds one
        EventRoute::Social { epoch, .. } => Ok(Some(SkipReason::EpochNotIndexed { epoch: *epoch })),
        EventRoute::UnsupportedEpoch { version } => Ok(Some(SkipReason::UnsupportedEpoch {
            version: version.clone(),
        })),
        EventRoute::Malformed { .. } => Ok(None),
    }
}

/// Reads the fetched `bytes` of a PUT with its epoch's reader. `uri` is the event URI as received,
/// which the file handler stores.
pub fn translate_put(
    route: &EventRoute,
    uri: &str,
    bytes: &[u8],
) -> Result<TranslatedPut, EventProcessorError> {
    match route {
        EventRoute::Legacy(parsed) => legacy::translate_put(parsed, uri, bytes),
        EventRoute::Social { .. }
        | EventRoute::UnsupportedEpoch { .. }
        | EventRoute::Malformed { .. } => Err(untranslatable_route_error(uri)),
    }
}

/// Maps a DEL to its handler call from the ids in the path. `uri` is the event URI as received,
/// which the tag handler takes.
pub fn translate_del(route: &EventRoute, uri: &str) -> Result<TranslatedDel, EventProcessorError> {
    match route {
        EventRoute::Legacy(parsed) => legacy::translate_del(parsed, uri),
        EventRoute::Social { .. }
        | EventRoute::UnsupportedEpoch { .. }
        | EventRoute::Malformed { .. } => Err(untranslatable_route_error(uri)),
    }
}

/// Parsing skips or rejects every route but `Legacy` before an event exists, so this is a bug.
fn untranslatable_route_error(uri: &str) -> EventProcessorError {
    EventProcessorError::internal_error(format!("No translation for the route of {uri}"))
}

#[cfg(test)]
mod tests {
    use super::test_support::{host, other, uri, HASH, OTHER, TS};
    use super::*;
    use base32::{encode, Alphabet};
    use chrono::Utc;
    use pubky_social_specs::legacy_v0::traits::HashId;
    use pubky_social_specs::legacy_v0::{
        tag_id, PubkyAppBlob, PubkyAppBookmark, PubkyAppFeed, PubkyAppFeedConfig,
        PubkyAppFeedLayout, PubkyAppFeedReach, PubkyAppFeedSort,
    };

    /// A timestamp id for now: the frozen reader bounds these by the clock.
    fn now_id() -> String {
        encode(
            Alphabet::Crockford,
            &Utc::now().timestamp_micros().to_be_bytes(),
        )
    }

    fn skip(path: &str) -> Option<SkipReason> {
        let uri = uri(path);
        skip_reason(&route(&uri), &uri).unwrap_or_else(|e| panic!("{path} should be decided: {e}"))
    }

    fn put(path: &str, json: &str) -> TranslatedPut {
        let uri = uri(path);
        translate_put(&route(&uri), &uri, json.as_bytes())
            .unwrap_or_else(|e| panic!("{path} should translate: {e}"))
    }

    fn del(path: &str) -> TranslatedDel {
        let uri = uri(path);
        translate_del(&route(&uri), &uri).unwrap_or_else(|e| panic!("{path} should translate: {e}"))
    }

    #[test]
    fn skip_reason_names_the_routes_skipped_before_any_fetch() {
        let cases = [
            ("pub/pubky.app/last_read".to_string(), SkipReason::LastRead),
            (format!("pub/pubky.app/feeds/{HASH}"), SkipReason::Feed),
            (format!("pub/pubky.app/blobs/{HASH}"), SkipReason::Blob),
            (
                format!("pub/social/v1/posts/{TS}/{TS}.json"),
                SkipReason::EpochNotIndexed { epoch: 1 },
            ),
            (
                "pub/social/v1/widgets/ABC".to_string(),
                SkipReason::EpochNotIndexed { epoch: 1 },
            ),
            (
                format!("pub/social/v2/posts/{TS}"),
                SkipReason::UnsupportedEpoch {
                    version: "v2".into(),
                },
            ),
        ];
        for (path, expected) in cases {
            assert_eq!(skip(&path), Some(expected), "{path}");
        }
    }

    /// The other v0 resources are fetched first, and parsing rejects a malformed route instead.
    #[test]
    fn skip_reason_lets_the_other_routes_through() {
        for path in [
            "pub/pubky.app/profile.json".to_string(),
            format!("pub/pubky.app/posts/{TS}"),
            format!("pub/pubky.app/follows/{OTHER}"),
            format!("pub/pubky.app/mutes/{OTHER}"),
            format!("pub/pubky.app/bookmarks/{HASH}"),
            format!("pub/pubky.app/tags/{HASH}"),
            format!("pub/pubky.app/files/{TS}"),
            "pub/mapky/tags/ABC123".to_string(),
            "pub/social/tags/ABC".to_string(),
        ] {
            assert_eq!(skip(&path), None, "{path}");
        }
    }

    /// Parsing and the DEL translation reject it with the same error.
    #[test]
    fn unknown_v0_resource_is_an_invalid_event_line() {
        let uri = uri("pub/pubky.app/widgets/ABC");
        let route = route(&uri);
        for err in [
            skip_reason(&route, &uri).unwrap_err(),
            translate_del(&route, &uri).unwrap_err(),
        ] {
            match err {
                EventProcessorError::InvalidEventLine(message) => {
                    assert_eq!(message, format!("Unknown resource in URI: {uri}"))
                }
                other => panic!("expected InvalidEventLine, got {other:?}"),
            }
        }
    }

    #[test]
    fn put_user() {
        let json = r#"{"name":"Alice","bio":"Hi","image":null,"links":null,"status":null}"#;
        match put("pub/pubky.app/profile.json", json) {
            TranslatedPut::User { user_id, user } => {
                assert_eq!(user_id, host());
                assert_eq!(user.name, "Alice");
            }
            other => panic!("expected User, got {other:?}"),
        }
    }

    #[test]
    fn put_post() {
        let post_id = now_id();
        let json =
            r#"{"content":"Hello","kind":"short","parent":null,"embed":null,"attachments":null}"#;
        match put(&format!("pub/pubky.app/posts/{post_id}"), json) {
            TranslatedPut::Post {
                author_id,
                post_id: id,
                post,
            } => {
                assert_eq!(author_id, host());
                assert_eq!(id, post_id);
                assert_eq!(post.content, "Hello");
            }
            other => panic!("expected Post, got {other:?}"),
        }
    }

    #[test]
    fn put_follow() {
        match put(
            &format!("pub/pubky.app/follows/{OTHER}"),
            r#"{"created_at":1}"#,
        ) {
            TranslatedPut::Follow {
                user_id,
                followee_id,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(followee_id, other());
            }
            other => panic!("expected Follow, got {other:?}"),
        }
    }

    #[test]
    fn put_tag() {
        let target = uri(&format!("pub/pubky.app/posts/{TS}"));
        let json = format!(r#"{{"uri":"{target}","label":"rust","created_at":1}}"#);
        let id = tag_id(&target, "rust");
        match put(&format!("pub/pubky.app/tags/{id}"), &json) {
            TranslatedPut::Tag {
                tagger_id,
                tag_id,
                tag,
                app,
            } => {
                assert_eq!(tagger_id, host());
                assert_eq!(tag_id, id);
                assert_eq!(tag.label, "rust");
                assert_eq!(app, None);
            }
            other => panic!("expected Tag, got {other:?}"),
        }
    }

    #[test]
    fn put_universal_tag_carries_the_app() {
        let json = r#"{"uri":"https://example.com/","label":"maps","created_at":1}"#;
        let id = tag_id("https://example.com/", "maps");
        match put(&format!("pub/mapky/tags/{id}"), json) {
            TranslatedPut::Tag {
                tagger_id,
                tag_id,
                app,
                ..
            } => {
                assert_eq!(tagger_id, host());
                assert_eq!(tag_id, id);
                assert_eq!(app.as_deref(), Some("mapky"));
            }
            other => panic!("expected Tag, got {other:?}"),
        }
    }

    #[test]
    fn put_bookmark() {
        let target = uri(&format!("pub/pubky.app/posts/{TS}"));
        let bookmark = PubkyAppBookmark {
            uri: target.clone(),
            created_at: 1,
        };
        let id = bookmark.create_id();
        let json = format!(r#"{{"uri":"{target}","created_at":1}}"#);
        match put(&format!("pub/pubky.app/bookmarks/{id}"), &json) {
            TranslatedPut::Bookmark {
                user_id,
                bookmark_id,
                bookmark,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(bookmark_id, id);
                assert_eq!(bookmark.target, target);
            }
            other => panic!("expected Bookmark, got {other:?}"),
        }
    }

    #[test]
    fn put_file_carries_the_event_uri() {
        let file_id = now_id();
        let src = uri(&format!("pub/pubky.app/blobs/{HASH}"));
        let json = format!(
            r#"{{"name":"a.png","created_at":1,"src":"{src}","content_type":"image/png","size":3}}"#
        );
        let path = format!("pub/pubky.app/files/{file_id}");
        match put(&path, &json) {
            TranslatedPut::File {
                user_id,
                file_id: id,
                file,
                uri: event_uri,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(id, file_id);
                assert_eq!(file.src, src);
                assert_eq!(event_uri, uri(&path));
            }
            other => panic!("expected File, got {other:?}"),
        }
    }

    #[test]
    fn put_mute_is_skipped() {
        match put(
            &format!("pub/pubky.app/mutes/{OTHER}"),
            r#"{"created_at":1}"#,
        ) {
            TranslatedPut::Skip { reason } => assert_eq!(reason, SkipReason::Mute),
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// Parsing skips these before any fetch; the translation still names them.
    #[test]
    fn put_unindexed_v0_objects_are_skipped() {
        let blob = b"abc";
        let blob_id = PubkyAppBlob(blob.to_vec()).create_id();
        let blob_uri = uri(&format!("pub/pubky.app/blobs/{blob_id}"));
        match translate_put(&route(&blob_uri), &blob_uri, blob).unwrap() {
            TranslatedPut::Skip { reason } => assert_eq!(reason, SkipReason::Blob),
            other => panic!("expected Skip, got {other:?}"),
        }

        match put("pub/pubky.app/last_read", r#"{"timestamp":1}"#) {
            TranslatedPut::Skip { reason } => assert_eq!(reason, SkipReason::LastRead),
            other => panic!("expected Skip, got {other:?}"),
        }

        let feed = PubkyAppFeed {
            feed: PubkyAppFeedConfig {
                tags: Some(vec!["bitcoin".into()]),
                domain_tags: None,
                reach: PubkyAppFeedReach::All,
                layout: PubkyAppFeedLayout::Columns,
                sort: PubkyAppFeedSort::Recent,
                content: None,
            },
            name: "Bitcoin".into(),
            icon: None,
            created_at: 1,
        };
        let feed_path = format!("pub/pubky.app/feeds/{}", feed.create_id());
        match put(&feed_path, &serde_json::to_string(&feed).unwrap()) {
            TranslatedPut::Skip { reason } => assert_eq!(reason, SkipReason::Feed),
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// Validation failures are deterministic, so they must not be retried.
    #[test]
    fn invalid_bytes_are_a_spec_validation_error() {
        let uri = uri(&format!("pub/pubky.app/posts/{}", now_id()));
        let route = route(&uri);
        for bytes in [&b"not json"[..], br#"{"content":"","kind":"short"}"#] {
            let err = translate_put(&route, &uri, bytes).unwrap_err();
            assert!(
                matches!(err, EventProcessorError::SpecValidation(_)),
                "expected SpecValidation, got {err:?}"
            );
        }
    }

    #[test]
    fn del_maps_each_resource_to_its_handler_inputs() {
        match del("pub/pubky.app/profile.json") {
            TranslatedDel::User { user_id } => assert_eq!(user_id, host()),
            other => panic!("expected User, got {other:?}"),
        }
        match del(&format!("pub/pubky.app/posts/{TS}")) {
            TranslatedDel::Post { author_id, post_id } => {
                assert_eq!(author_id, host());
                assert_eq!(post_id, TS);
            }
            other => panic!("expected Post, got {other:?}"),
        }
        match del(&format!("pub/pubky.app/follows/{OTHER}")) {
            TranslatedDel::Follow {
                user_id,
                followee_id,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(followee_id, other());
            }
            other => panic!("expected Follow, got {other:?}"),
        }
        match del(&format!("pub/pubky.app/bookmarks/{HASH}")) {
            TranslatedDel::Bookmark {
                user_id,
                bookmark_id,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(bookmark_id, HASH);
            }
            other => panic!("expected Bookmark, got {other:?}"),
        }
        match del(&format!("pub/pubky.app/files/{TS}")) {
            TranslatedDel::File { user_id, file_id } => {
                assert_eq!(user_id, host());
                assert_eq!(file_id, TS);
            }
            other => panic!("expected File, got {other:?}"),
        }
    }

    #[test]
    fn del_tag_carries_the_event_uri() {
        for path in [
            format!("pub/pubky.app/tags/{HASH}"),
            "pub/mapky/tags/ABC123".into(),
        ] {
            match del(&path) {
                TranslatedDel::Tag { uri: event_uri } => assert_eq!(event_uri, uri(&path)),
                other => panic!("expected Tag, got {other:?}"),
            }
        }
    }

    #[test]
    fn del_unhandled_resources_are_skipped() {
        let cases = [
            (format!("pub/pubky.app/mutes/{OTHER}"), SkipReason::Mute),
            (format!("pub/pubky.app/feeds/{HASH}"), SkipReason::Feed),
            (format!("pub/pubky.app/blobs/{HASH}"), SkipReason::Blob),
            ("pub/pubky.app/last_read".into(), SkipReason::LastRead),
        ];
        for (path, expected) in cases {
            match del(&path) {
                TranslatedDel::Skip { reason } => assert_eq!(reason, expected, "{path}"),
                other => panic!("{path}: expected Skip, got {other:?}"),
            }
        }
    }

    /// Only legacy routes become events; any other route reaching translation is a bug and must
    /// surface as an error, not a panic.
    #[test]
    fn non_legacy_routes_are_internal_errors() {
        let paths = [
            format!("pub/social/v1/posts/{TS}/{TS}.json"),
            format!("pub/social/v2/posts/{TS}"),
            "pub/social/tags/ABC".to_string(),
        ];
        for path in paths {
            let uri = uri(&path);
            let route = route(&uri);
            assert!(!matches!(route, EventRoute::Legacy(_)), "{path}");
            for err in [
                translate_put(&route, &uri, b"{}").unwrap_err(),
                translate_del(&route, &uri).unwrap_err(),
            ] {
                assert!(
                    matches!(err, EventProcessorError::InternalError(_)),
                    "{path}: expected InternalError, got {err:?}"
                );
            }
        }
    }
}
