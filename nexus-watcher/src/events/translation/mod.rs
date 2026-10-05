//! The explicit layer between an event and its handler: [`route`] classifies the URI by epoch,
//! [`translate_put`] and [`translate_del`] turn a routed event into the handler call that indexes
//! it. Pure functions: no I/O, no database, no homeserver.

mod legacy;
mod route;

pub use route::{route, EventRoute};

use crate::errors::EventProcessorError;
use pubky_social_specs::legacy_v0::{
    PubkyAppBookmark, PubkyAppFile, PubkyAppPost, PubkyAppTag, PubkyAppUser, PubkyId,
};

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

/// One handler call, carrying the inputs that handler takes. The objects are the v0 types.
#[derive(Debug)]
pub enum Translated {
    PutUser {
        user_id: PubkyId,
        user: PubkyAppUser,
    },
    PutPost {
        author_id: PubkyId,
        post_id: String,
        post: PubkyAppPost,
    },
    PutFollow {
        user_id: PubkyId,
        followee_id: PubkyId,
    },
    /// `app` is set for a universal tag written under another app's namespace.
    PutTag {
        tagger_id: PubkyId,
        tag_id: String,
        tag: PubkyAppTag,
        app: Option<String>,
    },
    PutBookmark {
        user_id: PubkyId,
        bookmark_id: String,
        bookmark: PubkyAppBookmark,
    },
    PutFile {
        user_id: PubkyId,
        file_id: String,
        file: PubkyAppFile,
        uri: String,
    },
    DelUser {
        user_id: PubkyId,
    },
    DelPost {
        author_id: PubkyId,
        post_id: String,
    },
    DelFollow {
        user_id: PubkyId,
        followee_id: PubkyId,
    },
    DelTag {
        uri: String,
    },
    DelBookmark {
        user_id: PubkyId,
        bookmark_id: String,
    },
    DelFile {
        user_id: PubkyId,
        file_id: String,
    },
    Skip {
        reason: SkipReason,
    },
}

/// Reads the fetched `bytes` of a PUT with its epoch's reader. `uri` is the event URI as received,
/// which the file handler stores.
pub fn translate_put(
    route: &EventRoute,
    uri: &str,
    bytes: &[u8],
) -> Result<Translated, EventProcessorError> {
    match route {
        EventRoute::Legacy(parsed) => legacy::translate_put(parsed, uri, bytes),
        EventRoute::Social { .. }
        | EventRoute::UnsupportedEpoch { .. }
        | EventRoute::Malformed { .. } => Err(untranslatable(uri)),
    }
}

/// Maps a DEL to its handler call from the ids in the path. `uri` is the event URI as received,
/// which the tag handler takes.
pub fn translate_del(route: &EventRoute, uri: &str) -> Result<Translated, EventProcessorError> {
    match route {
        EventRoute::Legacy(parsed) => legacy::translate_del(parsed, uri),
        EventRoute::Social { .. }
        | EventRoute::UnsupportedEpoch { .. }
        | EventRoute::Malformed { .. } => Err(untranslatable(uri)),
    }
}

/// Parsing skips or rejects every route but `Legacy` before an event exists, so this is a bug.
fn untranslatable(uri: &str) -> EventProcessorError {
    EventProcessorError::internal_error(format!("No translation for the route of {uri}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base32::{encode, Alphabet};
    use chrono::Utc;
    use pubky_social_specs::legacy_v0::traits::HashId;
    use pubky_social_specs::legacy_v0::{
        tag_id, PubkyAppBlob, PubkyAppFeed, PubkyAppFeedConfig, PubkyAppFeedLayout,
        PubkyAppFeedReach, PubkyAppFeedSort,
    };

    const HOST: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const TS: &str = "0032SSN7Q4EVG";
    const HASH: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

    fn uri(path: &str) -> String {
        format!("pubky://{HOST}/{path}")
    }

    fn host() -> PubkyId {
        PubkyId::try_from(HOST).unwrap()
    }

    fn other() -> PubkyId {
        PubkyId::try_from(OTHER).unwrap()
    }

    /// A timestamp id for now: the frozen reader bounds these by the clock.
    fn now_id() -> String {
        encode(
            Alphabet::Crockford,
            &Utc::now().timestamp_micros().to_be_bytes(),
        )
    }

    fn put(path: &str, json: &str) -> Translated {
        let uri = uri(path);
        translate_put(&route(&uri), &uri, json.as_bytes())
            .unwrap_or_else(|e| panic!("{path} should translate: {e}"))
    }

    fn del(path: &str) -> Translated {
        let uri = uri(path);
        translate_del(&route(&uri), &uri).unwrap_or_else(|e| panic!("{path} should translate: {e}"))
    }

    #[test]
    fn put_user() {
        let json = r#"{"name":"Alice","bio":"Hi","image":null,"links":null,"status":null}"#;
        match put("pub/pubky.app/profile.json", json) {
            Translated::PutUser { user_id, user } => {
                assert_eq!(user_id, host());
                assert_eq!(user.name, "Alice");
            }
            other => panic!("expected PutUser, got {other:?}"),
        }
    }

    #[test]
    fn put_post() {
        let post_id = now_id();
        let json =
            r#"{"content":"Hello","kind":"short","parent":null,"embed":null,"attachments":null}"#;
        match put(&format!("pub/pubky.app/posts/{post_id}"), json) {
            Translated::PutPost {
                author_id,
                post_id: id,
                post,
            } => {
                assert_eq!(author_id, host());
                assert_eq!(id, post_id);
                assert_eq!(post.content, "Hello");
            }
            other => panic!("expected PutPost, got {other:?}"),
        }
    }

    #[test]
    fn put_follow() {
        match put(
            &format!("pub/pubky.app/follows/{OTHER}"),
            r#"{"created_at":1}"#,
        ) {
            Translated::PutFollow {
                user_id,
                followee_id,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(followee_id, other());
            }
            other => panic!("expected PutFollow, got {other:?}"),
        }
    }

    #[test]
    fn put_tag() {
        let target = uri(&format!("pub/pubky.app/posts/{TS}"));
        let json = format!(r#"{{"uri":"{target}","label":"rust","created_at":1}}"#);
        let id = tag_id(&target, "rust");
        match put(&format!("pub/pubky.app/tags/{id}"), &json) {
            Translated::PutTag {
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
            other => panic!("expected PutTag, got {other:?}"),
        }
    }

    #[test]
    fn put_universal_tag_carries_the_app() {
        let json = r#"{"uri":"https://example.com/","label":"maps","created_at":1}"#;
        let id = tag_id("https://example.com/", "maps");
        match put(&format!("pub/mapky/tags/{id}"), json) {
            Translated::PutTag {
                tagger_id,
                tag_id,
                app,
                ..
            } => {
                assert_eq!(tagger_id, host());
                assert_eq!(tag_id, id);
                assert_eq!(app.as_deref(), Some("mapky"));
            }
            other => panic!("expected PutTag, got {other:?}"),
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
            Translated::PutBookmark {
                user_id,
                bookmark_id,
                bookmark,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(bookmark_id, id);
                assert_eq!(bookmark.uri, target);
            }
            other => panic!("expected PutBookmark, got {other:?}"),
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
            Translated::PutFile {
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
            other => panic!("expected PutFile, got {other:?}"),
        }
    }

    #[test]
    fn put_mute_is_skipped() {
        match put(
            &format!("pub/pubky.app/mutes/{OTHER}"),
            r#"{"created_at":1}"#,
        ) {
            Translated::Skip { reason } => assert_eq!(reason, SkipReason::Mute),
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// Parsing skips these before any fetch; the translation still names them.
    #[test]
    fn put_unindexed_v0_objects_are_skipped() {
        let blob = b"abc";
        let blob_id = PubkyAppBlob(blob.to_vec()).create_id();
        let blob_uri = uri(&format!("pub/pubky.app/blobs/{blob_id}"));
        let skipped = translate_put(&route(&blob_uri), &blob_uri, blob).unwrap();
        assert!(matches!(
            skipped,
            Translated::Skip {
                reason: SkipReason::Blob
            }
        ));

        assert!(matches!(
            put("pub/pubky.app/last_read", r#"{"timestamp":1}"#),
            Translated::Skip {
                reason: SkipReason::LastRead
            }
        ));

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
        assert!(matches!(
            put(&feed_path, &serde_json::to_string(&feed).unwrap()),
            Translated::Skip {
                reason: SkipReason::Feed
            }
        ));
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
        assert!(matches!(
            del("pub/pubky.app/profile.json"),
            Translated::DelUser { user_id } if user_id == host()
        ));
        assert!(matches!(
            del(&format!("pub/pubky.app/posts/{TS}")),
            Translated::DelPost { author_id, post_id } if author_id == host() && post_id == TS
        ));
        assert!(matches!(
            del(&format!("pub/pubky.app/follows/{OTHER}")),
            Translated::DelFollow { user_id, followee_id }
                if user_id == host() && followee_id == other()
        ));
        assert!(matches!(
            del(&format!("pub/pubky.app/bookmarks/{HASH}")),
            Translated::DelBookmark { user_id, bookmark_id }
                if user_id == host() && bookmark_id == HASH
        ));
        assert!(matches!(
            del(&format!("pub/pubky.app/files/{TS}")),
            Translated::DelFile { user_id, file_id } if user_id == host() && file_id == TS
        ));
    }

    #[test]
    fn del_tag_carries_the_event_uri() {
        for path in [
            format!("pub/pubky.app/tags/{HASH}"),
            "pub/mapky/tags/ABC123".into(),
        ] {
            match del(&path) {
                Translated::DelTag { uri: event_uri } => assert_eq!(event_uri, uri(&path)),
                other => panic!("expected DelTag, got {other:?}"),
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
                Translated::Skip { reason } => assert_eq!(reason, expected, "{path}"),
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
