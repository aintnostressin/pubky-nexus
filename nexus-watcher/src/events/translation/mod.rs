//! The explicit layer between an event and its handler: [`route`] classifies the URI by epoch,
//! [`skip_reason`] picks the routed events parsing skips before any fetch, [`is_canonical`] the
//! alias paths it rejects, and [`translate_put`] and [`translate_del`] turn a routed event into the
//! handler call that indexes it. [`resolve_reference`] reads every URI an object points at, so no
//! other code parses a spec's URI grammar. Pure functions: no I/O, no database, no homeserver.
//!
//! No translation failure can change on a retry, so each is an error the retry queue drops: a
//! route or resource translation cannot handle is `InvalidEventLine`, a payload the reader rejects
//! or classifies inconsistently with its path is `SpecValidation`.

mod legacy;
mod route;
mod social;
#[cfg(test)]
pub(super) mod test_support;

pub use route::{route, EventRoute, UNKNOWN_LABEL};

use crate::errors::EventProcessorError;
use crate::events::handlers::{
    BookmarkInput, FileInput, PostInput, Reference, TagInput, Target, UserInput,
};
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
/// Apart from [`TranslatedDel`] so a PUT cannot translate to a deletion. Each `uri` is the address
/// Nexus stores for the object: for v0, the event URI as received.
#[derive(Debug)]
pub enum TranslatedPut {
    User {
        user_id: PubkyId,
        user: UserInput,
        uri: String,
    },
    Post {
        author_id: PubkyId,
        post_id: String,
        post: PostInput,
        uri: String,
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
        uri: String,
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
    /// `app` is set for a universal tag written under another app's namespace.
    Tag {
        tagger_id: PubkyId,
        tag_id: String,
        app: Option<String>,
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

/// Whether `uri` is the address its epoch's specs render for the resource it routes to, so an alias
/// path can neither overwrite nor delete the resource it parses to. Parsing asks it of the events
/// it does not skip.
pub fn is_canonical(route: &EventRoute, uri: &str) -> bool {
    match route {
        EventRoute::Legacy(parsed) => legacy::is_canonical(parsed, uri),
        // Parsing skips or rejects these before asking
        EventRoute::Social { .. }
        | EventRoute::UnsupportedEpoch { .. }
        | EventRoute::Malformed { .. } => true,
    }
}

/// Reads the bytes fetched from a file's source. Only a v0 blob holds a file's bytes; its reader
/// checks them, so a blob that fails is a `SpecValidation` error, and a source naming anything else
/// is an `InvalidEventLine`.
pub fn read_file_source(src: &Reference, bytes: &[u8]) -> Result<Vec<u8>, EventProcessorError> {
    match &src.target {
        Target::Blob { blob_id, .. } => legacy::read_blob(blob_id, bytes),
        _ => Err(EventProcessorError::InvalidEventLine(format!(
            "The file has a source uri that is not a blob path: {}",
            src.uri
        ))),
    }
}

/// The posts a stored collection envelope curates, keyed, in curator order; items naming anything
/// else are dropped. Every stored envelope is a v0 one until a later epoch is indexed. Errors when
/// `content` is not an envelope.
pub fn collection_item_keys(content: &str) -> Result<Vec<(PubkyId, String)>, String> {
    Ok(legacy::collection_items(content)?
        .iter()
        .filter_map(Reference::post_key)
        .collect())
}

/// Resolves a URI an object points at with the router event paths take, so each reference is read
/// by its own epoch's parser, whichever epoch wrote the object holding it. A URI no parser reads,
/// a bare `pubky://<pk>` included, names nothing Nexus reads.
pub fn resolve_reference(uri: &str) -> Reference {
    let target = match route(uri) {
        EventRoute::Legacy(parsed) => legacy::target(&parsed),
        EventRoute::Social { parsed, .. } => social::target(&parsed),
        EventRoute::UnsupportedEpoch { .. } | EventRoute::Malformed { .. } => Target::External,
    };
    Reference {
        uri: uri.to_string(),
        target,
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

/// Maps a DEL to its handler call from the ids in the path. `uri` is the event URI as received.
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
    EventProcessorError::InvalidEventLine(format!("No translation for the route of {uri}"))
}

#[cfg(test)]
mod tests {
    use super::test_support::{host, other, uri, HASH, HOST, OTHER, TS};
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
            (format!("pub/pubky.app/mutes/{OTHER}"), SkipReason::Mute),
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
            TranslatedPut::User {
                user_id,
                user,
                uri: stored,
            } => {
                assert_eq!(user_id, host());
                assert_eq!(user.name, "Alice");
                assert_eq!(stored, uri("pub/pubky.app/profile.json"));
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
                uri: stored,
            } => {
                assert_eq!(author_id, host());
                assert_eq!(id, post_id);
                assert_eq!(post.content, "Hello");
                assert_eq!(stored, uri(&format!("pub/pubky.app/posts/{post_id}")));
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
                uri: stored,
            } => {
                assert_eq!(stored, uri(&format!("pub/pubky.app/tags/{id}")));
                assert_eq!(tagger_id, host());
                assert_eq!(tag_id, id);
                assert_eq!(tag.label, "rust");
                assert_eq!(tag.target.uri, target);
                assert_eq!(
                    tag.target.target,
                    Target::Post {
                        author_id: host(),
                        post_id: TS.into()
                    }
                );
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
                assert_eq!(bookmark.target.uri, target);
                assert_eq!(bookmark.target.post_key(), Some((host(), TS.to_string())));
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
                assert_eq!(file.src.uri, src);
                assert_eq!(
                    file.src.target,
                    Target::Blob {
                        owner_id: host(),
                        blob_id: HASH.into()
                    }
                );
                assert_eq!(event_uri, uri(&path));
            }
            other => panic!("expected File, got {other:?}"),
        }
    }

    /// Parsing skips a mute before any fetch; the translation still names it.
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
    fn del_tag_carries_the_tag_keys() {
        let cases = [
            (format!("pub/pubky.app/tags/{HASH}"), HASH, None),
            ("pub/mapky/tags/ABC123".into(), "ABC123", Some("mapky")),
        ];
        for (path, expected_id, expected_app) in cases {
            match del(&path) {
                TranslatedDel::Tag {
                    tagger_id,
                    tag_id,
                    app,
                } => {
                    assert_eq!(tagger_id, host(), "{path}");
                    assert_eq!(tag_id, expected_id, "{path}");
                    assert_eq!(app.as_deref(), expected_app, "{path}");
                }
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

    /// A collection's post items, keyed in curator order; other items and malformed URIs dropped.
    #[test]
    fn collection_item_keys_keep_post_items_in_curator_order() {
        let items = [
            uri(&format!("pub/pubky.app/posts/{TS}")),
            uri("pub/pubky.app/profile.json"),
            format!("pubky://not-a-pubky-id/pub/pubky.app/posts/{TS}"),
            format!("pubky://{OTHER}/pub/pubky.app/posts/{TS}"),
            uri(&format!("pub/social/v1/posts/{TS}")),
        ];
        let content = format!(
            r#"{{"name":"n","items":[{}]}}"#,
            items.map(|item| format!("\"{item}\"")).join(",")
        );
        assert_eq!(
            collection_item_keys(&content).unwrap(),
            vec![
                (host(), TS.to_string()),
                (other(), TS.to_string()),
                (host(), TS.to_string()),
            ]
        );
        assert!(collection_item_keys("just a short post").is_err());
    }

    /// A v0 collection's items are resolved when it is copied into the input; another kind has none.
    #[test]
    fn put_collection_resolves_its_items() {
        let post_id = now_id();
        let item = uri(&format!("pub/pubky.app/posts/{TS}"));
        let content = format!(r#"{{\"name\":\"n\",\"items\":[\"{item}\"]}}"#);
        let json = format!(
            r#"{{"content":"{content}","kind":"collection","parent":null,"embed":null,"attachments":null}}"#
        );
        match put(&format!("pub/pubky.app/posts/{post_id}"), &json) {
            TranslatedPut::Post { post, .. } => {
                assert_eq!(post.collected_keys(), vec![(host(), TS.to_string())]);
            }
            other => panic!("expected Post, got {other:?}"),
        }
    }

    #[test]
    fn a_file_source_is_read_as_the_blob_it_names() {
        let bytes = b"hello".to_vec();
        let id = PubkyAppBlob::new(bytes.clone()).create_id();
        let blob = resolve_reference(&uri(&format!("pub/pubky.app/blobs/{id}")));
        assert_eq!(read_file_source(&blob, &bytes).unwrap(), bytes);

        // The v0 reader checks the bytes against the blob id
        let err = read_file_source(&blob, b"other bytes").unwrap_err();
        assert!(
            matches!(err, EventProcessorError::SpecValidation(_)),
            "{err:?}"
        );

        let post = resolve_reference(&uri(&format!("pub/pubky.app/posts/{TS}")));
        let err = read_file_source(&post, &bytes).unwrap_err();
        assert!(
            matches!(err, EventProcessorError::InvalidEventLine(_)),
            "{err:?}"
        );
    }

    /// What a reference names; it keeps the URI exactly as written.
    fn target(reference: &str) -> Target {
        let resolved = resolve_reference(reference);
        assert_eq!(resolved.uri, reference);
        resolved.target
    }

    #[test]
    fn references_in_either_epoch_resolve_to_what_they_name() {
        let post = || Target::Post {
            author_id: host(),
            post_id: TS.into(),
        };
        let cases = [
            (format!("pub/pubky.app/posts/{TS}"), post()),
            ("pub/pubky.app/profile.json".into(), Target::User(host())),
            (
                format!("pub/pubky.app/files/{TS}"),
                Target::File {
                    owner_id: host(),
                    file_id: TS.into(),
                },
            ),
            (
                format!("pub/pubky.app/blobs/{HASH}"),
                Target::Blob {
                    owner_id: host(),
                    blob_id: HASH.into(),
                },
            ),
            (
                format!("pub/pubky.app/tags/{HASH}"),
                Target::Tag {
                    tagger_id: host(),
                    tag_id: HASH.into(),
                    app: None,
                },
            ),
            (
                "pub/mapky/tags/ABC123".into(),
                Target::Tag {
                    tagger_id: host(),
                    tag_id: "ABC123".into(),
                    app: Some("mapky".into()),
                },
            ),
            (
                format!("pub/pubky.app/follows/{OTHER}"),
                Target::Unkeyed { owner_id: host() },
            ),
            (
                "pub/pubky.app/widgets/ABC".into(),
                Target::Unkeyed { owner_id: host() },
            ),
            // A v1 reference names the post, whether versionless or a version file
            (format!("pub/social/v1/posts/{TS}"), post()),
            (format!("pub/social/v1/posts/{TS}/{TS}.json"), post()),
            ("pub/social/v1/profile.json".into(), Target::User(host())),
            (
                format!("pub/social/v1/files/{HASH}.png"),
                Target::File {
                    owner_id: host(),
                    file_id: HASH.into(),
                },
            ),
            (
                format!("pub/social/v1/tags/{HASH}.json"),
                Target::Tag {
                    tagger_id: host(),
                    tag_id: HASH.into(),
                    app: None,
                },
            ),
            (
                format!("pub/social/v1/follows/{OTHER}.json"),
                Target::Unkeyed { owner_id: host() },
            ),
        ];
        for (path, expected) in cases {
            assert_eq!(target(&uri(&path)), expected, "{path}");
        }
    }

    /// A trailing segment does not change what a v0 reference names, and the URI stays as written.
    #[test]
    fn a_sloppy_v0_reference_names_its_post() {
        assert_eq!(
            target(&uri(&format!("pub/pubky.app/posts/{TS}/"))),
            Target::Post {
                author_id: host(),
                post_id: TS.into(),
            }
        );
    }

    #[test]
    fn references_naming_nothing_nexus_reads_are_external() {
        let references = [
            "https://example.com/article".to_string(),
            "nostr:note1abc123".to_string(),
            "not a uri".to_string(),
            // A bare key is v1's user reference; it waits for a v1 reader to need it.
            format!("pubky://{HOST}"),
            uri("pub/eventky.app/events/E001"),
            format!("PUBKY://{OTHER}/pub/eventky.app/events/E001"),
            uri(&format!("pub/social/v2/posts/{TS}")),
            uri("pub/social/tags/ABC"),
        ];
        for reference in references {
            assert_eq!(target(&reference), Target::External, "{reference}");
        }
    }

    /// Only legacy routes become events; any other route reaching translation is a bug and must
    /// surface as an error the retry queue drops, not a panic.
    #[test]
    fn non_legacy_routes_are_invalid_event_lines() {
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
                    matches!(err, EventProcessorError::InvalidEventLine(_)),
                    "{path}: expected InvalidEventLine, got {err:?}"
                );
            }
        }
    }
}
