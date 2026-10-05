use pubky_social_specs::legacy_v0::{try_parse_pubky_path, ExtendedParsedUri};
use pubky_social_specs::{
    epoch_segment, ParsedUri, PubkyId, Resource, SOCIAL_EPOCH, SOCIAL_NAMESPACE,
};

/// The label of a resource or epoch that classifies as none, spelled as the spec's unknown resource
/// so a route of none labels like a v1 path of no known resource.
pub const UNKNOWN_LABEL: &str = "unknown";

/// An event URI classified by its namespace segment, so each epoch's paths are judged by that
/// epoch's own parser.
#[derive(Debug, Clone, PartialEq)]
pub enum EventRoute {
    /// A `pubky.app` path, or another app's universal tag, read by the frozen v0 parser.
    Legacy(ExtendedParsedUri),
    /// A `social` path in the epoch this crate speaks.
    Social { epoch: u8, parsed: ParsedUri },
    /// A `social/vN` path in an epoch this crate does not speak: the spec's "upgrade me" signal.
    UnsupportedEpoch { version: String },
    /// No namespace to route on, or a path its namespace's parser rejects.
    Malformed { reason: String },
}

impl EventRoute {
    /// The user the URI belongs to, for the routes that name one.
    pub fn user_id(&self) -> Option<&PubkyId> {
        match self {
            EventRoute::Legacy(parsed) => Some(parsed.user_id()),
            EventRoute::Social { parsed, .. } => Some(&parsed.user_id),
            EventRoute::UnsupportedEpoch { .. } | EventRoute::Malformed { .. } => None,
        }
    }

    /// The resource kind, [`UNKNOWN_LABEL`] for a route that classifies none.
    pub fn resource_name(&self) -> String {
        match self {
            EventRoute::Legacy(parsed) => parsed.resource().to_string(),
            EventRoute::Social { parsed, .. } => parsed.resource.to_string(),
            EventRoute::UnsupportedEpoch { .. } | EventRoute::Malformed { .. } => {
                UNKNOWN_LABEL.to_string()
            }
        }
    }

    /// The id the path carries, for resources that have one.
    pub fn resource_id(&self) -> Option<String> {
        match self {
            EventRoute::Legacy(parsed) => parsed.resource().id(),
            EventRoute::Social { parsed, .. } => parsed.resource.id(),
            EventRoute::UnsupportedEpoch { .. } | EventRoute::Malformed { .. } => None,
        }
    }
}

/// Routes an event URI on its namespace segment: `social` to the v1 parser, `pubky.app` and every
/// other app to the frozen v0 parser. It never tries one parser and falls back to the other.
pub fn route(uri: &str) -> EventRoute {
    let path = match try_parse_pubky_path(uri) {
        Ok(path) => path,
        Err(reason) => return EventRoute::Malformed { reason },
    };

    match path.app.as_str() {
        SOCIAL_NAMESPACE => route_social(uri, &path.segments),
        // `pubky.app` itself, and the other apps whose universal tags the v0 parser claims
        _ => match ExtendedParsedUri::try_from(uri) {
            Ok(parsed) => EventRoute::Legacy(parsed),
            Err(reason) => EventRoute::Malformed { reason },
        },
    }
}

fn route_social(uri: &str, segments: &[String]) -> EventRoute {
    match ParsedUri::try_from(uri) {
        Err(reason) => EventRoute::Malformed { reason },
        Ok(ParsedUri {
            resource: Resource::UnsupportedVersion { version },
            ..
        }) => EventRoute::UnsupportedEpoch { version },
        // The v1 parser reads a v1 path it cannot classify and an epoch-less path alike as
        // `Unknown`; only the first is an object of the epoch.
        Ok(parsed) if segments.first().is_some_and(|s| *s == epoch_segment()) => {
            EventRoute::Social {
                epoch: SOCIAL_EPOCH,
                parsed,
            }
        }
        Ok(_) => EventRoute::Malformed {
            reason: format!("Social path without an epoch segment: {uri}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::translation::test_support::{host, other, uri, HASH, HOST, OTHER, TS};
    use pubky_social_specs::legacy_v0::Resource as V0Resource;

    /// The v1 resource of a supported-epoch route, or a panic naming what came back.
    fn social(path: &str) -> Resource {
        match route(&uri(path)) {
            EventRoute::Social { epoch, parsed } => {
                assert_eq!(epoch, SOCIAL_EPOCH, "{path}");
                assert_eq!(parsed.user_id, host(), "{path}");
                parsed.resource
            }
            other => panic!("{path} should route to social, got {other:?}"),
        }
    }

    /// The v0 resource of a `pubky.app` route, or a panic naming what came back.
    fn legacy(uri: &str) -> V0Resource {
        match route(uri) {
            EventRoute::Legacy(ExtendedParsedUri::PubkyApp { user_id, resource }) => {
                assert_eq!(user_id, host(), "{uri}");
                resource
            }
            other => panic!("{uri} should route to pubky.app, got {other:?}"),
        }
    }

    fn assert_malformed(uri: &str) {
        let route = route(uri);
        assert!(
            matches!(route, EventRoute::Malformed { .. }),
            "{uri} should be malformed, got {route:?}"
        );
    }

    #[test]
    fn v1_post_version_routes_to_social() {
        let path = format!("pub/social/v1/posts/{TS}/{TS}.json");
        assert_eq!(
            social(&path),
            Resource::Post {
                id: TS.into(),
                version: Some(TS.into()),
                label: None,
            }
        );

        let route = route(&uri(&path));
        assert_eq!(route.user_id(), Some(&host()));
        assert_eq!(route.resource_name(), "posts");
        assert_eq!(route.resource_id().as_deref(), Some(TS));
    }

    #[test]
    fn v1_post_reference_routes_to_social() {
        assert_eq!(
            social(&format!("pub/social/v1/posts/{TS}")),
            Resource::Post {
                id: TS.into(),
                version: None,
                label: None,
            }
        );
    }

    #[test]
    fn v1_profile_routes_to_social() {
        assert_eq!(social("pub/social/v1/profile.json"), Resource::User);
        let route = route(&uri("pub/social/v1/profile.json"));
        assert_eq!(route.resource_name(), "profile.json");
        assert_eq!(route.resource_id(), None);
    }

    #[test]
    fn v1_tag_follow_file_and_feed_route_to_social() {
        assert_eq!(
            social(&format!("pub/social/v1/tags/{HASH}.json")),
            Resource::Tag(HASH.into())
        );
        assert_eq!(
            social(&format!("pub/social/v1/follows/{OTHER}.json")),
            Resource::Follow(other())
        );
        assert_eq!(
            social(&format!("pub/social/v1/files/{HASH}.png")),
            Resource::File(format!("{HASH}.png"))
        );
        assert_eq!(
            social(&format!("pub/social/v1/feeds/{HASH}.json")),
            Resource::Feed(HASH.into())
        );

        let file = route(&uri(&format!("pub/social/v1/files/{HASH}.png")));
        assert_eq!(file.resource_name(), "files");
        assert_eq!(file.resource_id().as_deref(), Some(HASH));
    }

    /// The splitter only knows the public root, and the events feed never carries `/priv/`.
    #[test]
    fn priv_bookmark_is_malformed() {
        assert_malformed(&uri(&format!("priv/social/v1/bookmarks/~{HASH}.json")));
    }

    /// An epoch path the v1 parser cannot classify is still an object of that epoch.
    #[test]
    fn unknown_v1_resource_routes_to_social() {
        assert_eq!(social("pub/social/v1/widgets/ABC"), Resource::Unknown);
        assert_eq!(
            route(&uri("pub/social/v1/widgets/ABC")).resource_name(),
            "unknown"
        );
    }

    /// Today's v0 parser claims this as a universal tag of an app named `social`; the namespace is
    /// reserved now.
    #[test]
    fn epoch_less_social_path_is_malformed() {
        assert_malformed(&uri("pub/social/tags/ABC"));
    }

    /// A v1 path of no known resource labels as the routes that classify none do.
    #[test]
    fn unknown_label_is_the_spec_spelling() {
        assert_eq!(Resource::Unknown.to_string(), UNKNOWN_LABEL);
    }

    #[test]
    fn other_social_epoch_is_unsupported() {
        let route = route(&uri(&format!("pub/social/v2/posts/{TS}")));
        assert_eq!(
            route,
            EventRoute::UnsupportedEpoch {
                version: "v2".into()
            }
        );
        assert_eq!(route.user_id(), None);
        assert_eq!(route.resource_name(), "unknown");
        assert_eq!(route.resource_id(), None);
    }

    #[test]
    fn pubky_app_resources_route_to_legacy() {
        let cases = [
            ("profile.json".to_string(), V0Resource::User),
            (format!("posts/{TS}"), V0Resource::Post(TS.into())),
            (format!("follows/{OTHER}"), V0Resource::Follow(other())),
            (format!("mutes/{OTHER}"), V0Resource::Mute(other())),
            (
                format!("bookmarks/{HASH}"),
                V0Resource::Bookmark(HASH.into()),
            ),
            (format!("tags/{HASH}"), V0Resource::Tag(HASH.into())),
            (format!("files/{TS}"), V0Resource::File(TS.into())),
            (format!("blobs/{HASH}"), V0Resource::Blob(HASH.into())),
            (format!("feeds/{HASH}"), V0Resource::Feed(HASH.into())),
            ("last_read".to_string(), V0Resource::LastRead),
            ("widgets/ABC".to_string(), V0Resource::Unknown),
        ];
        for (path, expected) in cases {
            assert_eq!(legacy(&uri(&format!("pub/pubky.app/{path}"))), expected);
        }

        let route = route(&uri(&format!("pub/pubky.app/posts/{TS}")));
        assert_eq!(route.user_id(), Some(&host()));
        assert_eq!(route.resource_name(), "posts");
        assert_eq!(route.resource_id().as_deref(), Some(TS));
    }

    #[test]
    fn other_app_tag_routes_to_legacy_universal_tag() {
        let route = route(&uri("pub/mapky/tags/ABC123"));
        assert_eq!(
            route,
            EventRoute::Legacy(ExtendedParsedUri::UniversalTag {
                user_id: host(),
                app: "mapky".into(),
                resource: V0Resource::Tag("ABC123".into()),
            })
        );
        assert_eq!(route.resource_name(), "tags");
        assert_eq!(route.resource_id().as_deref(), Some("ABC123"));
    }

    /// Another app's non-tag path goes to the v0 parser too, which rejects it as it does today.
    #[test]
    fn other_app_non_tag_path_is_malformed() {
        assert_malformed(&uri("pub/eventky.app/events/E001"));
    }

    #[test]
    fn unsplittable_uris_are_malformed() {
        assert_malformed("not a uri");
        assert_malformed(&format!("https://{HOST}/pub/pubky.app/posts/{TS}"));
        assert_malformed(&format!("pubky://not-a-pubky-id/pub/pubky.app/posts/{TS}"));
        assert_malformed(&uri("pub"));
        assert_malformed(&format!("pubky://{HOST}"));
    }

    /// Shapes the v0 parser accepts and the v1 parser rejects. Routing on the namespace keeps them
    /// on the v0 parser by construction; a v1-first fallback would only keep them by accident.
    #[test]
    fn sloppy_v0_shapes_stay_on_the_legacy_parser() {
        let post = V0Resource::Post(TS.into());
        let cases = [
            (
                "trailing slash",
                uri(&format!("pub/pubky.app/posts/{TS}/")),
                post.clone(),
            ),
            (
                "double slash",
                uri(&format!("pub/pubky.app/posts/{TS}//")),
                post.clone(),
            ),
            (
                "query string",
                uri(&format!("pub/pubky.app/posts/{TS}?a=b")),
                post.clone(),
            ),
            (
                "fragment",
                uri(&format!("pub/pubky.app/posts/{TS}#top")),
                post.clone(),
            ),
            (
                "percent escape",
                uri("pub/pubky.app/posts/0032SSN7Q4EV%47"),
                V0Resource::Post("0032SSN7Q4EV%47".into()),
            ),
            (
                "uppercase scheme",
                format!("PUBKY://{HOST}/pub/pubky.app/posts/{TS}"),
                post.clone(),
            ),
            (
                "dot-dot segment",
                uri(&format!("pub/pubky.app/feeds/../posts/{TS}")),
                post.clone(),
            ),
            (
                "embedded space",
                uri("pub/pubky.app/posts/0032SSN7 Q4EVG"),
                V0Resource::Post("0032SSN7%20Q4EVG".into()),
            ),
            (
                "embedded tab",
                uri("pub/pubky.app/posts/0032SSN7\tQ4EVG"),
                post.clone(),
            ),
        ];
        for (shape, uri, expected) in cases {
            assert!(
                ParsedUri::try_from(uri.as_str()).is_err(),
                "{shape}: the v1 parser accepts {uri}"
            );
            assert_eq!(legacy(&uri), expected, "{shape}");
        }
    }
}
