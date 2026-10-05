use pubky_social_specs::legacy_v0::PubkyAppPostKind;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use utoipa::ToSchema;

// A copy of the v0 enum: its names are the strings Redis, Neo4j and the API already hold, and the
// doc comment and schema name are kept verbatim so the OpenAPI document stays the same.
/// Represents the type of pubky-app posted data
/// Used primarily to best display the content in UI
#[derive(Serialize, Deserialize, ToSchema, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
#[schema(as = PubkyAppPostKind)]
pub enum PostKind {
    #[default]
    Short,
    Long,
    Image,
    Video,
    Link,
    File,
    Collection,
    #[serde(other)]
    Unknown,
}

impl fmt::Display for PostKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string_repr = serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        write!(f, "{string_repr}")
    }
}

impl FromStr for PostKind {
    type Err = String;

    /// Strict, as in v0: `unknown` is the serde catch-all, not a name a caller can ask for.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "short" => Ok(PostKind::Short),
            "long" => Ok(PostKind::Long),
            "image" => Ok(PostKind::Image),
            "video" => Ok(PostKind::Video),
            "link" => Ok(PostKind::Link),
            "file" => Ok(PostKind::File),
            "collection" => Ok(PostKind::Collection),
            _ => Err(format!("Invalid content kind: {s}")),
        }
    }
}

impl From<PubkyAppPostKind> for PostKind {
    fn from(kind: PubkyAppPostKind) -> Self {
        match kind {
            PubkyAppPostKind::Short => PostKind::Short,
            PubkyAppPostKind::Long => PostKind::Long,
            PubkyAppPostKind::Image => PostKind::Image,
            PubkyAppPostKind::Video => PostKind::Video,
            PubkyAppPostKind::Link => PostKind::Link,
            PubkyAppPostKind::File => PostKind::File,
            PubkyAppPostKind::Collection => PostKind::Collection,
            PubkyAppPostKind::Unknown => PostKind::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::PartialSchema;

    /// Names both enums must read alike: every wire name, names neither knows, a miscased name and
    /// the empty string.
    const INPUTS: [&str; 12] = [
        "short",
        "long",
        "image",
        "video",
        "link",
        "file",
        "collection",
        "unknown",
        "note",
        "article",
        "Short",
        "",
    ];

    /// Every variant next to its v0 counterpart and the wire name both must write.
    fn all() -> [(PostKind, PubkyAppPostKind, &'static str); 8] {
        [
            (PostKind::Short, PubkyAppPostKind::Short, "short"),
            (PostKind::Long, PubkyAppPostKind::Long, "long"),
            (PostKind::Image, PubkyAppPostKind::Image, "image"),
            (PostKind::Video, PubkyAppPostKind::Video, "video"),
            (PostKind::Link, PubkyAppPostKind::Link, "link"),
            (PostKind::File, PubkyAppPostKind::File, "file"),
            (
                PostKind::Collection,
                PubkyAppPostKind::Collection,
                "collection",
            ),
            (PostKind::Unknown, PubkyAppPostKind::Unknown, "unknown"),
        ]
    }

    #[test]
    fn from_legacy_maps_name_for_name() {
        for (owned, legacy, _) in all() {
            assert_eq!(PostKind::from(legacy), owned);
        }
    }

    #[test]
    fn serializes_to_the_legacy_wire_name() {
        for (owned, legacy, wire) in all() {
            let json = serde_json::to_string(&owned).unwrap();
            assert_eq!(json, serde_json::to_string(&legacy).unwrap());
            assert_eq!(json, format!("\"{wire}\""));
        }
    }

    #[test]
    fn deserializes_like_legacy() {
        for input in INPUTS {
            let json = format!("\"{input}\"");
            let owned: PostKind = serde_json::from_str(&json).unwrap();
            let legacy: PubkyAppPostKind = serde_json::from_str(&json).unwrap();
            assert_eq!(owned, PostKind::from(legacy), "{input:?}");
        }
    }

    /// `put.rs` stores both `to_string()` and the trimmed `serde_json::to_string`; all must agree.
    #[test]
    fn displays_like_legacy() {
        for (owned, legacy, wire) in all() {
            assert_eq!(owned.to_string(), legacy.to_string());
            assert_eq!(owned.to_string(), wire);
            assert_eq!(
                serde_json::to_string(&owned).unwrap().trim_matches('"'),
                owned.to_string()
            );
        }
    }

    #[test]
    fn parses_like_legacy() {
        for input in INPUTS {
            let owned = input.parse::<PostKind>();
            let legacy = input.parse::<PubkyAppPostKind>().map(PostKind::from);
            assert_eq!(owned, legacy, "{input:?}");
        }
    }

    #[test]
    fn default_is_legacy_default() {
        assert_eq!(
            PostKind::default(),
            PostKind::from(PubkyAppPostKind::default())
        );
    }

    #[test]
    fn schema_matches_legacy() {
        assert_eq!(PostKind::name(), PubkyAppPostKind::name());
        assert_eq!(
            serde_json::to_value(PostKind::schema()).unwrap(),
            serde_json::to_value(PubkyAppPostKind::schema()).unwrap()
        );
    }
}
