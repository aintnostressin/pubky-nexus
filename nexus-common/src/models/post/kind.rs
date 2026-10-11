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

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::PartialSchema;

    /// Every variant and the wire name it writes, the v0 names Redis, Neo4j and the API hold.
    const ALL: [(PostKind, &str); 8] = [
        (PostKind::Short, "short"),
        (PostKind::Long, "long"),
        (PostKind::Image, "image"),
        (PostKind::Video, "video"),
        (PostKind::Link, "link"),
        (PostKind::File, "file"),
        (PostKind::Collection, "collection"),
        (PostKind::Unknown, "unknown"),
    ];

    /// Names no variant writes: the catch-all's own name, other epochs' names, a miscased name and
    /// the empty string.
    const UNKNOWN_NAMES: [&str; 5] = ["unknown", "note", "article", "Short", ""];

    #[test]
    fn serializes_to_the_wire_name() {
        for (kind, wire) in ALL {
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{wire}\""));
        }
    }

    #[test]
    fn deserializes_unknown_names_to_unknown() {
        for (kind, wire) in ALL {
            let read: PostKind = serde_json::from_str(&format!("\"{wire}\"")).unwrap();
            assert_eq!(read, kind, "{wire:?}");
        }
        for name in UNKNOWN_NAMES {
            let read: PostKind = serde_json::from_str(&format!("\"{name}\"")).unwrap();
            assert_eq!(read, PostKind::Unknown, "{name:?}");
        }
    }

    /// `put.rs` stores both `to_string()` and the trimmed `serde_json::to_string`; all must agree.
    #[test]
    fn displays_the_wire_name() {
        for (kind, wire) in ALL {
            assert_eq!(kind.to_string(), wire);
            assert_eq!(
                serde_json::to_string(&kind).unwrap().trim_matches('"'),
                kind.to_string()
            );
        }
    }

    /// Strict, as in v0: the catch-all is not a name a caller can ask for.
    #[test]
    fn parses_only_the_named_kinds() {
        for (kind, wire) in &ALL[..7] {
            assert_eq!(wire.parse::<PostKind>().as_ref(), Ok(kind));
        }
        for name in UNKNOWN_NAMES {
            assert!(name.parse::<PostKind>().is_err(), "{name:?}");
        }
    }

    #[test]
    fn default_is_short() {
        assert_eq!(PostKind::default(), PostKind::Short);
    }

    /// The v0 component name and schema, so the OpenAPI document does not change.
    #[test]
    fn schema_is_the_v0_one() {
        assert_eq!(<PostKind as utoipa::ToSchema>::name(), "PubkyAppPostKind");
        assert_eq!(
            serde_json::to_value(PostKind::schema()).unwrap(),
            serde_json::json!({
                "type": "string",
                "description": "Represents the type of pubky-app posted data\nUsed primarily to best display the content in UI",
                "enum": ["short", "long", "image", "video", "link", "file", "collection", "unknown"]
            })
        );
    }
}
