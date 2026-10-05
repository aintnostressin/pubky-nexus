use pubky_social_specs::legacy_v0::PubkyAppUserLink;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// A copy of the v0 link; the doc comment and schema name are kept verbatim so the OpenAPI document
// stays the same.
/// Represents a user's single link with a title and URL.
#[derive(Serialize, Deserialize, ToSchema, Default, Clone, Debug)]
#[schema(as = PubkyAppUserLink)]
pub struct UserLink {
    pub title: String,
    pub url: String,
}

impl From<PubkyAppUserLink> for UserLink {
    fn from(link: PubkyAppUserLink) -> Self {
        Self {
            title: link.title,
            url: link.url,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::PartialSchema;

    fn legacy() -> PubkyAppUserLink {
        PubkyAppUserLink {
            title: "Website".into(),
            url: "https://example.com".into(),
        }
    }

    #[test]
    fn from_legacy_copies_the_fields() {
        let link = UserLink::from(legacy());
        assert_eq!(link.title, "Website");
        assert_eq!(link.url, "https://example.com");
    }

    /// `create_user` stores the links as this JSON string in the graph.
    #[test]
    fn serializes_like_legacy() {
        let owned = Some(vec![UserLink::from(legacy())]);
        let legacy = Some(vec![legacy()]);
        assert_eq!(
            serde_json::to_string(&owned).unwrap(),
            serde_json::to_string(&legacy).unwrap()
        );
    }

    #[test]
    fn deserializes_like_legacy() {
        let json = r#"{"title":"Website","url":"https://example.com"}"#;
        let owned: UserLink = serde_json::from_str(json).unwrap();
        let legacy: PubkyAppUserLink = serde_json::from_str(json).unwrap();
        assert_eq!((owned.title, owned.url), (legacy.title, legacy.url));
    }

    #[test]
    fn schema_matches_legacy() {
        assert_eq!(UserLink::name(), PubkyAppUserLink::name());
        assert_eq!(
            serde_json::to_value(UserLink::schema()).unwrap(),
            serde_json::to_value(PubkyAppUserLink::schema()).unwrap()
        );
    }
}
