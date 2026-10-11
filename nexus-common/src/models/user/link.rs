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

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::PartialSchema;

    const JSON: &str = r#"{"title":"Website","url":"https://example.com"}"#;

    /// `create_user` stores the links as this JSON string in the graph.
    #[test]
    fn serializes_as_title_and_url() {
        let link = UserLink {
            title: "Website".into(),
            url: "https://example.com".into(),
        };
        assert_eq!(serde_json::to_string(&link).unwrap(), JSON);
    }

    #[test]
    fn deserializes_title_and_url() {
        let link: UserLink = serde_json::from_str(JSON).unwrap();
        assert_eq!(
            (link.title.as_str(), link.url.as_str()),
            ("Website", "https://example.com")
        );
    }

    /// The v0 component name and schema, so the OpenAPI document does not change.
    #[test]
    fn schema_is_the_v0_one() {
        assert_eq!(<UserLink as utoipa::ToSchema>::name(), "PubkyAppUserLink");
        assert_eq!(
            serde_json::to_value(UserLink::schema()).unwrap(),
            serde_json::json!({
                "type": "object",
                "description": "Represents a user's single link with a title and URL.",
                "required": ["title", "url"],
                "properties": {
                    "title": {"type": "string"},
                    "url": {"type": "string"}
                }
            })
        );
    }
}
