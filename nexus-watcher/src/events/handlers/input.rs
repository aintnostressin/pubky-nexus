//! What the handlers take: the fields Nexus reads from a homeserver object, which the translation
//! layer copies out of each epoch's objects, and the stored models built from them.

use chrono::Utc;
use nexus_common::models::file::{FileDetails, FileUrls};
use nexus_common::models::post::{PostDetails, PostKind, PostRelationships};
use nexus_common::models::user::{UserDetails, UserLink};
use pubky_social_specs::legacy_v0::{post_uri_builder, ParsedUri, Resource};
use pubky_social_specs::PubkyId;

/// A post as the post handler reads it.
#[derive(Debug, Clone)]
pub struct PostInput {
    pub content: String,
    pub kind: PostKind,
    /// URI of the post this one replies to.
    pub parent: Option<String>,
    /// URI of the embedded object, the only part of an embed Nexus reads.
    pub embed: Option<String>,
    pub attachments: Option<Vec<String>>,
    pub lock: Option<String>,
}

impl PostInput {
    pub fn into_details(self, author_id: &PubkyId, post_id: &str) -> PostDetails {
        PostDetails {
            uri: post_uri_builder(author_id.to_string(), post_id.into()),
            content: self.content,
            id: post_id.to_string(),
            indexed_at: Utc::now().timestamp_millis(),
            author: author_id.to_string(),
            kind: self.kind,
            attachments: self.attachments,
            lock: self.lock,
        }
    }

    /// The post this one replies to and the post it reposts.
    pub fn relationships(&self) -> PostRelationships {
        let mut relationship = PostRelationships::default();

        if let Some(parent_uri) = &self.parent {
            relationship.replied = ParsedUri::try_from(parent_uri.as_str()).ok()
        }

        // Only a post can be reposted; other embed targets stay plain embeds.
        if let Some(embed_uri) = &self.embed {
            relationship.reposted = ParsedUri::try_from(embed_uri.as_str())
                .ok()
                .filter(|uri| matches!(uri.resource, Resource::Post(_)));
        }
        relationship
    }
}

/// A bookmark as the bookmark handler reads it.
#[derive(Debug, Clone)]
pub struct BookmarkInput {
    /// URI of the bookmarked post.
    pub target: String,
}

/// A profile as the user handler reads it.
#[derive(Debug, Clone)]
pub struct UserInput {
    pub name: String,
    pub bio: Option<String>,
    pub image: Option<String>,
    pub links: Option<Vec<UserLink>>,
    pub status: Option<String>,
}

impl UserInput {
    pub fn into_details(self, user_id: &PubkyId) -> UserDetails {
        UserDetails {
            name: self.name,
            bio: self.bio,
            status: self.status,
            links: self.links,
            image: self.image,
            id: user_id.clone(),
            indexed_at: Utc::now().timestamp_millis(),
            deleted: false,
        }
    }
}

/// A tag as the tag handlers and moderation read it.
#[derive(Debug, Clone)]
pub struct TagInput {
    /// URI of the tagged object.
    pub target: String,
    pub label: String,
}

/// A file as the file handler reads it.
#[derive(Debug, Clone)]
pub struct FileInput {
    pub name: String,
    /// URI of the blob holding the bytes.
    pub src: String,
    pub content_type: String,
    pub size: usize,
    pub created_at: i64,
}

impl FileInput {
    pub fn into_details(self, uri: String, user_id: String, file_id: String) -> FileDetails {
        let urls = FileUrls::new(&user_id, &file_id, &self.content_type);
        FileDetails {
            urls,
            name: self.name,
            src: self.src,
            content_type: self.content_type,
            uri,
            id: file_id,
            created_at: self.created_at,
            indexed_at: Utc::now().timestamp_millis(),
            owner_id: user_id,
            size: self.size as i64,
            metadata: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky_social_specs::legacy_v0::file_uri_builder;

    const AUTHOR: &str = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    const POST_ID: &str = "003286NSMY490";

    fn uri_of_post(id: &str) -> String {
        post_uri_builder(AUTHOR.into(), id.into())
    }

    fn post_without_relations() -> PostInput {
        PostInput {
            content: "x".into(),
            kind: PostKind::Short,
            parent: None,
            embed: None,
            attachments: None,
            lock: None,
        }
    }

    /// The edit check compares `PostDetails.attachments`, so a missing list must stay missing.
    #[test]
    fn post_without_attachments_keeps_none() {
        let post = PostInput {
            attachments: None,
            ..post_without_relations()
        };

        let author = PubkyId::try_from(AUTHOR).unwrap();
        let details = post.into_details(&author, POST_ID);
        assert_eq!(details.attachments, None);
        assert_eq!(details.uri, uri_of_post(POST_ID));
    }

    #[test]
    fn post_embed_becomes_a_repost() {
        let rel = PostInput {
            embed: Some(uri_of_post(POST_ID)),
            ..post_without_relations()
        }
        .relationships();
        assert!(
            matches!(rel.reposted.as_ref(), Some(u) if matches!(u.resource, Resource::Post(_))),
            "a post embed should become a reposted relationship"
        );
    }

    #[test]
    fn non_post_embed_is_not_a_repost() {
        // A non-post embed (file URI) must not become a repost.
        let rel = PostInput {
            embed: Some(file_uri_builder(AUTHOR.into(), POST_ID.into())),
            ..post_without_relations()
        }
        .relationships();
        assert!(
            rel.reposted.is_none(),
            "a non-post embed must not become a repost"
        );
    }
}
