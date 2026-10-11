//! What the handlers take: the fields Nexus reads from a homeserver object, which the translation
//! layer copies out of each epoch's objects, and the stored models built from them.

use chrono::Utc;
use nexus_common::models::file::{FileDetails, FileUrls};
use nexus_common::models::post::{PostDetails, PostKind, PostLinks, PostRelationships};
use nexus_common::models::user::{UserDetails, UserLink};
use pubky_social_specs::PubkyId;

/// A URI an object points at, resolved once by the translation layer: the URI as the object wrote
/// it, which is what Nexus stores and serves, and what it names.
#[derive(Debug, Clone, PartialEq)]
pub struct Reference {
    pub uri: String,
    pub target: Target,
}

/// What a reference names, keyed the way Nexus keys it, whichever epoch's spelling the URI uses.
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    Post {
        author_id: PubkyId,
        post_id: String,
    },
    User(PubkyId),
    File {
        owner_id: PubkyId,
        file_id: String,
    },
    /// A v0 blob: the bytes a v0 file points at.
    Blob {
        owner_id: PubkyId,
        blob_id: String,
    },
    /// A tag file. `app` is the folder of another app's universal tag, `None` for a `pubky.app` or
    /// `social` tag.
    Tag {
        tagger_id: PubkyId,
        tag_id: String,
        app: Option<String>,
    },
    /// Another resource of `pubky.app` or `social`, one Nexus keys nothing by: a follow, a mute, a
    /// bookmark, a feed, or a path naming no resource.
    Unkeyed {
        owner_id: PubkyId,
    },
    /// Nothing Nexus reads: another app's path, a URI of another scheme, a `social` epoch this
    /// build does not speak, or no URI at all.
    External,
}

impl Reference {
    /// Whether the reference names a post.
    pub fn names_a_post(&self) -> bool {
        matches!(self.target, Target::Post { .. })
    }

    /// The `(author_id, post_id)` of the post the reference names.
    pub fn post_key(&self) -> Option<(PubkyId, String)> {
        match &self.target {
            Target::Post { author_id, post_id } => Some((author_id.clone(), post_id.clone())),
            _ => None,
        }
    }
}

impl Target {
    /// The user the named resource belongs to.
    pub fn owner_id(&self) -> Option<&PubkyId> {
        match self {
            Target::Post { author_id, .. } => Some(author_id),
            Target::User(user_id) => Some(user_id),
            Target::File { owner_id, .. }
            | Target::Blob { owner_id, .. }
            | Target::Unkeyed { owner_id } => Some(owner_id),
            Target::Tag { tagger_id, .. } => Some(tagger_id),
            Target::External => None,
        }
    }
}

/// A post as the post handler reads it.
#[derive(Debug, Clone)]
pub struct PostInput {
    pub content: String,
    pub kind: PostKind,
    /// The post this one replies to.
    pub parent: Option<Reference>,
    /// The embedded object, the only part of an embed Nexus reads.
    pub embed: Option<Reference>,
    pub attachments: Option<Vec<String>>,
    pub lock: Option<String>,
}

impl PostInput {
    /// `uri` is the address the post was read from: the event path.
    pub fn into_details(self, uri: String, author_id: &PubkyId, post_id: &str) -> PostDetails {
        PostDetails {
            uri,
            content: self.content,
            id: post_id.to_string(),
            indexed_at: Utc::now().timestamp_millis(),
            author: author_id.to_string(),
            kind: self.kind,
            attachments: self.attachments,
            lock: self.lock,
            deleted: false,
        }
    }

    /// The parent, when it names a post: only a post can be replied to.
    pub fn replied(&self) -> Option<&Reference> {
        self.parent.as_ref().filter(|parent| parent.names_a_post())
    }

    /// The embed, when it names a post: only a post can be reposted; other embeds stay plain
    /// embeds.
    pub fn reposted(&self) -> Option<&Reference> {
        self.embed.as_ref().filter(|embed| embed.names_a_post())
    }

    /// Whether the parent names a resource other than a post, which no reply can point at. A parent
    /// naming nothing Nexus reads is not one: such a post is indexed as a reply to no post.
    pub fn replies_to_a_non_post(&self) -> bool {
        self.parent.as_ref().is_some_and(|parent| {
            !matches!(
                parent.target,
                Target::Post { .. } | Target::External | Target::Tag { app: Some(_), .. }
            )
        })
    }

    /// The URIs of the post this one replies to and the post it reposts, as the post wrote them.
    pub fn relationships(&self) -> PostRelationships {
        PostRelationships {
            replied: self.replied().map(|parent| parent.uri.clone()),
            reposted: self.reposted().map(|embed| embed.uri.clone()),
            mentioned: Vec::new(),
        }
    }

    /// The posts the graph write links this one to.
    pub fn links(&self) -> PostLinks {
        PostLinks {
            replied: self.replied().and_then(Reference::post_key),
            reposted: self.reposted().and_then(Reference::post_key),
        }
    }
}

/// A bookmark as the bookmark handler reads it.
#[derive(Debug, Clone)]
pub struct BookmarkInput {
    /// The bookmarked post.
    pub target: Reference,
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
    /// The tagged object.
    pub target: Reference,
    pub label: String,
}

/// A file as the file handler reads it.
#[derive(Debug, Clone)]
pub struct FileInput {
    pub name: String,
    /// The blob holding the bytes.
    pub src: Reference,
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
            src: self.src.uri,
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

    const AUTHOR: &str = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    const POST_ID: &str = "003286NSMY490";
    const POST_URI: &str =
        "pubky://4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro/pub/pubky.app/posts/003286NSMY490";

    fn author() -> PubkyId {
        PubkyId::try_from(AUTHOR).unwrap()
    }

    fn reference(target: Target) -> Reference {
        Reference {
            uri: POST_URI.to_string(),
            target,
        }
    }

    fn post() -> Target {
        Target::Post {
            author_id: author(),
            post_id: POST_ID.to_string(),
        }
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

        let details = post.into_details(POST_URI.to_string(), &author(), POST_ID);
        assert_eq!(details.attachments, None);
        assert_eq!(details.uri, POST_URI);
        assert!(!details.deleted);
    }

    #[test]
    fn post_embed_becomes_a_repost() {
        let post = PostInput {
            embed: Some(reference(post())),
            ..post_without_relations()
        };
        assert_eq!(post.relationships().reposted.as_deref(), Some(POST_URI));
        assert_eq!(post.links().reposted, Some((author(), POST_ID.to_string())));
    }

    #[test]
    fn non_post_embed_is_not_a_repost() {
        let file = Target::File {
            owner_id: author(),
            file_id: POST_ID.to_string(),
        };
        let post = PostInput {
            embed: Some(reference(file)),
            ..post_without_relations()
        };
        assert!(
            post.relationships().reposted.is_none(),
            "a non-post embed must not become a repost"
        );
        assert!(post.links().reposted.is_none());
    }

    #[test]
    fn post_parent_becomes_a_reply() {
        let post = PostInput {
            parent: Some(reference(post())),
            ..post_without_relations()
        };
        assert_eq!(post.relationships().replied.as_deref(), Some(POST_URI));
        assert_eq!(post.links().replied, Some((author(), POST_ID.to_string())));
        assert!(!post.replies_to_a_non_post());
    }

    /// A parent naming nothing Nexus reads leaves a reply to no post; one naming a resource of
    /// another kind cannot be indexed at all.
    #[test]
    fn non_post_parents() {
        let universal_tag = Target::Tag {
            tagger_id: author(),
            tag_id: "ABC".into(),
            app: Some("mapky".into()),
        };
        for target in [Target::External, universal_tag] {
            let post = PostInput {
                parent: Some(reference(target.clone())),
                ..post_without_relations()
            };
            assert!(post.relationships().replied.is_none(), "{target:?}");
            assert!(!post.replies_to_a_non_post(), "{target:?}");
        }
        for target in [
            Target::User(author()),
            Target::Unkeyed { owner_id: author() },
        ] {
            let post = PostInput {
                parent: Some(reference(target.clone())),
                ..post_without_relations()
            };
            assert!(post.replies_to_a_non_post(), "{target:?}");
        }
    }
}
