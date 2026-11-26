//! The two layouts a user's public objects can live in on their homeserver,
//! and how a client writes each kind of object in either. The users of one
//! test all write the same layout, so a scenario reads the same for both.
//!
//! Nexus reads a user through one layout: `pubky.app` until it has indexed
//! their `social/v1/profile.json`, `social/v1` from then on, and it ignores
//! what the user writes in the other.

use anyhow::Result;

/// One object as a client stores it: its owner-relative path and its bytes.
pub type Object = (String, Vec<u8>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// `/pub/pubky.app/`, the legacy layout.
    PubkyApp,
    /// `/pub/social/v1/`. Bookmarks are private there (`/priv/social/v1/`),
    /// which no event stream carries, so nexus never sees one.
    SocialV1,
}

impl Layout {
    /// The address of the folder holding every public object `user_id` wrote
    /// in this layout.
    pub fn tree_root(self, user_id: String) -> String {
        match self {
            Self::PubkyApp => pubky_app::tree_root(user_id),
            Self::SocialV1 => social_v1::tree_root(user_id),
        }
    }

    pub fn profile_path(self) -> String {
        match self {
            Self::PubkyApp => pubky_app::profile_path(),
            Self::SocialV1 => social_v1::profile_path(),
        }
    }

    pub fn profile(self, name: &str) -> Result<Object> {
        match self {
            Self::PubkyApp => pubky_app::profile(name),
            Self::SocialV1 => social_v1::profile(name),
        }
    }

    /// A short post, a reply when `parent` is given, with the id it is stored under.
    pub fn post(self, text: &str, parent: Option<String>) -> Result<(String, Object)> {
        match self {
            Self::PubkyApp => pubky_app::post(text, parent),
            Self::SocialV1 => social_v1::post(text, parent),
        }
    }

    /// Where the post `post_id` is stored, as [`Self::post`] writes it.
    pub fn post_path(self, post_id: &str) -> String {
        match self {
            Self::PubkyApp => pubky_app::post_path(post_id),
            Self::SocialV1 => social_v1::post_path(post_id),
        }
    }

    /// The address a reply, a tag or a bookmark names the post by.
    pub fn post_uri(self, author_id: String, post_id: &str) -> String {
        match self {
            Self::PubkyApp => pubky_app::post_uri(author_id, post_id),
            Self::SocialV1 => social_v1::post_uri(author_id, post_id),
        }
    }

    pub fn follow(self, target: &str) -> Result<Object> {
        match self {
            Self::PubkyApp => pubky_app::follow(target),
            Self::SocialV1 => social_v1::follow(target),
        }
    }

    pub fn tag(self, uri: String, label: &str) -> Result<Object> {
        match self {
            Self::PubkyApp => pubky_app::tag(uri, label),
            Self::SocialV1 => social_v1::tag(uri, label),
        }
    }

    /// An upload of `bytes` by `owner_id`, in the order the app writes it: in
    /// `pubky.app` a blob and the file record naming it, in `social/v1` the
    /// one file, named by the hash of its bytes.
    pub fn upload(self, owner_id: String, bytes: &[u8]) -> Result<Vec<Object>> {
        match self {
            Self::PubkyApp => pubky_app::upload(owner_id, bytes),
            Self::SocialV1 => Ok(vec![social_v1::file(bytes)]),
        }
    }

    /// What `MigrationTest::files` reports while nexus holds `uploads` of a
    /// user: a record and a stored file for each in `pubky.app`, only the
    /// stored file in `social/v1`, where a file has no record.
    pub fn indexed_uploads(self, uploads: usize) -> (i64, usize) {
        match self {
            Self::PubkyApp => (uploads as i64, uploads),
            Self::SocialV1 => (0, uploads),
        }
    }
}

/// Declares each named scenario of the calling module, an
/// `async fn(Layout) -> Result<()>`, as two tests of its name:
/// `pubky_app::<name>` and `social_v1::<name>`. A scenario that holds in one
/// layout only goes after `social_v1(ignore = "what must exist")`, which
/// ignores its `social/v1` test for that reason.
macro_rules! in_each_layout {
    ($($(social_v1(ignore = $reason:literal))? $scenario:ident),+ $(,)?) => {
        mod pubky_app {
            $(
                #[tokio_shared_rt::test(shared)]
                async fn $scenario() -> anyhow::Result<()> {
                    super::$scenario($crate::layout::Layout::PubkyApp).await
                }
            )+
        }

        mod social_v1 {
            $(
                #[tokio_shared_rt::test(shared)]
                $(#[ignore = $reason])?
                async fn $scenario() -> anyhow::Result<()> {
                    super::$scenario($crate::layout::Layout::SocialV1).await
                }
            )+
        }
    };
}
pub(crate) use in_each_layout;

fn object(path: String, body: &impl serde::Serialize) -> Result<Object> {
    Ok((path, serde_json::to_vec(body)?))
}

const BIO: &str = "migration suite";

mod pubky_app {
    use anyhow::Result;
    use pubky_social_specs::legacy_v0::traits::{HasIdPath, HasPath, HashId, TimestampId};
    use pubky_social_specs::legacy_v0::{
        blob_uri_builder, post_uri_builder, PubkyAppBlob, PubkyAppFile, PubkyAppFollow,
        PubkyAppPost, PubkyAppPostKind, PubkyAppTag, PubkyAppUser, APP_PATH, PROTOCOL, PUBLIC_PATH,
    };

    use super::{object, Object, BIO};

    pub fn tree_root(user_id: String) -> String {
        [PROTOCOL, &user_id, PUBLIC_PATH, APP_PATH].concat()
    }

    pub fn profile_path() -> String {
        PubkyAppUser::create_path()
    }

    pub fn profile(name: &str) -> Result<Object> {
        let profile = PubkyAppUser {
            name: name.to_string(),
            bio: Some(BIO.to_string()),
            image: None,
            links: None,
            status: None,
        };
        object(profile_path(), &profile)
    }

    pub fn post(text: &str, parent: Option<String>) -> Result<(String, Object)> {
        let post = PubkyAppPost::new(
            text.to_string(),
            PubkyAppPostKind::Short,
            parent,
            None,
            None,
        );
        let id = post.create_id();
        let stored = object(post_path(&id), &post)?;
        Ok((id, stored))
    }

    pub fn post_path(post_id: &str) -> String {
        PubkyAppPost::create_path(post_id)
    }

    pub fn post_uri(author_id: String, post_id: &str) -> String {
        post_uri_builder(author_id, post_id.to_string())
    }

    pub fn follow(target: &str) -> Result<Object> {
        let follow = PubkyAppFollow {
            created_at: chrono::Utc::now().timestamp_millis(),
        };
        object(PubkyAppFollow::create_path(target), &follow)
    }

    pub fn tag(uri: String, label: &str) -> Result<Object> {
        let tag = PubkyAppTag::new(uri, label.to_string());
        object(PubkyAppTag::create_path(&tag.create_id()), &tag)
    }

    pub fn upload(owner_id: String, bytes: &[u8]) -> Result<Vec<Object>> {
        let blob = PubkyAppBlob::new(bytes.to_vec());
        let hash = blob.create_id();
        let file = PubkyAppFile {
            name: "picture.png".to_string(),
            content_type: "image/png".to_string(),
            src: blob_uri_builder(owner_id, hash.clone()),
            size: bytes.len(),
            created_at: chrono::Utc::now().timestamp_millis(),
        };
        let record = object(PubkyAppFile::create_path(&file.create_id()), &file)?;
        Ok(vec![(PubkyAppBlob::create_path(&hash), blob.0), record])
    }
}

/// Objects built as the watcher's event tests build them
/// (`tests/event_processor`: `put_v1_*` in `utils/watcher.rs`, `V1File` in
/// `files/social_v1.rs`).
mod social_v1 {
    use anyhow::Result;
    use pubky_social_specs::traits::{HasIdPath, HasPath, HashId, TimestampId};
    use pubky_social_specs::{
        list_prefix_builder, post_uri_builder, PubkySocialFile, PubkySocialFollow, PubkySocialPost,
        PubkySocialPostKind, PubkySocialTag, PubkySocialUser,
    };

    use super::{object, Object, BIO};

    pub fn tree_root(user_id: String) -> String {
        list_prefix_builder(user_id)
    }

    pub fn profile_path() -> String {
        PubkySocialUser::create_path()
    }

    pub fn profile(name: &str) -> Result<Object> {
        let profile =
            PubkySocialUser::new(name.to_string(), Some(BIO.to_string()), None, None, None);
        object(profile_path(), &profile)
    }

    /// The post's first version, whose edit id is the post id.
    pub fn post(text: &str, parent: Option<String>) -> Result<(String, Object)> {
        let post = PubkySocialPost::new(
            text.to_string(),
            PubkySocialPostKind::Note,
            parent,
            None,
            vec![],
        );
        let id = post.create_id();
        let stored = object(post_path(&id), &post)?;
        Ok((id, stored))
    }

    pub fn post_path(post_id: &str) -> String {
        PubkySocialPost::create_path(post_id)
    }

    /// The versionless address every reference to a post uses.
    pub fn post_uri(author_id: String, post_id: &str) -> String {
        post_uri_builder(author_id, post_id.to_string())
    }

    pub fn follow(target: &str) -> Result<Object> {
        object(
            PubkySocialFollow::create_path(target),
            &PubkySocialFollow::new(),
        )
    }

    pub fn tag(uri: String, label: &str) -> Result<Object> {
        let tag = PubkySocialTag::new(uri, label.to_string());
        object(PubkySocialTag::create_path(&tag.create_id()), &tag)
    }

    pub fn file(bytes: &[u8]) -> Object {
        let hash = PubkySocialFile(bytes.to_vec()).create_id();
        let path = PubkySocialFile::create_path(&format!("{hash}.png"));
        (path, bytes.to_vec())
    }
}
