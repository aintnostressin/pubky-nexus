use std::path::Path;
use std::sync::Arc;

use crate::errors::EventProcessorError;
use crate::events::handlers::{self, TagInput, Target};
use nexus_common::models::user::UserIngestor;
use nexus_common::WatcherConfig;
use pubky_social_specs::PubkyId;
use tracing::info;

pub struct Moderation {
    /// Moderator trusted user id
    pub id: PubkyId,
    /// Tags to be moderated (tagged content is deleted)
    pub tags: Vec<String>,
}

impl Moderation {
    pub fn from_config(config: &WatcherConfig) -> Arc<Self> {
        Arc::new(Self {
            id: config.moderation_id.clone(),
            tags: config.moderated_tags.clone(),
        })
    }

    /// Check if a tag should trigger deletion of the tagged content.
    ///
    /// Returns `true` if the tag was applied by the moderator and matches
    /// a moderated tag label.
    pub fn should_delete(&self, tag: &TagInput, tagger_id: &PubkyId) -> bool {
        tagger_id == &self.id && self.tags.contains(&tag.label)
    }

    /// Apply moderation to the tagged resource.
    ///
    /// Removes the resource the moderator tag names (a tag, a post, a user profile or a file).
    /// Posts are hard-deleted when edge-free or tombstoned when engagement edges remain.
    #[tracing::instrument(name = "moderation.apply", skip_all)]
    pub async fn apply_moderation(
        &self,
        moderator_tag: TagInput,
        files_path: &Path,
        ingestor: &UserIngestor,
    ) -> Result<(), EventProcessorError> {
        let label = moderator_tag.label;
        let moderated = moderator_tag.target;

        match moderated.target {
            Target::Tag {
                tagger_id,
                tag_id,
                app,
            } => {
                info!(
                    "Moderation tag '{label}' detected. Deleting moderated tag {}",
                    moderated.uri
                );
                handlers::tag::del(tagger_id, tag_id, app).await
            }
            Target::Post { author_id, post_id } => {
                info!("Moderation tag '{label}' detected. Deleting post {author_id}:{post_id}");
                // Same gate as user-issued DEL events: hard-delete when edge-free, tombstone
                // when any engagement edge remains (tags, bookmarks, replies, reposts, mentions).
                handlers::post::del(author_id, post_id, ingestor).await
            }
            Target::User(user_id) => {
                info!("Moderation tag '{label}' detected. Deleting user profile {user_id}");
                handlers::user::del(user_id).await
            }
            Target::File { owner_id, file_id } => {
                info!("Moderation tag '{label}' detected. Deleting file {owner_id}:{file_id}");
                handlers::file::del(&owner_id, file_id, files_path).await
            }
            Target::Blob { .. } | Target::Unkeyed { .. } => Ok(()),
            Target::External => Err(EventProcessorError::generic(format!(
                "Moderated URI names nothing Nexus indexes: {}",
                moderated.uri
            ))),
        }
    }
}
