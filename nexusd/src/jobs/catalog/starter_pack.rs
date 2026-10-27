use async_trait::async_trait;
use nexus_common::config::watcher::MODERATED_TAGS;
use nexus_common::models::tag::stream::{HotTags, GLOBAL_HOT_TAGS_CACHE_SIZE};
use nexus_common::models::tag::TaggedType;
use nexus_common::models::user::StarterPackRank;
use nexus_common::types::routes::HotTagsInputDTO;
use nexus_common::types::Timeframe;
use std::error::Error;
use tracing::warn;

use crate::jobs::Job;

/// Pre-warm the starter pack ranking of every this-month hot tag, the labels onboarding offers,
/// so the first person to pick one does not wait for it to be built. A request builds a missing
/// ranking itself, so the job is opt-in.
pub struct StarterPackCacheJob;

#[async_trait]
impl Job for StarterPackCacheJob {
    fn name(&self) -> &'static str {
        "starter-pack-cache"
    }

    /// Tries every label, so one failure leaves no other cold, and reports the first failure.
    async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Only the labels are needed, not their taggers.
        let input = HotTagsInputDTO::new(
            Timeframe::ThisMonth,
            GLOBAL_HOT_TAGS_CACHE_SIZE,
            0,
            0,
            Some(TaggedType::Post),
        );
        let hot_tags = HotTags::get_hot_tags(None, None, &input)
            .await?
            .unwrap_or_default();

        let mut first_error = None;
        // The API rejects moderation labels, so nobody reads their rankings.
        for tag in hot_tags
            .iter()
            .filter(|tag| !MODERATED_TAGS.contains(&tag.label.as_str()))
        {
            if let Err(e) = StarterPackRank::compute_and_cache(&tag.label).await {
                warn!(label = %tag.label, error = ?e, "Failed to cache the starter pack ranking");
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }
}
