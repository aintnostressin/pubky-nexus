use serde::{Deserialize, Serialize};

use crate::db::kv::RedisResult;
use crate::models::post::trust_filter;
use crate::models::tag::stream::HotTagsRule;

/// App feature switches (`[features]`), all off by default. The watcher reads
/// them at startup and stores them in Redis, where every service follows them:
/// the API and the jobs don't read `[features]`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    /// Hide posts by authors outside the trust ranking from the streams every
    /// viewer shares.
    pub hide_unranked_authors: bool,
    /// Rank hot tags by tagger trust instead of by post count, see
    /// [`HotTagsRule::Trust`].
    pub trust_ranked_hot_tags: bool,
}

impl FeaturesConfig {
    /// Stores the switches in Redis, where every service reads them.
    pub async fn store(&self) -> RedisResult<()> {
        trust_filter::switch(self.hide_unranked_authors).await?;
        HotTagsRule::switch(self.trust_ranked_hot_tags).await
    }

    /// Stores the switches, then brings the stored data in line with them, so a
    /// feature toggled since the last start takes effect now. Needs the stack
    /// set up. Fails only if the switches can't be stored: a failed update is
    /// logged and left to the next trust recompute, which retries it.
    pub async fn toggle(&self) -> RedisResult<()> {
        self.store().await?;
        if let Err(error) = trust_filter::reconcile().await {
            tracing::error!(%error, "Failed to bring the shared post sets in line with the features");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FeaturesConfig;
    use crate::models::tag::stream::HotTagsRule;
    use crate::{types::DynError, StackConfig, StackManager};

    /// The hot-tags switch reaches every service through Redis. It ends on, as
    /// the mock data stores it.
    #[tokio_shared_rt::test(shared)]
    async fn store_round_trips_the_hot_tags_switch() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let features = |trust_ranked_hot_tags| FeaturesConfig {
            hide_unranked_authors: true,
            trust_ranked_hot_tags,
        };

        let off = features(false).store().await;
        let stored_off = HotTagsRule::stored().await;
        let on = features(true).store().await;
        let stored_on = HotTagsRule::stored().await;

        off?;
        on?;
        assert_eq!(stored_off?, HotTagsRule::Posts);
        assert_eq!(stored_on?, HotTagsRule::Trust);
        Ok(())
    }
}
