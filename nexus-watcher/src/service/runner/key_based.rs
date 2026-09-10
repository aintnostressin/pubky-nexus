use super::{TEventProcessorRunner, UserNotFoundBackoff};
use crate::events::retry::RetryScheduler;
use crate::events::{DefaultEventHandler, EventHandler};
use crate::service::indexer::{
    KeyBasedEventProcessor, KeyBasedEventSource, PubkyKeyBasedEventSource, TEventProcessor,
};
use crate::service::runner::key_based_hs_backoff::HomeserverBackoff;
use crate::service::stats::{ProcessedStats, ProcessorRunStatus, RunAllProcessorsStats};
use nexus_common::models::homeserver::{Homeserver, HsBlacklist};
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use opentelemetry::global;
use opentelemetry::metrics::Gauge;
use pubky_app_specs::PubkyId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::sync::{watch::Receiver, Mutex};
use tracing::{debug, info, warn};

/// Active homeservers eligible for polling, before [`WatcherConfig::monitored_homeservers_limit`]
/// is applied.
static ELIGIBLE_HS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter(crate::service::indexer::METER_NAME)
        .u64_gauge("watcher.monitored_hs.eligible")
        .with_description("Active homeservers eligible for polling before the monitored limit")
        .build()
});

/// The configured ceiling, published alongside the eligible count so a dashboard
/// can compute headroom without hard-coding the operator's config.
static MONITORED_HS_LIMIT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter(crate::service::indexer::METER_NAME)
        .u64_gauge("watcher.monitored_hs.limit")
        .with_description("Configured maximum number of monitored homeservers")
        .build()
});

/// Homeservers cut by the limit this cycle.
///
/// Any non-zero value means those homeservers are not polled *at all*, so their
/// users' events never arrive — not that they are polled later.
static DROPPED_HS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter(crate::service::indexer::METER_NAME)
        .u64_gauge("watcher.monitored_hs.dropped")
        .with_description("Eligible homeservers dropped by the monitored limit")
        .build()
});

/// Last reported drop count, so a sustained overflow logs once instead of on
/// every cycle. Process-global rather than a struct field: there is one runner
/// per process, and a field would change the struct literal every test builds.
static LAST_DROPPED: AtomicUsize = AtomicUsize::new(0);

/// What the log should say about the limit, given this cycle's drop count and
/// the previously reported one.
///
/// Split out from the emitting code so the interesting part — say it once, and
/// say when it clears — is testable without standing up a meter.
#[derive(Debug, PartialEq, Eq)]
enum LimitReport {
    /// Unchanged since the last cycle; already reported.
    Silent,
    /// Newly over the limit, or the overflow grew.
    Reached { eligible: usize, dropped: usize },
    /// Back within the limit after having exceeded it.
    Recovered { eligible: usize },
}

fn classify_limit(eligible: usize, dropped: usize, previous: usize) -> LimitReport {
    if dropped == previous {
        LimitReport::Silent
    } else if dropped > 0 {
        LimitReport::Reached { eligible, dropped }
    } else {
        LimitReport::Recovered { eligible }
    }
}

/// Runner for [KeyBasedEventProcessor]
pub struct KeyBasedEventProcessorRunner {
    /// See [WatcherConfig::key_based_events_limit]
    pub limit: u16,

    /// See [WatcherConfig::monitored_homeservers_limit]
    pub monitored_hs_limit: usize,

    pub event_handler: Arc<dyn EventHandler>,
    pub event_source: Arc<dyn KeyBasedEventSource>,
    pub shutdown_rx: Receiver<bool>,

    /// Primary homeserver ID, excluded from the external targets list
    pub primary_homeserver: PubkyId,

    /// HS PKs that must never be indexed. Excluded from `pre_run` and re-checked
    /// by each [`KeyBasedEventProcessor`] this runner builds.
    pub hs_blacklist: HsBlacklist,

    /// Per-target exponential backoff state
    pub backoff: Mutex<HomeserverBackoff>,

    pub user_not_found_backoff: Arc<UserNotFoundBackoff>,

    /// Scheduler shared with every processor this runner builds
    pub retry_scheduler: Arc<RetryScheduler>,
}

impl KeyBasedEventProcessorRunner {
    /// Creates a new instance from the provided configuration
    pub fn from_config(config: &WatcherConfig, shutdown_rx: Receiver<bool>) -> Self {
        Self {
            limit: config.key_based_events_limit,
            monitored_hs_limit: config.monitored_homeservers_limit,
            event_handler: Arc::new(DefaultEventHandler::from_config(config)),
            event_source: Arc::new(PubkyKeyBasedEventSource),
            shutdown_rx,
            primary_homeserver: config.homeserver.clone(),
            hs_blacklist: HsBlacklist::from_config(&config.stack),
            backoff: Mutex::new(HomeserverBackoff::new(
                config.initial_backoff_secs,
                config.max_backoff_secs,
            )),
            user_not_found_backoff: Arc::new(UserNotFoundBackoff::default()),
            retry_scheduler: Arc::new(RetryScheduler::from_config(config)),
        }
    }

    /// Returns the HS IDs relevant for this run, ordered by their priority.
    async fn hs_by_priority(&self) -> Result<Vec<String>, DynError> {
        let active_hs_ids = Homeserver::get_all_active_from_graph().await?;

        let result_hs_ids: Vec<String> = active_hs_ids
            .into_iter()
            // Exclude the primary HS, as it is processed separately
            .filter(|hs_id| hs_id != self.primary_homeserver.as_ref())
            // Exclude any blacklisted HS
            .filter(|hs_id| !self.hs_blacklist.is_blacklisted(hs_id))
            .collect();

        Ok(result_hs_ids)
    }

    /// Publishes how close the eligible homeserver set is to the configured limit.
    ///
    /// The truncation in [`Self::pre_run`] is silent: a homeserver past the limit
    /// is not polled late, it is not polled at all. Nothing reported that, so both
    /// "approaching the ceiling" and "already dropping homeservers" were invisible.
    ///
    /// Metrics are disabled by default, so this logs as well as measures —
    /// otherwise the common deployment learns nothing. The log is emitted only
    /// when the drop count changes, or a saturated instance would warn on every
    /// cycle (every `external_hs_monitoring_interval_ms`, 5s by default).
    fn report_monitored_hs(&self, eligible: usize, dropped: usize) {
        ELIGIBLE_HS.record(eligible as u64, &[]);
        MONITORED_HS_LIMIT.record(self.monitored_hs_limit as u64, &[]);
        DROPPED_HS.record(dropped as u64, &[]);

        let previous = LAST_DROPPED.swap(dropped, Ordering::Relaxed);
        match classify_limit(eligible, dropped, previous) {
            LimitReport::Silent => {}
            LimitReport::Reached { eligible, dropped } => warn!(
                eligible,
                limit = self.monitored_hs_limit,
                dropped,
                "Monitored homeserver limit reached; these homeservers are not polled at all"
            ),
            LimitReport::Recovered { eligible } => info!(
                eligible,
                limit = self.monitored_hs_limit,
                "Monitored homeserver limit no longer exceeded; all eligible homeservers are polled"
            ),
        }
    }
}

#[async_trait::async_trait]
impl TEventProcessorRunner for KeyBasedEventProcessorRunner {
    fn shutdown_rx(&self) -> Receiver<bool> {
        self.shutdown_rx.clone()
    }

    async fn build(&self, hs_id: &str) -> Result<Arc<dyn TEventProcessor>, DynError> {
        let homeserver_id = PubkyId::try_from(hs_id)?;

        Ok(Arc::new(KeyBasedEventProcessor {
            homeserver_id,
            limit: self.limit,
            event_handler: self.event_handler.clone(),
            event_source: self.event_source.clone(),
            user_not_found_backoff: self.user_not_found_backoff.clone(),
            retry_scheduler: self.retry_scheduler.clone(),
            hs_blacklist: self.hs_blacklist.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
        }))
    }

    async fn pre_run(&self) -> Result<Vec<String>, DynError> {
        let mut hs_ids = self.hs_by_priority().await?;
        let eligible = hs_ids.len();
        hs_ids.truncate(self.monitored_hs_limit);
        self.report_monitored_hs(eligible, eligible.saturating_sub(hs_ids.len()));
        Ok(hs_ids)
    }

    async fn backoff_hs_should_skip(&self, hs_id: &str) -> bool {
        let backoff = self.backoff.lock().await;
        backoff.should_skip(hs_id)
    }

    async fn backoff_hs_record_result(&self, hs_id: &str, status: &ProcessorRunStatus) {
        let mut backoff = self.backoff.lock().await;
        if *status == ProcessorRunStatus::Ok {
            backoff.record_success(hs_id);
        } else {
            backoff.record_failure(hs_id);
        }
    }

    async fn post_run(&self, stats: RunAllProcessorsStats) -> ProcessedStats {
        for individual_run_stat in &stats.stats {
            let hs_id = &individual_run_stat.hs_id;
            let duration = individual_run_stat.duration;
            let status = &individual_run_stat.status;
            debug!(homeserver = %hs_id, ?duration, ?status, "Event processor run completed");
        }

        let count_ok = stats.count_ok();
        let count_error = stats.count_error();
        let count_panic = stats.count_panic();
        let count_timeout = stats.count_timeout();
        let count_failed_to_build = stats.count_failed_to_build();
        let count_skipped = stats.count_skipped();
        let had_issues = count_error + count_panic + count_timeout + count_failed_to_build > 0;

        if had_issues {
            warn!(
                hs_ok = count_ok,
                hs_skipped = count_skipped,
                hs_failed_to_build = count_failed_to_build,
                hs_error = count_error,
                hs_panic = count_panic,
                hs_timeout = count_timeout,
                "Key-based indexing finished with issues"
            );
        } else if count_skipped > 0 {
            warn!(
                hs_ok = count_ok,
                hs_skipped = count_skipped,
                "Key-based indexing finished; some homeservers skipped (backoff)"
            );
        } else if count_ok == 0 {
            info!("Key-based indexing finished: no external homeservers");
        } else {
            info!(hs_ok = count_ok, "Key-based indexing finished");
        }

        ProcessedStats(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_limit, LimitReport};

    #[test]
    fn within_the_limit_says_nothing() {
        assert_eq!(classify_limit(12, 0, 0), LimitReport::Silent);
    }

    #[test]
    fn crossing_the_limit_reports_once() {
        assert_eq!(
            classify_limit(53, 3, 0),
            LimitReport::Reached {
                eligible: 53,
                dropped: 3
            }
        );
        // Same overflow next cycle: already said, stay quiet rather than warn
        // every few seconds for as long as the instance is saturated.
        assert_eq!(classify_limit(53, 3, 3), LimitReport::Silent);
    }

    #[test]
    fn a_growing_overflow_reports_again() {
        assert_eq!(
            classify_limit(60, 10, 3),
            LimitReport::Reached {
                eligible: 60,
                dropped: 10
            }
        );
    }

    // Without this the operator sees the warning but never learns it cleared.
    #[test]
    fn dropping_back_under_the_limit_reports_recovery() {
        assert_eq!(
            classify_limit(48, 0, 3),
            LimitReport::Recovered { eligible: 48 }
        );
    }
}
