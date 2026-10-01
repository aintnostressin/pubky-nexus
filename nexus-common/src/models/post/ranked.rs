//! Ranked copies of the sorted sets behind `source=all` with `sorting=timeline`:
//! the same members at the same scores, keeping only posts whose author is in
//! the trust ranking (`Sorted:Users:SocialGraph`, built by the trust job).
//!
//! One ranked set mirrors the global timeline and one mirrors each per-label
//! timeline. Serving a filtered stream from a pre-filtered set keeps paging
//! exact: a page is only short at the real end of the stream, which matters
//! because pubky-app treats a short page as the end of the feed.
//!
//! Writes mirror into the ranked sets as they happen, and every ranking
//! publish rebuilds them in full ([`rebuild`]), so drift lasts at most until
//! the next recompute. Readers only trust the ranked sets once a complete
//! rebuild has run (`built_at`); before that they serve the unfiltered sets.
//!
//! The scripts take several keys, so they assume a single Redis instance (not
//! Redis Cluster), as the rest of Nexus does.

use std::collections::BTreeSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::global;
use opentelemetry::metrics::Counter;
use redis::{AsyncCommands, Script};

use super::search::TAG_GLOBAL_POST_TIMELINE;
use super::stream::POST_TIMELINE_KEY_PARTS;
use crate::db::get_redis_conn;
use crate::db::kv::{release_lock, try_acquire_lock, RedisError, RedisResult};
use crate::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;

/// Ranked copy of the global timeline: `Sorted:Posts:Global:Timeline:Ranked`.
pub const POST_RANKED_TIMELINE_KEY_PARTS: [&str; 4] = ["Posts", "Global", "Timeline", "Ranked"];
/// Ranked copies of the per-label timelines: `Sorted:Tags:Ranked:Post:Timeline:<label>`.
/// A different third segment from the source sets, so a scan for one never matches the other.
pub const TAG_RANKED_POST_TIMELINE: [&str; 4] = ["Tags", "Ranked", "Post", "Timeline"];

const SORTED: &str = "Sorted";

/// Sets up to this size are rebuilt by one atomic script.
const ATOMIC_MAX: usize = 500;
/// Members per `ZSCAN` page and per copy script in a staged build. Sized so a
/// script stays well under 10 ms of Redis time: at a million root posts a
/// 1,000-member batch occasionally took just over 10 ms.
const BATCH: usize = 500;
/// Keys per `SCAN` page when listing labels; cheap per key, so larger.
const SCAN_COUNT: usize = 1_000;
/// TTL on a staging set, refreshed by every batch, so a crashed build cannot
/// leave writes mirroring into it for long.
const STAGING_LEASE_SECS: u64 = 3_600;
/// Seeds a staging set so writes see the build from its first instant, before
/// any member is copied. It has no `:`, so it can never collide with an
/// `author:post` member.
const SENTINEL: &str = "~rebuild";
/// How long a rebuild may hold the lock before it frees itself.
const LOCK_TTL_SECS: u64 = 3_600;
/// How long a rebuild waits for a running one before giving up.
const LOCK_WAIT: Duration = Duration::from_secs(15 * 60);
const LOCK_POLL: Duration = Duration::from_secs(1);

/// Every key the ranked sets touch, so tests and benchmarks can run in a
/// private namespace.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RankedLayout {
    /// The trust ranking: users with a positive trust score.
    pub trust: String,
    /// The global timeline and its ranked copy.
    pub global: RankedSet,
    /// Key prefixes of the per-label sets; the label follows the prefix.
    pub tag_source_prefix: String,
    pub tag_ranked_prefix: String,
    pub tag_staging_prefix: String,
    pub tag_old_prefix: String,
    /// Set once a complete rebuild has run; readers trust the ranked sets only then.
    pub built_at: String,
    /// Serializes rebuilds.
    pub lock: String,
}

/// One source set and the keys of its ranked copy.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedSet {
    pub source: String,
    pub ranked: String,
    /// Where a staged build writes before the swap.
    pub staging: String,
    /// Where the swap moves the previous ranked set before freeing it.
    pub old: String,
}

static PRODUCTION: LazyLock<RankedLayout> = LazyLock::new(|| {
    let global_source = format!("{SORTED}:{}", POST_TIMELINE_KEY_PARTS.join(":"));
    let global_ranked = format!("{SORTED}:{}", POST_RANKED_TIMELINE_KEY_PARTS.join(":"));
    let [tags, ranked, post, timeline] = TAG_RANKED_POST_TIMELINE;
    RankedLayout {
        trust: format!("{SORTED}:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":")),
        global: RankedSet {
            staging: format!("{global_source}:RankedStaging"),
            old: format!("{global_source}:RankedOld"),
            source: global_source,
            ranked: global_ranked,
        },
        tag_source_prefix: format!("{SORTED}:{}:", TAG_GLOBAL_POST_TIMELINE.join(":")),
        tag_ranked_prefix: format!("{SORTED}:{tags}:{ranked}:{post}:{timeline}:"),
        tag_staging_prefix: format!("{SORTED}:{tags}:{ranked}Staging:{post}:{timeline}:"),
        tag_old_prefix: format!("{SORTED}:{tags}:{ranked}Old:{post}:{timeline}:"),
        built_at: "Ranked:Timeline:BuiltAt".to_string(),
        lock: "lock:ranked-timeline-rebuild".to_string(),
    }
});

impl RankedLayout {
    pub fn production() -> &'static RankedLayout {
        &PRODUCTION
    }

    /// The same key shapes as production under `ns`, so scans behave the same.
    pub fn namespaced(ns: &str) -> RankedLayout {
        RankedLayout {
            trust: format!("{ns}:Trust"),
            global: RankedSet {
                source: format!("{ns}:Timeline"),
                ranked: format!("{ns}:Timeline:Ranked"),
                staging: format!("{ns}:Timeline:RankedStaging"),
                old: format!("{ns}:Timeline:RankedOld"),
            },
            tag_source_prefix: format!("{ns}:Tags:Global:"),
            tag_ranked_prefix: format!("{ns}:Tags:Ranked:"),
            tag_staging_prefix: format!("{ns}:Tags:RankedStaging:"),
            tag_old_prefix: format!("{ns}:Tags:RankedOld:"),
            built_at: format!("{ns}:BuiltAt"),
            lock: format!("{ns}:Lock"),
        }
    }

    /// The per-label timeline for `label` and its ranked copy.
    pub fn tag(&self, label: &str) -> RankedSet {
        RankedSet {
            source: format!("{}{label}", self.tag_source_prefix),
            ranked: format!("{}{label}", self.tag_ranked_prefix),
            staging: format!("{}{label}", self.tag_staging_prefix),
            old: format!("{}{label}", self.tag_old_prefix),
        }
    }
}

/// How the trust ranking applies to one in-scope request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustMode {
    /// Served exactly as without the filter.
    Off,
    /// Filtered: Redis shapes read the ranked sets, Cypher shapes add the rule.
    Ranked,
    /// Filtered on Cypher only: the ranked sets have not been built yet, so the
    /// Redis shapes serve the unfiltered sets.
    Unbuilt,
}

/// Trust-ranking state for one request, read in one round trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RankedState {
    pub trust_exists: bool,
    pub built: bool,
    /// Whether the viewer is in the ranking; `None` when the request has no viewer.
    pub viewer_ranked: Option<bool>,
}

impl RankedState {
    pub async fn load(layout: &RankedLayout, viewer_id: Option<&str>) -> RedisResult<Self> {
        let mut pipe = redis::pipe();
        pipe.exists(&layout.trust).exists(&layout.built_at);
        let mut conn = get_redis_conn().await?;
        match viewer_id {
            Some(viewer_id) => {
                pipe.zscore(&layout.trust, viewer_id);
                let (trust_exists, built, viewer_score): (bool, bool, Option<f64>) =
                    pipe.query_async(&mut conn).await?;
                Ok(Self {
                    trust_exists,
                    built,
                    viewer_ranked: Some(viewer_score.is_some()),
                })
            }
            None => {
                let (trust_exists, built): (bool, bool) = pipe.query_async(&mut conn).await?;
                Ok(Self {
                    trust_exists,
                    built,
                    viewer_ranked: None,
                })
            }
        }
    }

    /// No ranking means no filtering at all. A viewer outside the ranking,
    /// including one Nexus does not know, gets the unfiltered stream.
    pub fn mode(&self) -> TrustMode {
        if !self.trust_exists || self.viewer_ranked == Some(false) {
            return TrustMode::Off;
        }
        if self.built {
            TrustMode::Ranked
        } else {
            TrustMode::Unbuilt
        }
    }
}

static UNBUILT_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("stream")
        .u64_counter("stream.posts.ranked_unbuilt")
        .with_description(
            "source=all timeline requests served unfiltered because the ranked sets are not built yet",
        )
        .build()
});

/// Counts a request served unfiltered because the ranked sets are not built yet.
pub(crate) fn record_unbuilt() {
    UNBUILT_REQUESTS.add(1, &[]);
}

/// Mirrors a member just written to a source set into its ranked copy, and into
/// the staging set while a build runs, when its author is in the ranking. The
/// score is read from the source set, so a retry or a late call can never write
/// a stale score.
static MIRROR_ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
          if not score then return 0 end
          if not redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 0 end
          redis.call('ZADD', KEYS[3], score, ARGV[2])
          if redis.call('EXISTS', KEYS[4]) == 1 then
              redis.call('ZADD', KEYS[4], score, ARGV[2])
          end
          return 1",
    )
});

/// Removes a member from a ranked set and its staging set in one step, so a
/// swap cannot land between the two removals and keep the member.
static MIRROR_REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"redis.call('ZREM', KEYS[1], ARGV[1])
          redis.call('ZREM', KEYS[2], ARGV[1])
          return 1",
    )
});

/// Rebuilds a small set in one atomic step, dropping any staging or old set a
/// crashed build left behind. Returns -1 when the source has grown past
/// `ARGV[1]` since it was sized, so the caller stages it instead.
static REBUILD_SMALL: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"if redis.call('ZCARD', KEYS[1]) > tonumber(ARGV[1]) then return -1 end
          redis.call('UNLINK', KEYS[4], KEYS[5])
          local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
          redis.call('DEL', KEYS[3])
          local copied = 0
          for i = 1, #entries, 2 do
              local member = entries[i]
              local sep = string.find(member, ':', 1, true)
              if sep and redis.call('ZSCORE', KEYS[2], string.sub(member, 1, sep - 1)) then
                  redis.call('ZADD', KEYS[3], entries[i + 1], member)
                  copied = copied + 1
              end
          end
          return copied",
    )
});

/// Copies one batch of a staged build. Members deleted from the source since
/// they were scanned are skipped, and each score is re-read from the source.
static COPY_BATCH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"local copied = 0
          for i = 2, #ARGV do
              local member = ARGV[i]
              local score = redis.call('ZSCORE', KEYS[1], member)
              if score then
                  local sep = string.find(member, ':', 1, true)
                  if sep and redis.call('ZSCORE', KEYS[2], string.sub(member, 1, sep - 1)) then
                      redis.call('ZADD', KEYS[3], score, member)
                      copied = copied + 1
                  end
              end
          end
          redis.call('EXPIRE', KEYS[3], ARGV[1])
          return copied",
    )
});

/// Installs a finished staging set: drops the sentinel, moves the current
/// ranked set aside (the caller frees it with `UNLINK`, off the main thread),
/// and renames the staging set into place. A staging set left empty means no
/// ranked member, so the ranked set ends up absent.
static SWAP: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"redis.call('ZREM', KEYS[1], ARGV[1])
          if redis.call('EXISTS', KEYS[2]) == 1 then
              redis.call('RENAME', KEYS[2], KEYS[3])
          end
          if redis.call('EXISTS', KEYS[1]) == 1 then
              redis.call('RENAME', KEYS[1], KEYS[2])
              redis.call('PERSIST', KEYS[2])
          end
          return 1",
    )
});

/// Mirrors `member` (`author:post`), just written to `set.source`, into the ranked copy.
pub(crate) async fn mirror_add(
    trust: &str,
    set: &RankedSet,
    author_id: &str,
    member: &str,
) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: i64 = MIRROR_ADD
        .key(&set.source)
        .key(trust)
        .key(&set.ranked)
        .key(&set.staging)
        .arg(author_id)
        .arg(member)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

/// Removes `member`, just removed from `set.source`, from the ranked copy.
pub(crate) async fn mirror_remove(set: &RankedSet, member: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: i64 = MIRROR_REMOVE
        .key(&set.ranked)
        .key(&set.staging)
        .arg(member)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

/// What a rebuild did, for its log line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RankedRebuildStats {
    /// Ranked sets rebuilt (the global one plus one per label).
    pub sets: usize,
    /// Source members examined.
    pub scanned: usize,
    /// Members written to a ranked set.
    pub copied: usize,
    /// Ranked sets removed because their source set no longer exists.
    pub orphans: usize,
    /// No ranking existed, so every ranked set was dropped instead.
    pub dropped: bool,
}

/// Rebuilds every ranked set from the current ranking, or drops them all when
/// there is no ranking. Waits for a rebuild already running, so the last
/// ranking publish is always followed by a complete rebuild.
#[doc(hidden)]
pub async fn rebuild(layout: &RankedLayout) -> RedisResult<RankedRebuildStats> {
    let token = lock_token();
    let deadline = Instant::now() + LOCK_WAIT;
    while !try_acquire_lock(&layout.lock, &token, LOCK_TTL_SECS).await? {
        if Instant::now() >= deadline {
            return Err(RedisError::CommandFailed(
                "timed out waiting for another ranked-set rebuild to finish".into(),
            ));
        }
        tokio::time::sleep(LOCK_POLL).await;
    }

    let result = rebuild_locked(layout).await;
    let released = release_lock(&layout.lock, &token).await;
    let stats = result?;
    released?;
    Ok(stats)
}

/// Unique per call: the lock only lets the holder of its token release it.
fn lock_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{}:{nanos}", std::process::id())
}

async fn rebuild_locked(layout: &RankedLayout) -> RedisResult<RankedRebuildStats> {
    let mut stats = RankedRebuildStats::default();
    let mut conn = get_redis_conn().await?;

    let trust_exists: bool = conn.exists(&layout.trust).await?;
    if !trust_exists {
        drop_all(layout).await?;
        stats.dropped = true;
        return Ok(stats);
    }

    let tags = scan_tag_keys(layout).await?;
    rebuild_set(&layout.trust, &layout.global, &mut stats).await?;
    for label in &tags.sources {
        rebuild_set(&layout.trust, &layout.tag(label), &mut stats).await?;
    }

    // A ranked set normally empties, and so vanishes, with its source; one that
    // outlived its source drifted. Re-checked here, as a label may have been
    // tagged again since the scan.
    for label in tags.ranked.difference(&tags.sources) {
        let set = layout.tag(label);
        let source_exists: bool = conn.exists(&set.source).await?;
        if !source_exists {
            let _: () = conn.unlink(&set.ranked).await?;
            stats.orphans += 1;
        }
    }
    // Left by a build that crashed; the lock guarantees none is running now.
    unlink_keys(tags.leftovers).await?;

    let built_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let _: () = conn.set(&layout.built_at, built_at).await?;
    Ok(stats)
}

async fn rebuild_set(
    trust: &str,
    set: &RankedSet,
    stats: &mut RankedRebuildStats,
) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    stats.sets += 1;

    let size: usize = conn.zcard(&set.source).await?;
    if size == 0 {
        let _: () = conn.unlink(&[&set.ranked, &set.staging, &set.old]).await?;
        return Ok(());
    }

    if size <= ATOMIC_MAX {
        let copied: i64 = REBUILD_SMALL
            .key(&set.source)
            .key(trust)
            .key(&set.ranked)
            .key(&set.staging)
            .key(&set.old)
            .arg(ATOMIC_MAX)
            .invoke_async(&mut conn)
            .await?;
        if copied >= 0 {
            stats.scanned += size;
            stats.copied += copied as usize;
            return Ok(());
        }
        // Grew past the limit since it was sized: stage it instead.
    }

    let _: () = redis::pipe()
        .unlink(&[&set.staging, &set.old])
        .ignore()
        .zadd(&set.staging, SENTINEL, 0)
        .ignore()
        .expire(&set.staging, STAGING_LEASE_SECS as i64)
        .ignore()
        .query_async(&mut conn)
        .await?;

    let mut cursor: u64 = 0;
    loop {
        let (next, entries): (u64, Vec<String>) = redis::cmd("ZSCAN")
            .arg(&set.source)
            .arg(cursor)
            .arg("COUNT")
            .arg(BATCH)
            .query_async(&mut conn)
            .await?;
        // ZSCAN replies member, score, member, score...
        let members: Vec<&String> = entries.iter().step_by(2).collect();
        if !members.is_empty() {
            let copied: i64 = COPY_BATCH
                .key(&set.source)
                .key(trust)
                .key(&set.staging)
                .arg(STAGING_LEASE_SECS)
                .arg(&members)
                .invoke_async(&mut conn)
                .await?;
            stats.scanned += members.len();
            stats.copied += copied as usize;
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }

    let _: i64 = SWAP
        .key(&set.staging)
        .key(&set.ranked)
        .key(&set.old)
        .arg(SENTINEL)
        .invoke_async(&mut conn)
        .await?;
    let _: () = conn.unlink(&set.old).await?;
    Ok(())
}

/// Drops every ranked set. The ready marker goes first, so readers fall back
/// to the unfiltered sets before any ranked set disappears.
async fn drop_all(layout: &RankedLayout) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: () = conn.del(&layout.built_at).await?;
    let global = &layout.global;
    let _: () = conn
        .unlink(&[&global.ranked, &global.staging, &global.old])
        .await?;

    let tags = scan_tag_keys(layout).await?;
    let ranked = tags
        .ranked
        .iter()
        .map(|label| format!("{}{label}", layout.tag_ranked_prefix));
    unlink_keys(ranked.chain(tags.leftovers)).await
}

/// The per-label keys of every family, from one pass over the keyspace: `SCAN`
/// costs the whole keyspace however few keys match, so the families share it.
#[derive(Debug, Default)]
struct TagKeys {
    /// Labels with a timeline.
    sources: BTreeSet<String>,
    /// Labels with a ranked timeline.
    ranked: BTreeSet<String>,
    /// Full keys of staging and old sets.
    leftovers: BTreeSet<String>,
}

/// Collected up front: a rebuild only writes keys of families it has already
/// listed, and a deduplicating set absorbs the repeats `SCAN` may return.
async fn scan_tag_keys(layout: &RankedLayout) -> RedisResult<TagKeys> {
    let families = [
        layout.tag_source_prefix.as_str(),
        layout.tag_ranked_prefix.as_str(),
        layout.tag_staging_prefix.as_str(),
        layout.tag_old_prefix.as_str(),
    ];
    let pattern = format!("{}*", common_prefix(&families));
    let mut conn = get_redis_conn().await?;
    let mut tags = TagKeys::default();
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async(&mut conn)
            .await?;
        for key in keys {
            if let Some(label) = key.strip_prefix(families[0]) {
                tags.sources.insert(label.to_string());
            } else if let Some(label) = key.strip_prefix(families[1]) {
                tags.ranked.insert(label.to_string());
            } else if key.starts_with(families[2]) || key.starts_with(families[3]) {
                tags.leftovers.insert(key);
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(tags)
}

/// The longest prefix all of `prefixes` share, kept on a character boundary.
fn common_prefix<'a>(prefixes: &[&'a str]) -> &'a str {
    let first = prefixes.first().copied().unwrap_or_default();
    let mut len = prefixes.iter().fold(first.len(), |len, prefix| {
        first
            .bytes()
            .zip(prefix.bytes())
            .take(len)
            .take_while(|(a, b)| a == b)
            .count()
    });
    while !first.is_char_boundary(len) {
        len -= 1;
    }
    &first[..len]
}

async fn unlink_keys(keys: impl IntoIterator<Item = String>) -> RedisResult<()> {
    let keys: Vec<String> = keys.into_iter().collect();
    let mut conn = get_redis_conn().await?;
    for chunk in keys.chunks(SCAN_COUNT) {
        let _: () = conn.unlink(chunk).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(trust_exists: bool, built: bool, viewer_ranked: Option<bool>) -> RankedState {
        RankedState {
            trust_exists,
            built,
            viewer_ranked,
        }
    }

    #[test]
    fn mode_is_off_without_a_ranking() {
        for built in [false, true] {
            for viewer in [None, Some(false), Some(true)] {
                assert_eq!(state(false, built, viewer).mode(), TrustMode::Off);
            }
        }
    }

    #[test]
    fn mode_is_off_for_a_viewer_outside_the_ranking() {
        assert_eq!(state(true, true, Some(false)).mode(), TrustMode::Off);
        assert_eq!(state(true, false, Some(false)).mode(), TrustMode::Off);
    }

    #[test]
    fn mode_filters_anonymous_and_ranked_viewers() {
        for viewer in [None, Some(true)] {
            assert_eq!(state(true, true, viewer).mode(), TrustMode::Ranked);
            assert_eq!(state(true, false, viewer).mode(), TrustMode::Unbuilt);
        }
    }

    #[test]
    fn common_prefix_spans_every_tag_family() {
        let layout = RankedLayout::production();
        let families = [
            layout.tag_source_prefix.as_str(),
            layout.tag_ranked_prefix.as_str(),
            layout.tag_staging_prefix.as_str(),
            layout.tag_old_prefix.as_str(),
        ];
        assert_eq!(common_prefix(&families), "Sorted:Tags:");
        assert_eq!(common_prefix(&["abc", "abd"]), "ab");
        assert_eq!(common_prefix(&["é1", "é2"]), "é");
        assert_eq!(common_prefix(&["x"]), "x");
    }

    #[test]
    fn production_layout_matches_the_key_parts_readers_use() {
        let layout = RankedLayout::production();
        assert_eq!(layout.trust, "Sorted:Users:SocialGraph");
        assert_eq!(layout.global.source, "Sorted:Posts:Global:Timeline");
        assert_eq!(layout.global.ranked, "Sorted:Posts:Global:Timeline:Ranked");
        let tag = layout.tag("bitcoin");
        assert_eq!(tag.source, "Sorted:Tags:Global:Post:Timeline:bitcoin");
        assert_eq!(tag.ranked, "Sorted:Tags:Ranked:Post:Timeline:bitcoin");
        assert_eq!(
            tag.staging,
            "Sorted:Tags:RankedStaging:Post:Timeline:bitcoin"
        );
        assert_eq!(tag.old, "Sorted:Tags:RankedOld:Post:Timeline:bitcoin");
        // Scanning one family of keys must never match another.
        let prefixes = [
            &layout.tag_source_prefix,
            &layout.tag_ranked_prefix,
            &layout.tag_staging_prefix,
            &layout.tag_old_prefix,
        ];
        for (i, a) in prefixes.iter().enumerate() {
            for (j, b) in prefixes.iter().enumerate() {
                if i != j {
                    assert!(!a.starts_with(b.as_str()), "{a} starts with {b}");
                }
            }
        }
    }

    /// Live tests against Redis, each in its own key namespace so they see
    /// neither the fixture sets nor each other.
    mod live {
        use super::super::*;
        use crate::types::DynError;
        use crate::{StackConfig, StackManager};

        type TestResult = Result<(), DynError>;

        async fn setup(ns: &str) -> Result<RankedLayout, DynError> {
            StackManager::setup(&StackConfig::default()).await?;
            let mut conn = get_redis_conn().await?;
            let keys: Vec<String> = conn.keys(format!("{ns}:*")).await?;
            unlink_keys(keys).await?;
            Ok(RankedLayout::namespaced(ns))
        }

        async fn zadd(key: &str, entries: &[(f64, &str)]) -> TestResult {
            let mut conn = get_redis_conn().await?;
            for chunk in entries.chunks(BATCH) {
                let _: () = conn.zadd_multiple(key, chunk).await?;
            }
            Ok(())
        }

        async fn members(key: &str) -> Result<Vec<(String, f64)>, DynError> {
            let mut conn = get_redis_conn().await?;
            Ok(conn.zrange_withscores(key, 0, -1).await?)
        }

        async fn exists(key: &str) -> Result<bool, DynError> {
            let mut conn = get_redis_conn().await?;
            Ok(conn.exists(key).await?)
        }

        fn owned(entries: &[(&str, f64)]) -> Vec<(String, f64)> {
            entries.iter().map(|(m, s)| (m.to_string(), *s)).collect()
        }

        #[tokio_shared_rt::test(shared)]
        async fn mirror_add_copies_ranked_authors_only() -> TestResult {
            let l = setup("Test:Ranked:MirrorAdd").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(10.0, "alice:p1"), (20.0, "bob:p2")]).await?;

            mirror_add(&l.trust, &l.global, "alice", "alice:p1").await?;
            mirror_add(&l.trust, &l.global, "bob", "bob:p2").await?;
            // Not in the source: nothing to mirror.
            mirror_add(&l.trust, &l.global, "alice", "alice:gone").await?;

            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 10.0)])
            );
            // No build is running, so nothing is written to a staging set.
            assert!(!exists(&l.global.staging).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn mirror_add_without_a_ranking_writes_nothing() -> TestResult {
            let l = setup("Test:Ranked:NoRanking").await?;
            zadd(&l.global.source, &[(10.0, "alice:p1")]).await?;

            mirror_add(&l.trust, &l.global, "alice", "alice:p1").await?;

            assert!(!exists(&l.global.ranked).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn mirrors_write_through_to_a_running_build() -> TestResult {
            let l = setup("Test:Ranked:WriteThrough").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;
            zadd(&l.global.source, &[(10.0, "alice:p1")]).await?;

            mirror_add(&l.trust, &l.global, "alice", "alice:p1").await?;
            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 10.0)])
            );
            assert_eq!(
                members(&l.global.staging).await?,
                owned(&[(SENTINEL, 0.0), ("alice:p1", 10.0)])
            );

            mirror_remove(&l.global, "alice:p1").await?;
            assert!(!exists(&l.global.ranked).await?);
            assert_eq!(members(&l.global.staging).await?, owned(&[(SENTINEL, 0.0)]));
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_keeps_ranked_authors_in_every_set() -> TestResult {
            let l = setup("Test:Ranked:Rebuild").await?;
            zadd(&l.trust, &[(1.0, "alice"), (2.0, "carol")]).await?;
            zadd(
                &l.global.source,
                &[(1.0, "alice:p1"), (2.0, "bob:p2"), (3.0, "carol:p3")],
            )
            .await?;
            let rust = l.tag("rust");
            zadd(&rust.source, &[(1.0, "alice:p1"), (2.0, "bob:p2")]).await?;
            let spam = l.tag("spam");
            zadd(&spam.source, &[(9.0, "bob:p9")]).await?;
            // Drift the rebuild must correct.
            zadd(&l.global.ranked, &[(2.0, "bob:p2")]).await?;
            zadd(&spam.ranked, &[(9.0, "bob:p9")]).await?;

            let stats = rebuild(&l).await?;

            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 1.0), ("carol:p3", 3.0)])
            );
            assert_eq!(members(&rust.ranked).await?, owned(&[("alice:p1", 1.0)]));
            assert!(!exists(&spam.ranked).await?);
            assert!(exists(&l.built_at).await?);
            assert_eq!(stats.sets, 3);
            assert_eq!(stats.copied, 3);
            assert!(!stats.dropped);
            Ok(())
        }

        /// Past the atomic limit, across `ZSCAN` pages and a run of equal
        /// scores longer than a page.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_stages_large_sets() -> TestResult {
            let l = setup("Test:Ranked:Staged").await?;
            zadd(&l.trust, &[(1.0, "ranked-a"), (2.0, "ranked-b")]).await?;
            let mut posts: Vec<(String, f64)> = Vec::new();
            posts.extend((0..1_500).map(|i| (format!("ranked-a:P{i:05}"), 1_000.0)));
            posts.extend((0..700).map(|i| (format!("ranked-b:P{i:05}"), 2_000.0 + i as f64)));
            posts.extend((0..900).map(|i| (format!("unranked-c:P{i:05}"), 500.0 + i as f64)));
            let entries: Vec<(f64, &str)> = posts.iter().map(|(m, s)| (*s, m.as_str())).collect();
            zadd(&l.global.source, &entries).await?;

            rebuild(&l).await?;

            let mut expected: Vec<(String, f64)> = posts
                .into_iter()
                .filter(|(m, _)| !m.starts_with("unranked-c:"))
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(members(&l.global.ranked).await?, expected);
            assert!(!exists(&l.global.staging).await?, "staging set left behind");
            assert!(!exists(&l.global.old).await?, "old ranked set left behind");
            Ok(())
        }

        /// Writes landing during a staged build: a member deleted after its
        /// batch was copied is not resurrected, and one created after its
        /// region was scanned is kept.
        #[tokio_shared_rt::test(shared)]
        async fn staged_build_keeps_concurrent_writes() -> TestResult {
            let l = setup("Test:Ranked:Concurrent").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(1.0, "alice:p1"), (2.0, "alice:p2")]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;

            let mut conn = get_redis_conn().await?;
            let copied: i64 = COPY_BATCH
                .key(&l.global.source)
                .key(&l.trust)
                .key(&l.global.staging)
                .arg(STAGING_LEASE_SECS)
                .arg(&["alice:p1", "alice:p2", "alice:p0"])
                .invoke_async(&mut conn)
                .await?;
            assert_eq!(copied, 2, "a member missing from the source is not copied");

            // Deleted, then created, while the build is still running.
            let _: () = conn.zrem(&l.global.source, "alice:p2").await?;
            mirror_remove(&l.global, "alice:p2").await?;
            zadd(&l.global.source, &[(3.0, "alice:p3")]).await?;
            mirror_add(&l.trust, &l.global, "alice", "alice:p3").await?;

            let _: i64 = SWAP
                .key(&l.global.staging)
                .key(&l.global.ranked)
                .key(&l.global.old)
                .arg(SENTINEL)
                .invoke_async(&mut conn)
                .await?;

            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 1.0), ("alice:p3", 3.0)])
            );
            assert!(!exists(&l.global.staging).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn small_rebuild_defers_to_staging_when_the_set_grew() -> TestResult {
            let l = setup("Test:Ranked:Grew").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(1.0, "alice:p1"), (2.0, "alice:p2")]).await?;

            let mut conn = get_redis_conn().await?;
            let copied: i64 = REBUILD_SMALL
                .key(&l.global.source)
                .key(&l.trust)
                .key(&l.global.ranked)
                .key(&l.global.staging)
                .key(&l.global.old)
                .arg(1)
                .invoke_async(&mut conn)
                .await?;

            assert_eq!(copied, -1);
            assert!(
                !exists(&l.global.ranked).await?,
                "nothing written on deferral"
            );
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_without_a_ranking_drops_every_ranked_set() -> TestResult {
            let l = setup("Test:Ranked:Drop").await?;
            zadd(&l.global.source, &[(1.0, "alice:p1")]).await?;
            zadd(&l.global.ranked, &[(1.0, "alice:p1")]).await?;
            zadd(&l.tag("x").ranked, &[(1.0, "alice:p1")]).await?;
            zadd(&l.tag("y").staging, &[(0.0, SENTINEL)]).await?;
            zadd(&l.tag("z").old, &[(1.0, "alice:p1")]).await?;
            let mut conn = get_redis_conn().await?;
            let _: () = conn.set(&l.built_at, 1).await?;

            let stats = rebuild(&l).await?;

            assert!(stats.dropped);
            for key in [
                &l.built_at,
                &l.global.ranked,
                &l.tag("x").ranked,
                &l.tag("y").staging,
                &l.tag("z").old,
            ] {
                assert!(!exists(key).await?, "{key} survived without a ranking");
            }
            // The source sets are not the rebuild's to touch.
            assert!(exists(&l.global.source).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_removes_orphans_and_leftovers() -> TestResult {
            let l = setup("Test:Ranked:Orphans").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.tag("live").source, &[(1.0, "alice:p1")]).await?;
            zadd(&l.global.source, &[(4.0, "alice:p4")]).await?;
            // Ranked set whose source vanished, and leftovers of a crashed build,
            // including the global set's (small, so rebuilt atomically).
            zadd(&l.tag("gone").ranked, &[(2.0, "alice:p2")]).await?;
            zadd(&l.tag("live").staging, &[(0.0, SENTINEL)]).await?;
            zadd(&l.tag("other").old, &[(3.0, "alice:p3")]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;
            zadd(&l.global.old, &[(3.0, "alice:p3")]).await?;

            let stats = rebuild(&l).await?;

            assert_eq!(stats.orphans, 1);
            assert!(!exists(&l.tag("gone").ranked).await?);
            assert!(!exists(&l.tag("live").staging).await?);
            assert!(!exists(&l.tag("other").old).await?);
            assert!(!exists(&l.global.staging).await?);
            assert!(!exists(&l.global.old).await?);
            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p4", 4.0)])
            );
            assert_eq!(
                members(&l.tag("live").ranked).await?,
                owned(&[("alice:p1", 1.0)])
            );
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_waits_for_a_running_rebuild() -> TestResult {
            let l = setup("Test:Ranked:Lock").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(1.0, "alice:p1")]).await?;
            assert!(try_acquire_lock(&l.lock, "other-rebuild", 60).await?);

            let waiting = l.clone();
            let handle = tokio::spawn(async move { rebuild(&waiting).await });
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            assert!(
                !handle.is_finished(),
                "rebuild must wait for the lock holder"
            );
            assert!(!exists(&l.global.ranked).await?);

            release_lock(&l.lock, "other-rebuild").await?;
            handle.await??;
            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 1.0)])
            );
            assert!(
                !exists(&l.lock).await?,
                "the lock is released after the rebuild"
            );
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn state_reflects_the_ranking_marker_and_viewer() -> TestResult {
            let l = setup("Test:Ranked:State").await?;

            let none = RankedState::load(&l, Some("alice")).await?;
            assert_eq!(none.mode(), TrustMode::Off);

            zadd(&l.trust, &[(1.0, "alice")]).await?;
            let unbuilt = RankedState::load(&l, None).await?;
            assert_eq!(
                unbuilt,
                RankedState {
                    trust_exists: true,
                    built: false,
                    viewer_ranked: None
                }
            );

            let mut conn = get_redis_conn().await?;
            let _: () = conn.set(&l.built_at, 1).await?;
            assert_eq!(
                RankedState::load(&l, Some("alice")).await?.mode(),
                TrustMode::Ranked
            );
            assert_eq!(
                RankedState::load(&l, Some("stranger")).await?.mode(),
                TrustMode::Off
            );
            Ok(())
        }
    }
}
