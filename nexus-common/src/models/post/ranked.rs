//! Ranked copies of the sorted sets behind `source=all` with `sorting=timeline`:
//! the same members at the same scores, keeping only posts whose author is in
//! the trust ranking (`Sorted:Users:SocialGraph`, built by the trust job).
//!
//! One ranked set mirrors the global timeline and one mirrors each per-label
//! timeline. Serving a filtered stream from a pre-filtered set keeps paging
//! exact: a page is only short at the real end of the stream, which matters
//! because pubky-app treats a short page as the end of the feed.
//!
//! Every write to a source set goes through [`add`] and [`remove`], which
//! update the ranked copy in the same atomic step. Every ranking publish
//! rebuilds the copies in full ([`RankedLayout::rebuild`]), so drift lasts at
//! most until the next recompute. Readers only trust the ranked sets once a
//! complete rebuild has run (`built_at`); before that they serve the unfiltered
//! sets.
//!
//! The scripts take several keys, so they assume a single Redis instance (not
//! Redis Cluster), as the rest of Nexus does.

use std::collections::BTreeSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use deadpool_redis::Connection;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use redis::{AsyncCommands, Script};

use super::search::TAG_GLOBAL_POST_TIMELINE;
use super::stream::POST_TIMELINE_KEY_PARTS;
use crate::db::get_redis_conn;
use crate::db::kv::{release_lock, try_acquire_lock, RedisError, RedisResult, SORTED_PREFIX};
use crate::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;

/// Ranked copy of the global timeline: `Sorted:Posts:Global:Timeline:Ranked`.
pub const POST_RANKED_TIMELINE_KEY_PARTS: [&str; 4] = ["Posts", "Global", "Timeline", "Ranked"];
/// Ranked copies of the per-label timelines: `Sorted:Tags:Ranked:Post:Timeline:<label>`.
/// A different third segment from the source sets, so a scan for one never matches the other.
pub const TAG_RANKED_POST_TIMELINE: [&str; 4] = ["Tags", "Ranked", "Post", "Timeline"];

/// Sets up to this size are rebuilt by one atomic script.
const ATOMIC_MAX: usize = 500;
/// Members per `ZSCAN` page in a staged build. Sized so a script stays well
/// under 10 ms of Redis time: at a million root posts a 1,000-member batch
/// occasionally took just over 10 ms.
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
    /// `SCAN` pattern covering the three per-label families.
    pub tag_scan_pattern: String,
    /// Set once a complete rebuild has run; readers trust the ranked sets only then.
    pub built_at: String,
    /// Serializes rebuilds.
    pub lock: String,
}

/// One source set and the keys of its ranked copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedSet {
    pub source: String,
    pub ranked: String,
    /// Where a staged build writes before the swap.
    pub staging: String,
}

static PRODUCTION: LazyLock<RankedLayout> = LazyLock::new(|| {
    let global_source = format!("{SORTED_PREFIX}:{}", POST_TIMELINE_KEY_PARTS.join(":"));
    let [tags, ranked, post, timeline] = TAG_RANKED_POST_TIMELINE;
    RankedLayout {
        trust: format!("{SORTED_PREFIX}:{}", USER_SOCIAL_GRAPH_KEY_PARTS.join(":")),
        global: RankedSet {
            ranked: format!(
                "{SORTED_PREFIX}:{}",
                POST_RANKED_TIMELINE_KEY_PARTS.join(":")
            ),
            staging: format!("{global_source}:RankedStaging"),
            source: global_source,
        },
        tag_source_prefix: format!("{SORTED_PREFIX}:{}:", TAG_GLOBAL_POST_TIMELINE.join(":")),
        tag_ranked_prefix: format!("{SORTED_PREFIX}:{tags}:{ranked}:{post}:{timeline}:"),
        tag_staging_prefix: format!("{SORTED_PREFIX}:{tags}:{ranked}Staging:{post}:{timeline}:"),
        tag_scan_pattern: format!("{SORTED_PREFIX}:{tags}:*:{post}:{timeline}:*"),
        built_at: "Ranked:Timeline:BuiltAt".to_string(),
        lock: "lock:ranked-timeline-rebuild".to_string(),
    }
});

impl RankedLayout {
    pub(crate) fn production() -> &'static RankedLayout {
        &PRODUCTION
    }

    /// The same key shapes as production under `ns`, so scans behave the same.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn namespaced(ns: &str) -> RankedLayout {
        RankedLayout {
            trust: format!("{ns}:Trust"),
            global: RankedSet {
                source: format!("{ns}:Timeline"),
                ranked: format!("{ns}:Timeline:Ranked"),
                staging: format!("{ns}:Timeline:RankedStaging"),
            },
            tag_source_prefix: format!("{ns}:Tags:Global:"),
            tag_ranked_prefix: format!("{ns}:Tags:Ranked:"),
            tag_staging_prefix: format!("{ns}:Tags:RankedStaging:"),
            tag_scan_pattern: format!("{ns}:Tags:*"),
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
        }
    }

    /// Rebuilds every ranked set from the current ranking, or drops them all
    /// when there is no ranking. Waits for a rebuild already running, so the
    /// last ranking publish is always followed by a complete rebuild.
    pub async fn rebuild(&self) -> RedisResult<RankedRebuildStats> {
        let token = lock_token();
        let deadline = Instant::now() + LOCK_WAIT;
        while !try_acquire_lock(&self.lock, &token, LOCK_TTL_SECS).await? {
            if Instant::now() >= deadline {
                return Err(RedisError::CommandFailed(
                    "timed out waiting for another ranked-set rebuild to finish".into(),
                ));
            }
            tokio::time::sleep(LOCK_POLL).await;
        }

        let result = async {
            let mut conn = get_redis_conn().await?;
            self.rebuild_locked(&mut conn).await
        }
        .await;
        let released = release_lock(&self.lock, &token).await;
        let stats = result?;
        released?;
        Ok(stats)
    }

    async fn rebuild_locked(&self, conn: &mut Connection) -> RedisResult<RankedRebuildStats> {
        let mut stats = RankedRebuildStats::default();
        let trust_exists: bool = conn.exists(&self.trust).await?;
        if !trust_exists {
            self.drop_all(conn).await?;
            stats.dropped = true;
            return Ok(stats);
        }

        let tags = self.scan_tag_keys(conn).await?;
        rebuild_set(conn, &self.trust, &self.global, &mut stats).await?;
        for label in &tags.sources {
            rebuild_set(conn, &self.trust, &self.tag(label), &mut stats).await?;
        }

        // A ranked set normally empties, and so vanishes, with its source; one that
        // outlived its source drifted. Re-checked here, as a label may have been
        // tagged again since the scan.
        for label in tags.ranked.difference(&tags.sources) {
            let set = self.tag(label);
            let source_exists: bool = conn.exists(&set.source).await?;
            if !source_exists {
                let _: () = conn.unlink(&set.ranked).await?;
                stats.orphans += 1;
            }
        }
        // Left by a build that crashed; the lock guarantees none is running now.
        let leftovers = tags
            .staging
            .iter()
            .map(|label| format!("{}{label}", self.tag_staging_prefix));
        unlink_keys(conn, leftovers).await?;

        let built_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default();
        let _: () = conn.set(&self.built_at, built_at).await?;
        Ok(stats)
    }

    /// Drops every ranked set. The ready marker goes first, so readers fall back
    /// to the unfiltered sets before any ranked set disappears.
    async fn drop_all(&self, conn: &mut Connection) -> RedisResult<()> {
        let _: () = conn.del(&self.built_at).await?;
        let _: () = conn
            .unlink(&[&self.global.ranked, &self.global.staging])
            .await?;
        let tags = self.scan_tag_keys(conn).await?;
        let ranked = tags
            .ranked
            .iter()
            .map(|label| format!("{}{label}", self.tag_ranked_prefix));
        let staging = tags
            .staging
            .iter()
            .map(|label| format!("{}{label}", self.tag_staging_prefix));
        unlink_keys(conn, ranked.chain(staging)).await
    }

    /// The labels of every per-label family, from one pass over the keyspace:
    /// `SCAN` costs the whole keyspace however few keys match, so the families
    /// share it. Collected up front: a rebuild only writes keys of families it
    /// has already listed, and the sets absorb the repeats `SCAN` may return.
    async fn scan_tag_keys(&self, conn: &mut Connection) -> RedisResult<TagKeys> {
        let mut tags = TagKeys::default();
        let mut cursor: u64 = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&self.tag_scan_pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(conn)
                .await?;
            for key in keys {
                if let Some(label) = key.strip_prefix(self.tag_source_prefix.as_str()) {
                    tags.sources.insert(label.to_string());
                } else if let Some(label) = key.strip_prefix(self.tag_ranked_prefix.as_str()) {
                    tags.ranked.insert(label.to_string());
                } else if let Some(label) = key.strip_prefix(self.tag_staging_prefix.as_str()) {
                    tags.staging.insert(label.to_string());
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(tags)
    }
}

/// The labels found for each per-label family.
#[derive(Debug, Default)]
struct TagKeys {
    sources: BTreeSet<String>,
    ranked: BTreeSet<String>,
    staging: BTreeSet<String>,
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

impl TrustMode {
    /// Reads the ranking state for one request in one round trip.
    pub async fn load(layout: &RankedLayout, viewer_id: Option<&str>) -> RedisResult<Self> {
        let mut pipe = redis::pipe();
        pipe.exists(&layout.trust).exists(&layout.built_at);
        let mut conn = get_redis_conn().await?;
        let (trust_exists, built, viewer_ranked) = match viewer_id {
            Some(viewer_id) => {
                pipe.zscore(&layout.trust, viewer_id);
                let (trust_exists, built, score): (bool, bool, Option<f64>) =
                    pipe.query_async(&mut conn).await?;
                (trust_exists, built, Some(score.is_some()))
            }
            None => {
                let (trust_exists, built): (bool, bool) = pipe.query_async(&mut conn).await?;
                (trust_exists, built, None)
            }
        };
        Ok(Self::decide(trust_exists, built, viewer_ranked))
    }

    /// No ranking means no filtering at all. A viewer outside the ranking,
    /// including one Nexus does not know, gets the unfiltered stream;
    /// `viewer_ranked` is `None` when the request has no viewer.
    pub fn decide(trust_exists: bool, built: bool, viewer_ranked: Option<bool>) -> Self {
        if !trust_exists || viewer_ranked == Some(false) {
            TrustMode::Off
        } else if built {
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

/// Writes a member to a source set (when a score is given) and, when its author
/// is in the ranking, to the ranked copy and a running build's staging set, at
/// the source's score. One atomic step, so the copy never disagrees with the
/// source.
static ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"if ARGV[2] ~= '' then redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]) end
          local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
          if not score then return 0 end
          local sep = string.find(ARGV[1], ':', 1, true)
          if not sep or not redis.call('ZSCORE', KEYS[2], string.sub(ARGV[1], 1, sep - 1)) then
              return 0
          end
          redis.call('ZADD', KEYS[3], score, ARGV[1])
          if redis.call('EXISTS', KEYS[4]) == 1 then
              redis.call('ZADD', KEYS[4], score, ARGV[1])
          end
          return 1",
    )
});

/// Rebuilds a small set in one atomic step, dropping any staging set a crashed
/// build left behind. Returns `{size, copied}`, with `copied = -1` when the
/// source is larger than `ARGV[1]` and must be staged instead.
static REBUILD_SMALL: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"local size = redis.call('ZCARD', KEYS[1])
          if size > tonumber(ARGV[1]) then return {size, -1} end
          redis.call('UNLINK', KEYS[3], KEYS[4])
          local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
          local copied = 0
          for i = 1, #entries, 2 do
              local member = entries[i]
              local sep = string.find(member, ':', 1, true)
              if sep and redis.call('ZSCORE', KEYS[2], string.sub(member, 1, sep - 1)) then
                  redis.call('ZADD', KEYS[3], entries[i + 1], member)
                  copied = copied + 1
              end
          end
          return {size, copied}",
    )
});

/// Copies one `ZSCAN` page of a staged build, at the scores the scan read in the
/// same atomic step. Returns `{next cursor, scanned, copied}`.
static COPY_BATCH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"local page = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
          local entries = page[2]
          local copied = 0
          for i = 1, #entries, 2 do
              local member = entries[i]
              local sep = string.find(member, ':', 1, true)
              if sep and redis.call('ZSCORE', KEYS[2], string.sub(member, 1, sep - 1)) then
                  redis.call('ZADD', KEYS[3], entries[i + 1], member)
                  copied = copied + 1
              end
          end
          redis.call('EXPIRE', KEYS[3], ARGV[3])
          return {page[1], #entries / 2, copied}",
    )
});

/// Installs a finished staging set: drops the sentinel and the current ranked
/// set (`UNLINK` frees it off the main thread), then renames the staging set
/// into place. A staging set left empty means no ranked member, so the ranked
/// set ends up absent.
static SWAP: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"redis.call('ZREM', KEYS[1], ARGV[1])
          redis.call('UNLINK', KEYS[2])
          if redis.call('EXISTS', KEYS[1]) == 1 then
              redis.call('RENAME', KEYS[1], KEYS[2])
              redis.call('PERSIST', KEYS[2])
          end
          return 1",
    )
});

/// Adds `member` (`author:post`) to `set.source` at `score`, mirroring it into
/// the ranked copy. With `score` `None` only the mirror runs, from the score
/// already in the source, so a retry repairs a copy a crash left behind.
pub(crate) async fn add(
    trust: &str,
    set: &RankedSet,
    member: &str,
    score: Option<f64>,
) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: i64 = ADD
        .key(&set.source)
        .key(trust)
        .key(&set.ranked)
        .key(&set.staging)
        .arg(member)
        .arg(score.map(|score| score.to_string()).unwrap_or_default())
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

/// Removes `member` from `set.source`, its ranked copy and a running build's
/// staging set in one transaction, so a swap cannot land between the removals.
pub(crate) async fn remove(set: &RankedSet, member: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: () = redis::pipe()
        .atomic()
        .zrem(&set.source, member)
        .ignore()
        .zrem(&set.ranked, member)
        .ignore()
        .zrem(&set.staging, member)
        .ignore()
        .query_async(&mut conn)
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

/// Unique per call: the lock only lets the holder of its token release it.
fn lock_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{}:{nanos}", std::process::id())
}

async fn rebuild_set(
    conn: &mut Connection,
    trust: &str,
    set: &RankedSet,
    stats: &mut RankedRebuildStats,
) -> RedisResult<()> {
    stats.sets += 1;
    if let Some((scanned, copied)) = rebuild_small(conn, trust, set).await? {
        stats.scanned += scanned;
        stats.copied += copied;
        return Ok(());
    }

    // Too big for one atomic step: build into the staging set, then swap.
    let _: () = redis::pipe()
        .unlink(&set.staging)
        .ignore()
        .zadd(&set.staging, SENTINEL, 0)
        .ignore()
        .expire(&set.staging, STAGING_LEASE_SECS as i64)
        .ignore()
        .query_async(conn)
        .await?;
    let mut cursor: u64 = 0;
    loop {
        let (next, scanned, copied) = copy_batch(conn, trust, set, cursor).await?;
        stats.scanned += scanned;
        stats.copied += copied;
        if next == 0 {
            break;
        }
        cursor = next;
    }
    swap(conn, set).await
}

/// `Some((scanned, copied))` once rebuilt, `None` when the set is too big.
async fn rebuild_small(
    conn: &mut Connection,
    trust: &str,
    set: &RankedSet,
) -> RedisResult<Option<(usize, usize)>> {
    let (size, copied): (i64, i64) = REBUILD_SMALL
        .key(&set.source)
        .key(trust)
        .key(&set.ranked)
        .key(&set.staging)
        .arg(ATOMIC_MAX)
        .invoke_async(conn)
        .await?;
    Ok((copied >= 0).then_some((size as usize, copied as usize)))
}

/// `(next cursor, scanned, copied)` for the page at `cursor`.
async fn copy_batch(
    conn: &mut Connection,
    trust: &str,
    set: &RankedSet,
    cursor: u64,
) -> RedisResult<(u64, usize, usize)> {
    let (next, scanned, copied): (u64, i64, i64) = COPY_BATCH
        .key(&set.source)
        .key(trust)
        .key(&set.staging)
        .arg(cursor)
        .arg(BATCH)
        .arg(STAGING_LEASE_SECS)
        .invoke_async(conn)
        .await?;
    Ok((next, scanned as usize, copied as usize))
}

async fn swap(conn: &mut Connection, set: &RankedSet) -> RedisResult<()> {
    let _: i64 = SWAP
        .key(&set.staging)
        .key(&set.ranked)
        .arg(SENTINEL)
        .invoke_async(conn)
        .await?;
    Ok(())
}

async fn unlink_keys(
    conn: &mut Connection,
    keys: impl IntoIterator<Item = String>,
) -> RedisResult<()> {
    let keys: Vec<String> = keys.into_iter().collect();
    for chunk in keys.chunks(SCAN_COUNT) {
        let _: () = conn.unlink(chunk).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_is_off_without_a_ranking() {
        for built in [false, true] {
            for viewer in [None, Some(false), Some(true)] {
                assert_eq!(TrustMode::decide(false, built, viewer), TrustMode::Off);
            }
        }
    }

    #[test]
    fn decide_is_off_for_a_viewer_outside_the_ranking() {
        assert_eq!(TrustMode::decide(true, true, Some(false)), TrustMode::Off);
        assert_eq!(TrustMode::decide(true, false, Some(false)), TrustMode::Off);
    }

    #[test]
    fn decide_filters_anonymous_and_ranked_viewers() {
        for viewer in [None, Some(true)] {
            assert_eq!(TrustMode::decide(true, true, viewer), TrustMode::Ranked);
            assert_eq!(TrustMode::decide(true, false, viewer), TrustMode::Unbuilt);
        }
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
        assert_eq!(layout.tag_scan_pattern, "Sorted:Tags:*:Post:Timeline:*");
        // Scanning one family of keys must never match another.
        let prefixes = [
            &layout.tag_source_prefix,
            &layout.tag_ranked_prefix,
            &layout.tag_staging_prefix,
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
            unlink_keys(&mut conn, keys).await?;
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
        async fn add_mirrors_ranked_authors_only() -> TestResult {
            let l = setup("Test:Ranked:Add").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;

            add(&l.trust, &l.global, "alice:p1", Some(10.0)).await?;
            add(&l.trust, &l.global, "bob:p2", Some(20.0)).await?;
            // Neither in the source nor written: nothing to mirror.
            add(&l.trust, &l.global, "alice:gone", None).await?;

            assert_eq!(
                members(&l.global.source).await?,
                owned(&[("alice:p1", 10.0), ("bob:p2", 20.0)])
            );
            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 10.0)])
            );
            // No build is running, so nothing is written to a staging set.
            assert!(!exists(&l.global.staging).await?);
            Ok(())
        }

        /// Without a score only the mirror runs, from the source's own score.
        #[tokio_shared_rt::test(shared)]
        async fn add_without_a_score_repairs_the_ranked_copy() -> TestResult {
            let l = setup("Test:Ranked:Repair").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(10.0, "alice:p1")]).await?;

            add(&l.trust, &l.global, "alice:p1", None).await?;

            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 10.0)])
            );
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn add_without_a_ranking_writes_only_the_source() -> TestResult {
            let l = setup("Test:Ranked:NoRanking").await?;

            add(&l.trust, &l.global, "alice:p1", Some(10.0)).await?;

            assert_eq!(
                members(&l.global.source).await?,
                owned(&[("alice:p1", 10.0)])
            );
            assert!(!exists(&l.global.ranked).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn writes_go_through_to_a_running_build() -> TestResult {
            let l = setup("Test:Ranked:WriteThrough").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;

            add(&l.trust, &l.global, "alice:p1", Some(10.0)).await?;
            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 10.0)])
            );
            assert_eq!(
                members(&l.global.staging).await?,
                owned(&[(SENTINEL, 0.0), ("alice:p1", 10.0)])
            );

            remove(&l.global, "alice:p1").await?;
            assert!(!exists(&l.global.source).await?);
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

            let stats = l.rebuild().await?;

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

            l.rebuild().await?;

            let mut expected: Vec<(String, f64)> = posts
                .into_iter()
                .filter(|(m, _)| !m.starts_with("unranked-c:"))
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(members(&l.global.ranked).await?, expected);
            assert!(!exists(&l.global.staging).await?, "staging set left behind");
            Ok(())
        }

        /// Writes landing during a staged build: a member deleted after its
        /// page was copied is not resurrected, and one created after its region
        /// was scanned is kept.
        #[tokio_shared_rt::test(shared)]
        async fn staged_build_keeps_concurrent_writes() -> TestResult {
            let l = setup("Test:Ranked:Concurrent").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            zadd(&l.global.source, &[(1.0, "alice:p1"), (2.0, "alice:p2")]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;

            let mut conn = get_redis_conn().await?;
            let (next, scanned, copied) = copy_batch(&mut conn, &l.trust, &l.global, 0).await?;
            assert_eq!((next, scanned, copied), (0, 2, 2));

            // Deleted, then created, while the build is still running.
            remove(&l.global, "alice:p2").await?;
            add(&l.trust, &l.global, "alice:p3", Some(3.0)).await?;

            swap(&mut conn, &l.global).await?;

            assert_eq!(
                members(&l.global.ranked).await?,
                owned(&[("alice:p1", 1.0), ("alice:p3", 3.0)])
            );
            assert!(!exists(&l.global.staging).await?);
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn small_rebuild_defers_a_set_past_the_atomic_limit() -> TestResult {
            let l = setup("Test:Ranked:Defer").await?;
            zadd(&l.trust, &[(1.0, "alice")]).await?;
            let posts: Vec<String> = (0..=ATOMIC_MAX).map(|i| format!("alice:P{i:05}")).collect();
            let entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();
            zadd(&l.global.source, &entries).await?;

            let mut conn = get_redis_conn().await?;
            let rebuilt = rebuild_small(&mut conn, &l.trust, &l.global).await?;

            assert_eq!(rebuilt, None);
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
            let mut conn = get_redis_conn().await?;
            let _: () = conn.set(&l.built_at, 1).await?;

            let stats = l.rebuild().await?;

            assert!(stats.dropped);
            for key in [
                &l.built_at,
                &l.global.ranked,
                &l.tag("x").ranked,
                &l.tag("y").staging,
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
            zadd(&l.tag("dead").staging, &[(0.0, SENTINEL)]).await?;
            zadd(&l.global.staging, &[(0.0, SENTINEL)]).await?;

            let stats = l.rebuild().await?;

            assert_eq!(stats.orphans, 1);
            assert!(!exists(&l.tag("gone").ranked).await?);
            assert!(!exists(&l.tag("live").staging).await?);
            assert!(!exists(&l.tag("dead").staging).await?);
            assert!(!exists(&l.global.staging).await?);
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
            let handle = tokio::spawn(async move { waiting.rebuild().await });
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
        async fn load_reflects_the_ranking_marker_and_viewer() -> TestResult {
            let l = setup("Test:Ranked:State").await?;
            assert_eq!(TrustMode::load(&l, Some("alice")).await?, TrustMode::Off);

            zadd(&l.trust, &[(1.0, "alice")]).await?;
            assert_eq!(TrustMode::load(&l, None).await?, TrustMode::Unbuilt);

            let mut conn = get_redis_conn().await?;
            let _: () = conn.set(&l.built_at, 1).await?;
            assert_eq!(TrustMode::load(&l, Some("alice")).await?, TrustMode::Ranked);
            assert_eq!(TrustMode::load(&l, Some("stranger")).await?, TrustMode::Off);
            Ok(())
        }
    }
}
