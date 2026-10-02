//! Ranked copies of the sorted sets behind `source=all` with `sorting=timeline`:
//! the same members at the same scores, keeping only posts whose author is in
//! the trust ranking (`Sorted:Users:SocialGraph`, built by the trust job).
//!
//! One ranked set mirrors the global timeline and one mirrors each per-label
//! timeline. Serving a filtered stream from a pre-filtered set keeps paging
//! exact: a page is only short at the real end of the stream, which matters
//! because pubky-app treats a short page as the end of the feed.
//!
//! Every incremental write to a source set goes through [`add`] and [`remove`],
//! which update the ranked copy in the same atomic step; the bulk reindex
//! writes the sources directly and is followed by a rebuild. Every ranking publish
//! rebuilds the copies in full ([`rebuild`]), so drift lasts at most until the
//! next recompute. Readers only trust the ranked sets once a complete rebuild
//! has run ([`BUILT_AT_KEY`]); before that they serve the unfiltered sets.
//!
//! The scripts take several keys, so they assume a single Redis instance (not
//! Redis Cluster), as the rest of Nexus does.

use std::collections::BTreeSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use deadpool_redis::Connection;
use redis::{AsyncCommands, Script};

use super::search::TAG_GLOBAL_POST_TIMELINE;
use super::stream::POST_TIMELINE_KEY_PARTS;
use crate::db::get_redis_conn;
use crate::db::kv::{LockLease, RedisError, RedisResult, SORTED_PREFIX};
use crate::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;

/// Ranked copy of the global timeline: `Sorted:Posts:Ranked:Timeline`.
/// A different second segment from the source set, so a glob for one never matches the other.
pub const POST_RANKED_TIMELINE_KEY_PARTS: [&str; 3] = ["Posts", "Ranked", "Timeline"];
/// Where a staged rebuild of the global ranked copy writes before the swap.
const POST_RANKED_STAGING_TIMELINE_KEY_PARTS: [&str; 3] = ["Posts", "RankedStaging", "Timeline"];
/// Ranked copies of the per-label timelines: `Sorted:Tags:Ranked:Post:Timeline:<label>`.
/// A different second segment from the source sets, so a scan for one never matches the other.
pub const TAG_RANKED_POST_TIMELINE: [&str; 4] = ["Tags", "Ranked", "Post", "Timeline"];
/// Where a staged rebuild of a per-label ranked copy writes before the swap.
const TAG_RANKED_STAGING_POST_TIMELINE: [&str; 4] = ["Tags", "RankedStaging", "Post", "Timeline"];
/// Set once a complete rebuild has run; readers trust the ranked sets only then.
const BUILT_AT_KEY: &str = "Ranked:Timeline:BuiltAt";
/// Serializes rebuilds.
const REBUILD_LOCK_KEY: &str = "lock:ranked-timeline-rebuild";

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
/// How long the lock outlives a rebuild that stops renewing it, such as one
/// whose process was killed. Well under [`LOCK_WAIT`], so a waiting rebuild
/// outlasts a lock left behind.
const LOCK_TTL_SECS: u64 = 300;
/// How often a running rebuild renews the lock, between sets. A single set has
/// at least the TTL minus this to finish; the global one takes about 5 s at a
/// million posts.
const LOCK_RENEW: Duration = Duration::from_secs(60);
/// How long a rebuild waits for a running one before giving up.
const LOCK_WAIT: Duration = Duration::from_secs(15 * 60);
const LOCK_POLL: Duration = Duration::from_secs(1);

/// The key of the sorted set at `parts`.
fn sorted_key(parts: &[&str]) -> String {
    format!("{SORTED_PREFIX}:{}", parts.join(":"))
}

/// The trust ranking: users with a positive trust score.
fn ranking_key() -> String {
    sorted_key(&USER_SOCIAL_GRAPH_KEY_PARTS)
}

/// One source set and the keys of its ranked copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RankedSet {
    pub source: String,
    pub ranked: String,
    /// Where a staged build writes before the swap.
    pub staging: String,
}

impl RankedSet {
    /// The global timeline.
    pub fn global() -> Self {
        RankedSet {
            source: sorted_key(&POST_TIMELINE_KEY_PARTS),
            ranked: sorted_key(&POST_RANKED_TIMELINE_KEY_PARTS),
            staging: sorted_key(&POST_RANKED_STAGING_TIMELINE_KEY_PARTS),
        }
    }

    /// The per-label timeline for `label`.
    pub fn tag(label: &str) -> Self {
        let key = |parts: &[&str]| sorted_key(&[parts, &[label]].concat());
        RankedSet {
            source: key(&TAG_GLOBAL_POST_TIMELINE),
            ranked: key(&TAG_RANKED_POST_TIMELINE),
            staging: key(&TAG_RANKED_STAGING_POST_TIMELINE),
        }
    }
}

/// Rebuilds every ranked set from the current ranking, or drops them all when
/// there is no ranking. Waits for a rebuild already running, so the last
/// ranking publish is always followed by a complete rebuild.
pub(crate) async fn rebuild() -> RedisResult<RankedRebuildStats> {
    // Armed before the first attempt, so a rebuild cancelled or failing at any
    // point, the acquire included, still releases the lock.
    let lock = LockLease::new(REBUILD_LOCK_KEY);
    let deadline = Instant::now() + LOCK_WAIT;
    let mut waiting = false;
    loop {
        match lock.try_acquire(LOCK_TTL_SECS).await {
            Ok(true) => break,
            Ok(false) if Instant::now() < deadline => {
                if !waiting {
                    waiting = true;
                    tracing::warn!(
                        "Another ranked-set rebuild is running, waiting up to {LOCK_WAIT:?} for it"
                    );
                }
                tokio::time::sleep(LOCK_POLL).await
            }
            Ok(false) => {
                lock.disarm();
                return Err(RedisError::CommandFailed(
                    "timed out waiting for another ranked-set rebuild to finish".into(),
                ));
            }
            // The `SET` may have landed with its reply lost.
            Err(e) => {
                release(lock).await;
                return Err(e);
            }
        }
    }

    let result = async {
        let mut conn = get_redis_conn().await?;
        rebuild_locked(&mut conn, &lock).await
    }
    .await;
    release(lock).await;
    result
}

/// Releases the rebuild lock. A failure only delays the next rebuild until the
/// TTL frees the lock, so it is logged rather than returned.
async fn release(lock: LockLease) {
    if let Err(e) = lock.release().await {
        tracing::warn!("Could not release the ranked-set rebuild lock, its TTL frees it: {e}");
    }
}

async fn rebuild_locked(
    conn: &mut Connection,
    lock: &LockLease,
) -> RedisResult<RankedRebuildStats> {
    let mut renewed = Instant::now();
    let mut stats = RankedRebuildStats::default();
    let trust = ranking_key();
    let trust_exists: bool = conn.exists(&trust).await?;
    if !trust_exists {
        drop_all(conn).await?;
        stats.dropped = true;
        return Ok(stats);
    }

    let tags = scan_tag_keys(conn).await?;
    let labels = tags.sources.iter().map(|label| RankedSet::tag(label));
    for set in std::iter::once(RankedSet::global()).chain(labels) {
        keep_lock(lock, &mut renewed).await?;
        rebuild_set(conn, &trust, &set, &mut stats).await?;
    }
    // The sweeps below rely on no other rebuild running.
    keep_lock(lock, &mut renewed).await?;

    // A ranked set normally empties, and so vanishes, with its source; one that
    // outlived its source drifted. Re-checked here, as a label may have been
    // tagged again since the scan.
    for label in tags.ranked.difference(&tags.sources) {
        let set = RankedSet::tag(label);
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
        .map(|label| RankedSet::tag(label).staging);
    unlink_keys(conn, leftovers).await?;

    let built_at = chrono::Utc::now().timestamp_millis();
    let _: () = conn.set(BUILT_AT_KEY, built_at).await?;
    Ok(stats)
}

/// Renews the rebuild lock once [`LOCK_RENEW`] has passed since `renewed`, and
/// stops the rebuild if the lock was lost: another rebuild may hold it by then,
/// and two builds would overwrite each other's staging sets.
async fn keep_lock(lock: &LockLease, renewed: &mut Instant) -> RedisResult<()> {
    if renewed.elapsed() < LOCK_RENEW {
        return Ok(());
    }
    if !lock.extend(LOCK_TTL_SECS).await? {
        return Err(RedisError::CommandFailed(
            "lost the ranked-set rebuild lock".into(),
        ));
    }
    *renewed = Instant::now();
    Ok(())
}

/// Drops every ranked set. The ready marker goes first, so readers fall back
/// to the unfiltered sets before any ranked set disappears.
async fn drop_all(conn: &mut Connection) -> RedisResult<()> {
    let _: () = conn.del(BUILT_AT_KEY).await?;
    let global = RankedSet::global();
    let _: () = conn.unlink(&[&global.ranked, &global.staging]).await?;
    let tags = scan_tag_keys(conn).await?;
    let ranked = tags.ranked.iter().map(|label| RankedSet::tag(label).ranked);
    let staging = tags
        .staging
        .iter()
        .map(|label| RankedSet::tag(label).staging);
    unlink_keys(conn, ranked.chain(staging)).await
}

/// The labels of every per-label family, from one pass over the keyspace:
/// `SCAN` costs the whole keyspace however few keys match, so the families
/// share it. Collected up front: a rebuild only writes keys of families it
/// has already listed, and the sets absorb the repeats `SCAN` may return.
async fn scan_tag_keys(conn: &mut Connection) -> RedisResult<TagKeys> {
    let prefix = |parts: &[&str]| format!("{}:", sorted_key(parts));
    let source_prefix = prefix(&TAG_GLOBAL_POST_TIMELINE);
    let ranked_prefix = prefix(&TAG_RANKED_POST_TIMELINE);
    let staging_prefix = prefix(&TAG_RANKED_STAGING_POST_TIMELINE);
    let [tags_part, _, post_part, timeline_part] = TAG_RANKED_POST_TIMELINE;
    let pattern = format!("{SORTED_PREFIX}:{tags_part}:*:{post_part}:{timeline_part}:*");

    let mut tags = TagKeys::default();
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async(conn)
            .await?;
        for key in keys {
            if let Some(label) = key.strip_prefix(source_prefix.as_str()) {
                tags.sources.insert(label.to_string());
            } else if let Some(label) = key.strip_prefix(ranked_prefix.as_str()) {
                tags.ranked.insert(label.to_string());
            } else if let Some(label) = key.strip_prefix(staging_prefix.as_str()) {
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
    pub async fn load(viewer_id: Option<&str>) -> RedisResult<Self> {
        let trust = ranking_key();
        let mut pipe = redis::pipe();
        pipe.exists(&trust).exists(BUILT_AT_KEY);
        let mut conn = get_redis_conn().await?;
        let (trust_exists, built, viewer_ranked) = match viewer_id {
            Some(viewer_id) => {
                pipe.zscore(&trust, viewer_id);
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

/// Lua shared by the scripts below: whether `member` (`author:post`) has its
/// author in the ranking at `KEYS[2]`. One definition, so writes and rebuilds
/// always agree on who counts as ranked.
const IS_RANKED: &str = r"
local function is_ranked(member)
    local sep = string.find(member, ':', 1, true)
    return sep and redis.call('ZSCORE', KEYS[2], string.sub(member, 1, sep - 1))
end
";

/// Writes a member to a source set and, when its author is in the ranking, to
/// the ranked copy and a running build's staging set at the same score. One
/// atomic step, so the copy never disagrees with the source.
static ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            IS_RANKED,
            r"redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
              if not is_ranked(ARGV[1]) then return 0 end
              redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
              if redis.call('EXISTS', KEYS[4]) == 1 then
                  redis.call('ZADD', KEYS[4], ARGV[2], ARGV[1])
              end
              return 1",
        ]
        .concat(),
    )
});

/// Rebuilds a small set in one atomic step, dropping any staging set a crashed
/// build left behind. Returns `{size, copied}`, with `copied = -1` when the
/// source is larger than `ARGV[1]` and must be staged instead.
static REBUILD_SMALL: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            IS_RANKED,
            r"local size = redis.call('ZCARD', KEYS[1])
              if size > tonumber(ARGV[1]) then return {size, -1} end
              redis.call('UNLINK', KEYS[3], KEYS[4])
              local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
              local copied = 0
              for i = 1, #entries, 2 do
                  if is_ranked(entries[i]) then
                      redis.call('ZADD', KEYS[3], entries[i + 1], entries[i])
                      copied = copied + 1
                  end
              end
              return {size, copied}",
        ]
        .concat(),
    )
});

/// Copies one `ZSCAN` page of a staged build, at the scores the scan read in the
/// same atomic step. Returns `{next cursor, scanned, copied}`.
static COPY_BATCH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            IS_RANKED,
            r"local page = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
              local entries = page[2]
              local copied = 0
              for i = 1, #entries, 2 do
                  if is_ranked(entries[i]) then
                      redis.call('ZADD', KEYS[3], entries[i + 1], entries[i])
                      copied = copied + 1
                  end
              end
              redis.call('EXPIRE', KEYS[3], ARGV[3])
              return {page[1], #entries / 2, copied}",
        ]
        .concat(),
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

/// Adds `member` (`author:post`) to `set.source` at `score`, and to the ranked
/// copy when its author is in the ranking.
pub(crate) async fn add(set: &RankedSet, member: &str, score: f64) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: i64 = ADD
        .key(&set.source)
        .key(ranking_key())
        .key(&set.ranked)
        .key(&set.staging)
        .arg(member)
        .arg(score)
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
pub(crate) struct RankedRebuildStats {
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
    fn keys_match_the_key_parts_readers_use() {
        assert_eq!(ranking_key(), "Sorted:Users:SocialGraph");
        let global = RankedSet::global();
        assert_eq!(global.source, "Sorted:Posts:Global:Timeline");
        assert_eq!(global.ranked, "Sorted:Posts:Ranked:Timeline");
        assert_eq!(global.staging, "Sorted:Posts:RankedStaging:Timeline");
        let tag = RankedSet::tag("bitcoin");
        assert_eq!(tag.source, "Sorted:Tags:Global:Post:Timeline:bitcoin");
        assert_eq!(tag.ranked, "Sorted:Tags:Ranked:Post:Timeline:bitcoin");
        assert_eq!(
            tag.staging,
            "Sorted:Tags:RankedStaging:Post:Timeline:bitcoin"
        );
        // A scan or glob for one family of keys must never match another.
        let families = [
            global.source,
            global.ranked,
            global.staging,
            tag.source,
            tag.ranked,
            tag.staging,
        ];
        for (i, a) in families.iter().enumerate() {
            for (j, b) in families.iter().enumerate() {
                let prefix = b.trim_end_matches("bitcoin");
                assert!(i == j || !a.starts_with(prefix), "{a} starts with {prefix}");
            }
        }
    }

    /// Live tests against the shared Redis and its fixture ranking. Each test
    /// writes only its own labels and authors, and clears them before and
    /// after. A full rebuild, the lock and the ranking key are shared, so
    /// `.config/nextest.toml` runs these in the `ranked-sets` serial group.
    mod live {
        use super::super::*;
        use crate::db::kv::{release_lock, try_acquire_lock, RedisOps};
        use crate::models::post::PostStream;
        use crate::types::DynError;
        use crate::{StackConfig, StackManager};

        type TestResult = Result<(), DynError>;

        const ALICE: &str = "test-ranked-alice";
        const BOB: &str = "test-ranked-bob";
        const CAROL: &str = "test-ranked-carol";
        /// Where a test parks the ranking while it checks behaviour without one.
        const RANKING_ASIDE: &str = "Test:Ranked:RankingAside";

        /// Connects and clears what an earlier run may have left behind,
        /// including a ranking it parked and never restored.
        async fn setup(labels: &[&str]) -> TestResult {
            StackManager::setup(&StackConfig::default()).await?;
            let mut conn = get_redis_conn().await?;
            let parked: bool = conn.exists(RANKING_ASIDE).await?;
            if parked {
                restore_ranking().await?;
            }
            cleanup(labels).await
        }

        /// Clears the sets of `labels` and the test authors' ranks.
        async fn cleanup(labels: &[&str]) -> TestResult {
            let mut conn = get_redis_conn().await?;
            let keys = labels.iter().flat_map(|label| {
                let set = RankedSet::tag(label);
                [set.source, set.ranked, set.staging]
            });
            unlink_keys(&mut conn, keys).await?;
            let authors = [ALICE, BOB, CAROL];
            PostStream::remove_from_index_sorted_set(None, &USER_SOCIAL_GRAPH_KEY_PARTS, &authors)
                .await?;
            Ok(())
        }

        /// Ranks `authors` below every fixture user, so fixture ranks are untouched.
        async fn rank(authors: &[&str]) -> TestResult {
            let entries: Vec<(f64, &str)> = authors.iter().map(|author| (1e9, *author)).collect();
            PostStream::put_index_sorted_set(&USER_SOCIAL_GRAPH_KEY_PARTS, &entries, None, None)
                .await?;
            Ok(())
        }

        async fn zadd(key: &str, entries: &[(f64, &str)]) -> TestResult {
            let mut conn = get_redis_conn().await?;
            let _: () = conn.zadd_multiple(key, entries).await?;
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

        /// Moves the ranking aside, so the code under test sees none.
        async fn park_ranking() -> TestResult {
            let mut conn = get_redis_conn().await?;
            let _: () = conn.rename(ranking_key(), RANKING_ASIDE).await?;
            Ok(())
        }

        async fn restore_ranking() -> TestResult {
            let mut conn = get_redis_conn().await?;
            let _: () = conn.rename(RANKING_ASIDE, ranking_key()).await?;
            Ok(())
        }

        #[tokio_shared_rt::test(shared)]
        async fn add_mirrors_ranked_authors_only() -> TestResult {
            let label = "test-ranked-add";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);

            add(&set, "test-ranked-alice:p1", 10.0).await?;
            add(&set, "test-ranked-bob:p2", 20.0).await?;

            assert_eq!(
                members(&set.source).await?,
                owned(&[("test-ranked-alice:p1", 10.0), ("test-ranked-bob:p2", 20.0)])
            );
            assert_eq!(
                members(&set.ranked).await?,
                owned(&[("test-ranked-alice:p1", 10.0)])
            );
            // No build is running, so nothing is written to a staging set.
            assert!(!exists(&set.staging).await?);
            cleanup(&[label]).await
        }

        #[tokio_shared_rt::test(shared)]
        async fn writes_go_through_to_a_running_build() -> TestResult {
            let label = "test-ranked-write-through";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            zadd(&set.staging, &[(0.0, SENTINEL)]).await?;

            add(&set, "test-ranked-alice:p1", 10.0).await?;
            assert_eq!(
                members(&set.ranked).await?,
                owned(&[("test-ranked-alice:p1", 10.0)])
            );
            assert_eq!(
                members(&set.staging).await?,
                owned(&[(SENTINEL, 0.0), ("test-ranked-alice:p1", 10.0)])
            );

            remove(&set, "test-ranked-alice:p1").await?;
            assert!(!exists(&set.source).await?);
            assert!(!exists(&set.ranked).await?);
            assert_eq!(members(&set.staging).await?, owned(&[(SENTINEL, 0.0)]));
            cleanup(&[label]).await
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_keeps_ranked_authors_in_every_set() -> TestResult {
            let labels = ["test-ranked-rebuild-rust", "test-ranked-rebuild-spam"];
            setup(&labels).await?;
            rank(&[ALICE, CAROL]).await?;
            let [rust, spam] = labels.map(RankedSet::tag);
            zadd(
                &rust.source,
                &[(1.0, "test-ranked-alice:p1"), (2.0, "test-ranked-bob:p2")],
            )
            .await?;
            zadd(&spam.source, &[(9.0, "test-ranked-bob:p9")]).await?;
            // Drift the rebuild must correct.
            zadd(&rust.ranked, &[(2.0, "test-ranked-bob:p2")]).await?;
            zadd(&spam.ranked, &[(9.0, "test-ranked-bob:p9")]).await?;

            let stats = rebuild().await?;

            assert_eq!(
                members(&rust.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0)])
            );
            assert!(!exists(&spam.ranked).await?);
            assert!(exists(BUILT_AT_KEY).await?);
            assert!(!stats.dropped);
            // The global set is rebuilt from the fixture: only ranked authors.
            let mut conn = get_redis_conn().await?;
            let ranking: BTreeSet<String> = conn.zrange(ranking_key(), 0, -1).await?;
            let ranked_posts: Vec<String> = conn.zrange(RankedSet::global().ranked, 0, -1).await?;
            assert!(!ranked_posts.is_empty());
            let unranked: Vec<&String> = ranked_posts
                .iter()
                .filter(|post| {
                    !post
                        .split_once(':')
                        .is_some_and(|(author, _)| ranking.contains(author))
                })
                .collect();
            assert!(unranked.is_empty(), "unranked authors: {unranked:?}");
            cleanup(&labels).await
        }

        /// Past the atomic limit, across `ZSCAN` pages and a run of equal
        /// scores longer than a page.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_stages_large_sets() -> TestResult {
            let label = "test-ranked-staged";
            setup(&[label]).await?;
            rank(&[ALICE, BOB]).await?;
            let set = RankedSet::tag(label);
            let mut posts: Vec<(String, f64)> = Vec::new();
            posts.extend((0..1_500).map(|i| (format!("{ALICE}:P{i:05}"), 1_000.0)));
            posts.extend((0..700).map(|i| (format!("{BOB}:P{i:05}"), 2_000.0 + i as f64)));
            posts.extend((0..900).map(|i| (format!("{CAROL}:P{i:05}"), 500.0 + i as f64)));
            let entries: Vec<(f64, &str)> = posts.iter().map(|(m, s)| (*s, m.as_str())).collect();
            zadd(&set.source, &entries).await?;

            let mut conn = get_redis_conn().await?;
            let mut stats = RankedRebuildStats::default();
            rebuild_set(&mut conn, &ranking_key(), &set, &mut stats).await?;

            let mut expected: Vec<(String, f64)> = posts
                .into_iter()
                .filter(|(m, _)| !m.starts_with(CAROL))
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(members(&set.ranked).await?, expected);
            assert!(!exists(&set.staging).await?, "staging set left behind");
            let ttl: i64 = conn.ttl(&set.ranked).await?;
            assert_eq!(ttl, -1, "the ranked set kept the staging lease");
            cleanup(&[label]).await
        }

        /// Writes landing during a staged build: a member deleted after its
        /// page was copied is not resurrected, and one created after its region
        /// was scanned is kept.
        #[tokio_shared_rt::test(shared)]
        async fn staged_build_keeps_concurrent_writes() -> TestResult {
            let label = "test-ranked-concurrent";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            zadd(
                &set.source,
                &[(1.0, "test-ranked-alice:p1"), (2.0, "test-ranked-alice:p2")],
            )
            .await?;
            zadd(&set.staging, &[(0.0, SENTINEL)]).await?;

            let mut conn = get_redis_conn().await?;
            let (next, scanned, copied) = copy_batch(&mut conn, &ranking_key(), &set, 0).await?;
            assert_eq!((next, scanned, copied), (0, 2, 2));

            // Deleted, then created, while the build is still running.
            remove(&set, "test-ranked-alice:p2").await?;
            add(&set, "test-ranked-alice:p3", 3.0).await?;

            swap(&mut conn, &set).await?;

            assert_eq!(
                members(&set.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0), ("test-ranked-alice:p3", 3.0)])
            );
            assert!(!exists(&set.staging).await?);
            let ttl: i64 = conn.ttl(&set.ranked).await?;
            assert_eq!(ttl, -1, "the ranked set kept the staging lease");
            cleanup(&[label]).await
        }

        #[tokio_shared_rt::test(shared)]
        async fn small_rebuild_defers_a_set_past_the_atomic_limit() -> TestResult {
            let label = "test-ranked-defer";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            let posts: Vec<String> = (0..=ATOMIC_MAX)
                .map(|i| format!("{ALICE}:P{i:05}"))
                .collect();
            let entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();
            zadd(&set.source, &entries).await?;

            let mut conn = get_redis_conn().await?;
            let rebuilt = rebuild_small(&mut conn, &ranking_key(), &set).await?;

            assert_eq!(rebuilt, None);
            assert!(!exists(&set.ranked).await?, "nothing written on deferral");
            cleanup(&[label]).await
        }

        /// Without a ranking every ranked set goes, the ready marker first. The
        /// ranking is parked and restored, and a second rebuild brings the
        /// fixture's ranked sets back before anything is asserted.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_without_a_ranking_drops_every_ranked_set() -> TestResult {
            let labels = ["test-ranked-drop-x", "test-ranked-drop-y"];
            setup(&labels).await?;
            let [x, y] = labels.map(RankedSet::tag);
            zadd(&x.ranked, &[(1.0, "test-ranked-alice:p1")]).await?;
            zadd(&y.staging, &[(0.0, SENTINEL)]).await?;
            let global = RankedSet::global();

            park_ranking().await?;
            let dropped = rebuild().await;
            let survivors = async {
                let mut survivors = Vec::new();
                for key in [BUILT_AT_KEY, &global.ranked, &x.ranked, &y.staging] {
                    if exists(key).await? {
                        survivors.push(key.to_string());
                    }
                }
                Ok::<_, DynError>(survivors)
            }
            .await;
            let source_kept = exists(&global.source).await;
            restore_ranking().await?;
            rebuild().await?;

            assert!(dropped?.dropped);
            assert!(survivors?.is_empty(), "survived without a ranking");
            // The source sets are not the rebuild's to touch.
            assert!(source_kept?);
            cleanup(&labels).await
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_removes_orphans_and_leftovers() -> TestResult {
            let labels = ["test-ranked-live", "test-ranked-gone", "test-ranked-dead"];
            setup(&labels).await?;
            rank(&[ALICE]).await?;
            let [live, gone, dead] = labels.map(RankedSet::tag);
            zadd(&live.source, &[(1.0, "test-ranked-alice:p1")]).await?;
            // A ranked set whose source vanished, and leftovers of a crashed
            // build, including the global set's (staged, as the fixture is past
            // the atomic limit).
            zadd(&gone.ranked, &[(2.0, "test-ranked-alice:p2")]).await?;
            zadd(&live.staging, &[(0.0, SENTINEL)]).await?;
            zadd(&dead.staging, &[(0.0, SENTINEL)]).await?;
            zadd(&RankedSet::global().staging, &[(0.0, SENTINEL)]).await?;

            let stats = rebuild().await?;

            assert!(stats.orphans >= 1);
            for key in [
                &gone.ranked,
                &live.staging,
                &dead.staging,
                &RankedSet::global().staging,
            ] {
                assert!(!exists(key).await?, "{key} left behind");
            }
            assert_eq!(
                members(&live.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0)])
            );
            cleanup(&labels).await
        }

        #[tokio_shared_rt::test(shared)]
        async fn rebuild_waits_for_a_running_rebuild() -> TestResult {
            let label = "test-ranked-lock";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            zadd(&set.source, &[(1.0, "test-ranked-alice:p1")]).await?;
            let holder = "test-ranked-other-rebuild";
            assert!(
                try_acquire_lock(REBUILD_LOCK_KEY, holder, 60).await?,
                "another rebuild holds the lock"
            );

            let handle = tokio::spawn(rebuild());
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let waited = !handle.is_finished();
            let built_early = exists(&set.ranked).await;
            release_lock(REBUILD_LOCK_KEY, holder).await?;
            handle.await??;

            assert!(waited, "rebuild must wait for the lock holder");
            assert!(!built_early?);
            assert_eq!(
                members(&set.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0)])
            );
            assert!(
                !exists(REBUILD_LOCK_KEY).await?,
                "the lock is released after the rebuild"
            );
            cleanup(&[label]).await
        }

        /// A rebuild cancelled while it holds the lock, as when the trust job is
        /// dropped at shutdown or at its deadline, still releases it. The big
        /// label keeps the rebuild running long enough to cancel it midway.
        #[tokio_shared_rt::test(shared)]
        async fn a_cancelled_rebuild_releases_the_lock() -> TestResult {
            let label = "test-ranked-cancel";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let posts: Vec<String> = (0..20_000).map(|i| format!("{ALICE}:P{i:05}")).collect();
            let entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();
            zadd(&RankedSet::tag(label).source, &entries).await?;

            let handle = tokio::spawn(rebuild());
            while !exists(REBUILD_LOCK_KEY).await? && !handle.is_finished() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            handle.abort();
            let cancelled = handle.await.err().is_some_and(|e| e.is_cancelled());
            let mut released = false;
            for _ in 0..100 {
                if !exists(REBUILD_LOCK_KEY).await? {
                    released = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if !released {
                // Unblocks the rebuilds after this test before failing it.
                let mut conn = get_redis_conn().await?;
                let _: () = conn.del(REBUILD_LOCK_KEY).await?;
            }
            // Finishes the ranked sets the cancelled rebuild left half done.
            rebuild().await?;

            assert!(cancelled, "the rebuild finished before it was cancelled");
            assert!(released, "a cancelled rebuild left the lock held");
            cleanup(&[label]).await
        }

        /// Once the renewal interval has passed the lock is renewed, and a lost
        /// lock stops the rebuild.
        #[tokio_shared_rt::test(shared)]
        async fn keep_lock_renews_and_stops_once_the_lock_is_lost() -> TestResult {
            setup(&[]).await?;
            let key = "test-ranked-lock-renewal";
            let mut conn = get_redis_conn().await?;
            let _: () = conn.del(key).await?;
            let lease = LockLease::new(key);
            assert!(lease.try_acquire(5).await?);
            let due = || {
                Instant::now()
                    .checked_sub(LOCK_RENEW)
                    .ok_or("the monotonic clock is too young")
            };

            keep_lock(&lease, &mut due()?).await?;
            let ttl: i64 = conn.ttl(key).await?;
            let _: () = conn.del(key).await?;
            let lost = keep_lock(&lease, &mut due()?).await;
            lease.disarm();

            assert!(ttl > 5, "renewed to the rebuild TTL, got {ttl}");
            assert!(lost.is_err(), "a lost lock must stop the rebuild");
            Ok(())
        }

        /// The marker and the ranking are restored before anything is asserted.
        #[tokio_shared_rt::test(shared)]
        async fn load_reflects_the_ranking_marker_and_viewer() -> TestResult {
            setup(&[]).await?;
            rank(&[ALICE]).await?;
            rebuild().await?;

            let ranked = TrustMode::load(Some(ALICE)).await;
            let anonymous = TrustMode::load(None).await;
            let stranger = TrustMode::load(Some("test-ranked-stranger")).await;

            let mut conn = get_redis_conn().await?;
            let built_at: String = conn.get(BUILT_AT_KEY).await?;
            let _: () = conn.del(BUILT_AT_KEY).await?;
            let unbuilt = TrustMode::load(None).await;
            let _: () = conn.set(BUILT_AT_KEY, built_at).await?;

            park_ranking().await?;
            let no_ranking = TrustMode::load(Some(ALICE)).await;
            restore_ranking().await?;

            assert_eq!(ranked?, TrustMode::Ranked);
            assert_eq!(anonymous?, TrustMode::Ranked);
            assert_eq!(stranger?, TrustMode::Off);
            assert_eq!(unbuilt?, TrustMode::Unbuilt);
            assert_eq!(no_ranking?, TrustMode::Off);
            cleanup(&[]).await
        }
    }
}
