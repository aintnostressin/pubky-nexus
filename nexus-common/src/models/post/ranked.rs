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
//! reconciles the copies with their sources in place, a page at a time
//! ([`rebuild`]), so drift lasts at most until the next recompute and no call
//! holds Redis for more than one page. Readers only trust the ranked sets once a
//! complete rebuild has run ([`BUILT_AT_KEY`]); before that they serve the
//! unfiltered sets.
//!
//! The scripts take several keys, so they assume a single Redis instance (not
//! Redis Cluster), as the rest of Nexus does.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use deadpool_redis::Connection;
use redis::{AsyncCommands, Script};

use super::search::TAG_GLOBAL_POST_TIMELINE;
use super::stream::POST_TIMELINE_KEY_PARTS;
use crate::db::get_redis_conn;
use crate::db::kv::{RedisResult, SORTED_PREFIX};
use crate::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;

/// Ranked copy of the global timeline: `Sorted:Posts:Ranked:Timeline`.
/// A different second segment from the source set, so a glob for one never matches the other.
pub const POST_RANKED_TIMELINE_KEY_PARTS: [&str; 3] = ["Posts", "Ranked", "Timeline"];
/// Ranked copies of the per-label timelines: `Sorted:Tags:Ranked:Post:Timeline:<label>`.
/// A different second segment from the source sets, so a scan for one never matches the other.
pub const TAG_RANKED_POST_TIMELINE: [&str; 4] = ["Tags", "Ranked", "Post", "Timeline"];
/// Set once a complete rebuild has run; readers trust the ranked sets only then.
const BUILT_AT_KEY: &str = "Ranked:Timeline:BuiltAt";

/// Members per `ZSCAN` page, the most a rebuild call touches. Sized so a call
/// stays well under 10 ms of Redis time: at 200k root posts the slowest took 3 ms.
const BATCH: usize = 500;
/// Keys per `SCAN` page when listing labels; cheap per key, so larger.
const SCAN_COUNT: usize = 1_000;

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
}

impl RankedSet {
    /// The global timeline.
    pub fn global() -> Self {
        RankedSet {
            source: sorted_key(&POST_TIMELINE_KEY_PARTS),
            ranked: sorted_key(&POST_RANKED_TIMELINE_KEY_PARTS),
        }
    }

    /// The per-label timeline for `label`.
    pub fn tag(label: &str) -> Self {
        let key = |parts: &[&str]| sorted_key(&[parts, &[label]].concat());
        RankedSet {
            source: key(&TAG_GLOBAL_POST_TIMELINE),
            ranked: key(&TAG_RANKED_POST_TIMELINE),
        }
    }
}

/// Rebuilds every ranked set from the current ranking, or drops them all when
/// there is no ranking. Takes no lock: every page reads the live ranking, so
/// rebuilds that overlap converge on the same sets. In production only the
/// trust job rebuilds, one run at a time under its job lock.
pub(crate) async fn rebuild() -> RedisResult<RankedRebuildStats> {
    let mut conn = get_redis_conn().await?;
    let mut stats = RankedRebuildStats::default();
    let trust = ranking_key();
    let trust_exists: bool = conn.exists(&trust).await?;
    if !trust_exists {
        drop_all(&mut conn).await?;
        stats.dropped = true;
        return Ok(stats);
    }

    let tags = scan_tag_keys(&mut conn).await?;
    // A ranked set normally empties, and so vanishes, with its source; one that
    // outlived its source drifted, and reconciling it against the missing
    // source empties it.
    stats.orphans = tags.ranked.difference(&tags.sources).count();
    let labels = tags
        .sources
        .union(&tags.ranked)
        .map(|label| RankedSet::tag(label));
    for set in std::iter::once(RankedSet::global()).chain(labels) {
        rebuild_set(&mut conn, &trust, &set, &mut stats).await?;
    }

    let built_at = chrono::Utc::now().timestamp_millis();
    let _: () = conn.set(BUILT_AT_KEY, built_at).await?;
    Ok(stats)
}

/// Drops every ranked set. The ready marker goes first, so readers fall back
/// to the unfiltered sets before any ranked set disappears.
async fn drop_all(conn: &mut Connection) -> RedisResult<()> {
    let _: () = conn.del(BUILT_AT_KEY).await?;
    let _: () = conn.unlink(RankedSet::global().ranked).await?;
    let tags = scan_tag_keys(conn).await?;
    let ranked = tags.ranked.iter().map(|label| RankedSet::tag(label).ranked);
    unlink_keys(conn, ranked).await
}

/// The labels of every per-label family, from one pass over the keyspace:
/// `SCAN` costs the whole keyspace however few keys match, so the families
/// share it. Collected up front: a rebuild only writes keys of families it
/// has already listed, and the sets absorb the repeats `SCAN` may return.
async fn scan_tag_keys(conn: &mut Connection) -> RedisResult<TagKeys> {
    let prefix = |parts: &[&str]| format!("{}:", sorted_key(parts));
    let source_prefix = prefix(&TAG_GLOBAL_POST_TIMELINE);
    let ranked_prefix = prefix(&TAG_RANKED_POST_TIMELINE);
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

/// Lua shared by the scripts below. Each check takes a whole page in one
/// command, so a script makes a few calls per page rather than a few per post,
/// and writes and rebuilds agree on who counts as ranked.
/// - `ranked_flags(members)`: for each `author:post`, whether its author is in
///   the ranking at `KEYS[2]`.
/// - `prune(ranked, source, members)`: removes from `ranked` the `members` no
///   longer in `source`, returning how many went.
const LUA_HELPERS: &str = r"
local function ranked_flags(members)
    local authors = {}
    for i, member in ipairs(members) do
        local sep = string.find(member, ':', 1, true)
        authors[i] = sep and string.sub(member, 1, sep - 1) or ''
    end
    if #authors == 0 then return {} end
    return redis.call('ZMSCORE', KEYS[2], unpack(authors))
end
local function prune(ranked, source, members)
    if #members == 0 then return 0 end
    local scores, gone = redis.call('ZMSCORE', source, unpack(members)), {}
    for i, member in ipairs(members) do
        if not scores[i] then gone[#gone + 1] = member end
    end
    if #gone == 0 then return 0 end
    return redis.call('ZREM', ranked, unpack(gone))
end
";

/// Writes a member to a source set and, when its author is in the ranking, to
/// the ranked copy at the same score. One atomic step, so the copy never
/// disagrees with the source.
static ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            LUA_HELPERS,
            r"redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
              if not ranked_flags({ARGV[1]})[1] then return 0 end
              redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
              return 1",
        ]
        .concat(),
    )
});

/// Reconciles one `ZSCAN` page of a source set with its ranked copy: the page's
/// ranked authors' posts are written at the source's score, everyone else's
/// removed. A first call also prunes a ranked set small enough for one page of
/// posts no longer in the source, so a small set takes one call. Returns
/// `{next cursor, scanned, added, removed, pruned}`.
static RECONCILE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            LUA_HELPERS,
            r"local added, removed, pruned = 0, 0, 0
              if ARGV[1] == '0' and redis.call('ZCARD', KEYS[3]) <= tonumber(ARGV[2]) then
                  removed = prune(KEYS[3], KEYS[1], redis.call('ZRANGE', KEYS[3], 0, -1))
                  pruned = 1
              end
              local page = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
              local entries, members = page[2], {}
              for i = 1, #entries, 2 do members[#members + 1] = entries[i] end
              local flags, keep, drop = ranked_flags(members), {}, {}
              for j, member in ipairs(members) do
                  if flags[j] then
                      keep[#keep + 1] = entries[2 * j]
                      keep[#keep + 1] = member
                  else
                      drop[#drop + 1] = member
                  end
              end
              if #keep > 0 then added = redis.call('ZADD', KEYS[3], 'CH', unpack(keep)) end
              if #drop > 0 then removed = removed + redis.call('ZREM', KEYS[3], unpack(drop)) end
              return {page[1], #members, added, removed, pruned}",
        ]
        .concat(),
    )
});

/// Removes, from one `ZSCAN` page of a ranked set, the posts no longer in its
/// source. Returns `{next cursor, removed}`.
static PRUNE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        &[
            LUA_HELPERS,
            r"local page = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
              local members = {}
              for i = 1, #page[2], 2 do members[#members + 1] = page[2][i] end
              return {page[1], prune(KEYS[1], KEYS[2], members)}",
        ]
        .concat(),
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
        .arg(member)
        .arg(score)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

/// Removes `member` from `set.source` and its ranked copy in one transaction.
pub(crate) async fn remove(set: &RankedSet, member: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let _: () = redis::pipe()
        .atomic()
        .zrem(&set.source, member)
        .ignore()
        .zrem(&set.ranked, member)
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
    /// Members added to, or rescored in, a ranked set.
    pub added: usize,
    /// Members removed from a ranked set.
    pub removed: usize,
    /// Ranked sets whose source set no longer existed.
    pub orphans: usize,
    /// No ranking existed, so every ranked set was dropped instead.
    pub dropped: bool,
}

/// Reconciles `set.ranked` with `set.source` in place, a page at a time.
/// Concurrent writes stay correct: each page is one atomic script, and
/// [`add`] and [`remove`] keep the copy in step with the source in between.
async fn rebuild_set(
    conn: &mut Connection,
    trust: &str,
    set: &RankedSet,
    stats: &mut RankedRebuildStats,
) -> RedisResult<()> {
    stats.sets += 1;
    let mut pruned = false;
    let mut cursor = 0;
    loop {
        let (next, scanned, added, removed, small) =
            reconcile_page(conn, trust, set, cursor).await?;
        stats.scanned += scanned;
        stats.added += added;
        stats.removed += removed;
        pruned |= small;
        if next == 0 {
            break;
        }
        cursor = next;
    }
    // A ranked set too big to prune in the first call, a page at a time.
    let mut cursor = 0;
    while !pruned {
        let (next, removed) = prune_page(conn, set, cursor).await?;
        stats.removed += removed;
        pruned = next == 0;
        cursor = next;
    }
    Ok(())
}

/// `(next cursor, scanned, added, removed, pruned)` for the source page at `cursor`.
async fn reconcile_page(
    conn: &mut Connection,
    trust: &str,
    set: &RankedSet,
    cursor: u64,
) -> RedisResult<(u64, usize, usize, usize, bool)> {
    RECONCILE
        .key(&set.source)
        .key(trust)
        .key(&set.ranked)
        .arg(cursor)
        .arg(BATCH)
        .invoke_async(conn)
        .await
        .map_err(Into::into)
}

/// `(next cursor, removed)` for the ranked page at `cursor`.
async fn prune_page(
    conn: &mut Connection,
    set: &RankedSet,
    cursor: u64,
) -> RedisResult<(u64, usize)> {
    PRUNE
        .key(&set.ranked)
        .key(&set.source)
        .arg(cursor)
        .arg(BATCH)
        .invoke_async(conn)
        .await
        .map_err(Into::into)
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
        let tag = RankedSet::tag("bitcoin");
        assert_eq!(tag.source, "Sorted:Tags:Global:Post:Timeline:bitcoin");
        assert_eq!(tag.ranked, "Sorted:Tags:Ranked:Post:Timeline:bitcoin");
        // A scan or glob for one family of keys must never match another.
        let families = [global.source, global.ranked, tag.source, tag.ranked];
        for (i, a) in families.iter().enumerate() {
            for (j, b) in families.iter().enumerate() {
                let prefix = b.trim_end_matches("bitcoin");
                assert!(i == j || !a.starts_with(prefix), "{a} starts with {prefix}");
            }
        }
    }

    /// Live tests against the shared Redis and its fixture ranking. Each test
    /// writes only its own labels and authors, and clears them before and
    /// after. A full rebuild and the ranking key are shared, so
    /// `.config/nextest.toml` runs each of these alone.
    mod live {
        use super::super::*;
        use crate::db::kv::RedisOps;
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
                [set.source, set.ranked]
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

            remove(&set, "test-ranked-alice:p1").await?;
            assert!(!exists(&set.ranked).await?);
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

        /// Across `ZSCAN` pages and a run of equal scores longer than a page,
        /// with a ranked set too big to prune in the first call: posts gone from
        /// the source and an unranked author's posts are removed.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_reconciles_large_sets() -> TestResult {
            let label = "test-ranked-large";
            setup(&[label]).await?;
            rank(&[ALICE, BOB]).await?;
            let set = RankedSet::tag(label);
            let mut posts: Vec<(String, f64)> = Vec::new();
            posts.extend((0..1_500).map(|i| (format!("{ALICE}:P{i:05}"), 1_000.0)));
            posts.extend((0..700).map(|i| (format!("{BOB}:P{i:05}"), 2_000.0 + i as f64)));
            posts.extend((0..900).map(|i| (format!("{CAROL}:P{i:05}"), 500.0 + i as f64)));
            let entries: Vec<(f64, &str)> = posts.iter().map(|(m, s)| (*s, m.as_str())).collect();
            zadd(&set.source, &entries).await?;
            let gone: Vec<String> = (0..600).map(|i| format!("{BOB}:GONE{i:05}")).collect();
            let mut stale: Vec<(f64, &str)> = gone.iter().map(|m| (3_000.0, m.as_str())).collect();
            stale.extend(
                entries
                    .iter()
                    .filter(|(_, m)| m.starts_with(CAROL))
                    .take(50),
            );
            zadd(&set.ranked, &stale).await?;

            let mut conn = get_redis_conn().await?;
            let mut stats = RankedRebuildStats::default();
            rebuild_set(&mut conn, &ranking_key(), &set, &mut stats).await?;

            let mut expected: Vec<(String, f64)> = posts
                .into_iter()
                .filter(|(m, _)| !m.starts_with(CAROL))
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(members(&set.ranked).await?, expected);
            assert_eq!(
                stats.removed, 650,
                "600 posts gone, 50 by an unranked author"
            );
            cleanup(&[label]).await
        }

        /// Writes landing mid-rebuild: a post deleted after its page was
        /// reconciled is not resurrected, and one created meanwhile is kept.
        #[tokio_shared_rt::test(shared)]
        async fn writes_during_a_rebuild_are_kept() -> TestResult {
            let label = "test-ranked-concurrent";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            let posts: Vec<String> = (0..1_200).map(|i| format!("{ALICE}:P{i:05}")).collect();
            let entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();
            zadd(&set.source, &entries).await?;

            let mut conn = get_redis_conn().await?;
            let (mut cursor, ..) = reconcile_page(&mut conn, &ranking_key(), &set, 0).await?;
            assert_ne!(cursor, 0, "the set spans several pages");
            let reconciled: Vec<String> = conn.zrange(&set.ranked, 0, 0).await?;
            let deleted = reconciled.first().ok_or("nothing reconciled")?;
            remove(&set, deleted).await?;
            add(&set, "test-ranked-alice:NEW", 2.0).await?;
            while cursor != 0 {
                (cursor, ..) = reconcile_page(&mut conn, &ranking_key(), &set, cursor).await?;
            }

            let source = members(&set.source).await?;
            assert_eq!(members(&set.ranked).await?, source);
            assert!(!source.iter().any(|(m, _)| m == deleted));
            assert!(source.iter().any(|(m, _)| m == "test-ranked-alice:NEW"));
            cleanup(&[label]).await
        }

        /// A set small enough for one page, its ranked copy included, takes one call.
        #[tokio_shared_rt::test(shared)]
        async fn a_small_set_is_reconciled_in_one_call() -> TestResult {
            let label = "test-ranked-small";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            let posts = [(1.0, "test-ranked-alice:p1"), (2.0, "test-ranked-bob:p2")];
            zadd(&set.source, &posts).await?;
            // Stale: a post gone from the source, and an unranked author's post.
            let stale = [(3.0, "test-ranked-alice:gone"), (2.0, "test-ranked-bob:p2")];
            zadd(&set.ranked, &stale).await?;

            let mut conn = get_redis_conn().await?;
            let page = reconcile_page(&mut conn, &ranking_key(), &set, 0).await?;

            assert_eq!(page, (0, 2, 1, 2, true));
            assert_eq!(
                members(&set.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0)])
            );
            cleanup(&[label]).await
        }

        /// Without a ranking every ranked set goes, the ready marker first. The
        /// ranking is parked and restored, and a second rebuild brings the
        /// fixture's ranked sets back before anything is asserted.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_without_a_ranking_drops_every_ranked_set() -> TestResult {
            let label = "test-ranked-drop";
            setup(&[label]).await?;
            let set = RankedSet::tag(label);
            zadd(&set.ranked, &[(1.0, "test-ranked-alice:p1")]).await?;
            let global = RankedSet::global();

            park_ranking().await?;
            let dropped = rebuild().await;
            let survivors = async {
                let mut survivors = Vec::new();
                for key in [BUILT_AT_KEY, &global.ranked, &set.ranked] {
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
            cleanup(&[label]).await
        }

        /// A ranked set whose source is gone is emptied by reconciling it.
        #[tokio_shared_rt::test(shared)]
        async fn rebuild_empties_ranked_sets_whose_source_is_gone() -> TestResult {
            let labels = ["test-ranked-live", "test-ranked-gone"];
            setup(&labels).await?;
            rank(&[ALICE]).await?;
            let [live, gone] = labels.map(RankedSet::tag);
            zadd(&live.source, &[(1.0, "test-ranked-alice:p1")]).await?;
            zadd(&gone.ranked, &[(2.0, "test-ranked-alice:p2")]).await?;

            let stats = rebuild().await?;

            assert!(stats.orphans >= 1);
            assert!(
                !exists(&gone.ranked).await?,
                "orphaned ranked set left behind"
            );
            assert_eq!(
                members(&live.ranked).await?,
                owned(&[("test-ranked-alice:p1", 1.0)])
            );
            cleanup(&labels).await
        }

        /// No lock: two rebuilds running at once still leave every set exact.
        #[tokio_shared_rt::test(shared)]
        async fn overlapping_rebuilds_converge() -> TestResult {
            let label = "test-ranked-overlap";
            setup(&[label]).await?;
            rank(&[ALICE]).await?;
            let set = RankedSet::tag(label);
            let posts: Vec<String> = (0..1_200).map(|i| format!("{ALICE}:P{i:05}")).collect();
            let mut entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();
            entries.push((2.0, "test-ranked-bob:p1"));
            zadd(&set.source, &entries).await?;

            let (first, second) = tokio::join!(rebuild(), rebuild());
            first?;
            second?;

            let ranked: Vec<(String, f64)> = posts.iter().map(|m| (m.clone(), 1.0)).collect();
            assert_eq!(members(&set.ranked).await?, ranked);
            cleanup(&[label]).await
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
