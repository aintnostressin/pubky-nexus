use std::collections::BTreeMap;

use super::reconcile::{shared_entries, AuthorPost};
use super::*;
use crate::models::post::search::{TAG_GLOBAL_POST_ENGAGEMENT, TAG_GLOBAL_POST_TIMELINE};
use crate::models::post::{
    POST_REPLIES_PER_POST_KEY_PARTS, POST_TIMELINE_KEY_PARTS, POST_TOTAL_ENGAGEMENT_KEY_PARTS,
};
use crate::models::user::SocialGraphStatus;

fn ranking_key() -> String {
    SocialGraphStatus::ranking_key()
}

fn timeline_key() -> String {
    sorted_key(&POST_TIMELINE_KEY_PARTS)
}

fn engagement_key() -> String {
    sorted_key(&POST_TOTAL_ENGAGEMENT_KEY_PARTS)
}

fn tag_timeline_key(label: &str) -> String {
    sorted_key(&[&TAG_GLOBAL_POST_TIMELINE[..], &[label]].concat())
}

fn tag_engagement_key(label: &str) -> String {
    sorted_key(&[&TAG_GLOBAL_POST_ENGAGEMENT[..], &[label]].concat())
}

fn replies_key(author_id: &str, post_id: &str) -> String {
    sorted_key(&[&POST_REPLIES_PER_POST_KEY_PARTS[..], &[author_id, post_id]].concat())
}

/// Root posts go to the global sets, replies to their parent's thread unless
/// the thread's owner wrote them, and every post to the sets of its labels.
#[test]
fn shared_entries_cover_every_set_an_author_reaches() {
    let post = |id: &str, indexed_at, parent: Option<(&str, &str)>, labels: &[&str], engagement| {
        AuthorPost {
            author: "alice".to_string(),
            id: id.to_string(),
            indexed_at,
            parent: parent.map(|(a, p)| (a.to_string(), p.to_string())),
            labels: labels.iter().map(ToString::to_string).collect(),
            engagement,
            mentions: 1,
        }
    };
    let posts = [
        post("root", 10, None, &["x", "y"], 5),
        post("reply", 20, Some(("bob", "b1")), &["x"], 1),
        post("own", 30, Some(("alice", "root")), &["z"], 0),
    ];

    let entries = shared_entries(&posts);

    let entry = |score: f64, id: &str| (score, format!("alice:{id}"));
    let expected = BTreeMap::from([
        (timeline_key(), vec![entry(10.0, "root")]),
        (engagement_key(), vec![entry(5.0, "root")]),
        (replies_key("bob", "b1"), vec![entry(20.0, "reply")]),
        (
            tag_timeline_key("x"),
            vec![entry(10.0, "root"), entry(20.0, "reply")],
        ),
        // Engagement plus mentions, whatever the label.
        (
            tag_engagement_key("x"),
            vec![entry(6.0, "root"), entry(2.0, "reply")],
        ),
        (tag_timeline_key("y"), vec![entry(10.0, "root")]),
        (tag_engagement_key("y"), vec![entry(6.0, "root")]),
        (tag_timeline_key("z"), vec![entry(30.0, "own")]),
        (tag_engagement_key("z"), vec![entry(1.0, "own")]),
    ]);
    assert_eq!(entries, expected);
}

/// Live tests against the shared Redis, the fixture graph and its ranking. The
/// gate tests write only their own keys and authors; the reconcile tests move
/// the fixture's spammer in and out and put it back. All of them touch the
/// shared ranking, so `.config/nextest.toml` runs each one alone.
mod live {
    use redis::AsyncCommands;

    use super::super::reconcile::{shared_entries, write_pages, AuthorPost, Write};
    use super::super::*;
    use super::{
        engagement_key, ranking_key, replies_key, tag_engagement_key, tag_timeline_key,
        timeline_key, TAG_GLOBAL_POST_ENGAGEMENT, TAG_GLOBAL_POST_TIMELINE,
    };
    use crate::db::kv::SortOrder;
    use crate::db::{fetch_all_rows_from_graph, queries};
    use crate::models::bootstrap::{Bootstrap, ViewType};
    use crate::models::post::{KindFilter, PostStream, StreamSource};
    use crate::models::user::SocialGraphStatus;
    use crate::types::{DynError, Pagination, StreamSorting};
    use crate::{FeaturesConfig, StackConfig, StackManager};
    use pubky_app_specs::PubkyAppPostKind;

    type TestResult = Result<(), DynError>;

    const ALICE: &str = "test-filter-alice";
    const BOB: &str = "test-filter-bob";
    /// Where a test parks the ranking while it checks behaviour without one.
    const RANKING_ASIDE: &str = "Test:TrustFilter:RankingAside";

    /// wot.cypher's spammer: unranked, with a root post and a reply to D2's
    /// post that ranked users tagged.
    const SPAMMER: &str = "qdsygndnk45m9ru5jseg3uxk5xg4usj9hrcraqbzgigapzweaa9o";
    const D2: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y";
    /// Ranked skunk.cypher authors whose shared entries no test rewrites. The
    /// engaged author's posts have replies, reposts and tags; the mentioner's
    /// tagged posts mention users.
    const ENGAGED: &str = "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";
    const MENTIONER: &str = "h3fghnb3x59oh7r53x8y6a5x38oatqyjym9b31ybss17zqdnhcoy";

    /// Every entry the spammer's posts would have in the shared sets, with
    /// the scores a full reindex gives them. The reply has six tags.
    fn spammer_entries() -> Vec<(String, String, f64)> {
        let root = format!("{SPAMMER}:WOTPOSTS00006");
        let reply = format!("{SPAMMER}:WOTPOSTMODF01");
        let mut entries = vec![
            (timeline_key(), root.clone(), 1650000000006.0),
            (engagement_key(), root, 0.0),
            (
                replies_key(D2, "WOTPOSTTAGS01"),
                reply.clone(),
                1650000000013.0,
            ),
        ];
        for label in [
            "wmtag1",
            "wmtag2",
            "wmtag3",
            "wmtag4",
            "wmtag5",
            "wmtagflag",
        ] {
            entries.push((tag_timeline_key(label), reply.clone(), 1650000000013.0));
            entries.push((tag_engagement_key(label), reply.clone(), 6.0));
        }
        entries
    }

    /// The score of each spammer entry, `None` where it is absent.
    async fn spammer_scores() -> Result<Vec<Option<f64>>, DynError> {
        scores(&spammer_entries()).await
    }

    /// The score of each `(key, member, _)` entry, `None` where it is absent.
    async fn scores(entries: &[(String, String, f64)]) -> Result<Vec<Option<f64>>, DynError> {
        let mut conn = get_redis_conn().await?;
        let mut scores = Vec::new();
        for (key, member, _) in entries {
            scores.push(conn.zscore(key, member).await?);
        }
        Ok(scores)
    }

    /// Every entry `author`'s posts have in the shared sets, as `(key, member,
    /// score)`, read from the graph as [`reconcile()`] reads it.
    async fn author_entries(author: &str) -> Result<Vec<(String, String, f64)>, DynError> {
        let query = queries::get::author_post_entries(author, (i64::MIN, ""), 1_000);
        let rows = fetch_all_rows_from_graph(query).await?;
        let posts = rows
            .iter()
            .map(AuthorPost::from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let entries = shared_entries(&posts)
            .into_iter()
            .flat_map(|(key, entries)| {
                entries
                    .into_iter()
                    .map(move |(score, member)| (key.clone(), member, score))
            })
            .collect();
        Ok(entries)
    }

    /// The scores of `entries`, each one present.
    fn present(entries: &[(String, String, f64)]) -> Vec<Option<f64>> {
        entries.iter().map(|(_, _, score)| Some(*score)).collect()
    }

    fn all_present() -> Vec<Option<f64>> {
        present(&spammer_entries())
    }

    fn all_absent() -> Vec<Option<f64>> {
        vec![None; spammer_entries().len()]
    }

    /// Connects, and puts back what an earlier failed run may have left: a
    /// parked ranking, test authors in the ranking, the spammer ranked, the
    /// switch off, or the shared sets out of step with the ranking.
    async fn setup(keys: &[&str]) -> TestResult {
        StackManager::setup(&StackConfig::default()).await?;
        let mut conn = get_redis_conn().await?;
        let parked: bool = conn.exists(RANKING_ASIDE).await?;
        if parked {
            let _: () = conn.rename(RANKING_ASIDE, ranking_key()).await?;
        }
        cleanup(keys).await
    }

    async fn cleanup(keys: &[&str]) -> TestResult {
        let mut conn = get_redis_conn().await?;
        if !keys.is_empty() {
            let _: () = conn.unlink(keys).await?;
        }
        let _: () = conn.zrem(ranking_key(), &[ALICE, BOB, SPAMMER]).await?;
        switch(true).await?;
        reconcile().await?;
        Ok(())
    }

    /// Ranks `user_id` below every fixture user, so fixture ranks are untouched.
    async fn rank(user_id: &str) -> TestResult {
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(ranking_key(), user_id, 1e9).await?;
        Ok(())
    }

    async fn unrank(user_id: &str) -> TestResult {
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zrem(ranking_key(), user_id).await?;
        Ok(())
    }

    async fn members(key: &str) -> Result<Vec<(String, f64)>, DynError> {
        let mut conn = get_redis_conn().await?;
        Ok(conn.zrange_withscores(key, 0, -1).await?)
    }

    fn owned(entries: &[(&str, f64)]) -> Vec<(String, f64)> {
        entries.iter().map(|(m, s)| (m.to_string(), *s)).collect()
    }

    /// The key parts the writers pass for a test tag set, and its key.
    fn tag_set(family: [&'static str; 4], label: &'static str) -> (Vec<&'static str>, String) {
        let parts = [&family[..], &[label]].concat();
        let key = sorted_key(&parts);
        (parts, key)
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

    /// The gate holds across the batches one call is split into, and
    /// `add_always` skips it.
    #[tokio_shared_rt::test(shared)]
    async fn add_keeps_ranked_authors_only() -> TestResult {
        let (parts, key) = tag_set(TAG_GLOBAL_POST_TIMELINE, "test-filter-add");
        setup(&[&key]).await?;
        rank(ALICE).await?;
        let posts: Vec<String> = (0..1_200)
            .map(|i| match i % 2 {
                0 => format!("{ALICE}:P{i:05}"),
                _ => format!("{BOB}:P{i:05}"),
            })
            .collect();
        let entries: Vec<(f64, &str)> = posts.iter().map(|m| (1.0, m.as_str())).collect();

        add(&parts, &entries).await?;
        let gated = members(&key).await?;
        add_always(&parts, &[(3.0, "test-filter-bob:p3")]).await?;
        let forced = members(&key).await?;

        cleanup(&[&key]).await?;
        assert_eq!(gated.len(), 600);
        assert!(gated.iter().all(|(m, _)| m.starts_with(ALICE)));
        assert!(forced.contains(&("test-filter-bob:p3".to_string(), 3.0)));
        Ok(())
    }

    /// Without a ranking the gate takes everyone, as before the filter.
    #[tokio_shared_rt::test(shared)]
    async fn add_takes_everyone_without_a_ranking() -> TestResult {
        let (parts, key) = tag_set(TAG_GLOBAL_POST_TIMELINE, "test-filter-open");
        setup(&[&key]).await?;

        park_ranking().await?;
        let added = add(&parts, &[(1.0, "test-filter-bob:p1")]).await;
        // Cypher-served streams follow the applied copy, not the live ranking.
        let still_applied = is_ranking_applied().await;
        restore_ranking().await?;
        let kept = members(&key).await?;

        cleanup(&[&key]).await?;
        added?;
        assert_eq!(kept, owned(&[("test-filter-bob:p1", 1.0)]));
        assert!(still_applied?);
        Ok(())
    }

    /// An increment moves a member that is there, whoever wrote it, and creates
    /// one only for a ranked author; a decrement never creates one.
    #[tokio_shared_rt::test(shared)]
    async fn incr_never_creates_a_hidden_or_negative_entry() -> TestResult {
        let (parts, key) = tag_set(TAG_GLOBAL_POST_ENGAGEMENT, "test-filter-incr");
        setup(&[&key]).await?;
        rank(ALICE).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(&key, "test-filter-bob:there", 5.0).await?;

        incr(&parts, &[BOB, "new"], ScoreAction::Increment(1.0)).await?;
        incr(&parts, &[ALICE, "new"], ScoreAction::Increment(1.0)).await?;
        incr(&parts, &[ALICE, "gone"], ScoreAction::Decrement(1.0)).await?;
        incr(&parts, &[BOB, "there"], ScoreAction::Decrement(1.0)).await?;
        let scores = members(&key).await?;

        cleanup(&[&key]).await?;
        assert_eq!(
            scores,
            owned(&[
                ("test-filter-alice:new", 1.0),
                ("test-filter-bob:there", 4.0)
            ])
        );
        Ok(())
    }

    /// Off, the gate takes every author, Cypher-served streams stop filtering,
    /// and the next reconcile writes the hidden authors back and drops the copy.
    /// On again, the next reconcile hides them.
    #[tokio_shared_rt::test(shared)]
    async fn the_switch_turns_the_filter_off_and_back_on() -> TestResult {
        let (parts, key) = tag_set(TAG_GLOBAL_POST_TIMELINE, "test-filter-switch");
        setup(&[&key]).await?;

        switch(false).await?;
        let added = add(&parts, &[(1.0, "test-filter-bob:p1")]).await;
        let counted = incr(&parts, &[BOB, "p2"], ScoreAction::Increment(1.0)).await;
        let opened = reconcile().await;
        let open_scores = spammer_scores().await;
        let open_applied = is_ranking_applied().await;
        switch(true).await?;
        let closed = reconcile().await;
        let closed_scores = spammer_scores().await;
        let written = members(&key).await;

        cleanup(&[&key]).await?;
        added?;
        counted?;
        opened?;
        closed?;
        assert_eq!(
            written?,
            owned(&[("test-filter-bob:p1", 1.0), ("test-filter-bob:p2", 1.0)])
        );
        assert_eq!(open_scores?, all_present());
        assert!(!open_applied?);
        assert_eq!(closed_scores?, all_absent());
        Ok(())
    }

    /// Switching off shows every author the ranking may have hidden, even when
    /// its first reconcile was cut short: the filtered record it set first is
    /// there, but no copy. Switching off clears the record, and switching back
    /// on sets it again.
    #[tokio_shared_rt::test(shared)]
    async fn switching_off_shows_what_an_unfinished_apply_hid() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(applied_key()).await?;

        switch(false).await?;
        let shown = reconcile().await;
        let scores = spammer_scores().await;
        let left: bool = conn.exists(&[FILTERED_KEY, SHOWING_KEY]).await?;
        cleanup(&[]).await?;
        let restored: bool = conn.exists(FILTERED_KEY).await?;

        shown?;
        assert_eq!(scores?, all_present());
        assert!(!left, "switching off clears its records");
        assert!(restored, "switching on sets it again");
        Ok(())
    }

    /// Switching back on after a switch-off was cut short hides every author the
    /// ranking leaves out, though the copy still matches the ranking: the run
    /// cut short may already have shown some of them.
    #[tokio_shared_rt::test(shared)]
    async fn switching_on_after_an_unfinished_switch_off_hides_again() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        // A switch-off cut short: it marked itself and showed the spammer, but
        // never dropped the copy.
        let _: () = conn.set(SHOWING_KEY, 1).await?;
        switch(false).await?;
        let shown = write_pages(&mut conn, SPAMMER, Write::Show, 1_000).await;
        switch(true).await?;

        let hidden = reconcile().await;
        let scores = spammer_scores().await;
        let left: bool = conn.exists(SHOWING_KEY).await?;
        cleanup(&[]).await?;

        shown?;
        hidden?;
        assert_eq!(scores?, all_absent());
        assert!(!left, "switching on clears the marker");
        Ok(())
    }

    /// After a switch-off cut short, switching on also shows an author the
    /// ranking admits beyond the copy, whom the run cut short may not have
    /// reached.
    #[tokio_shared_rt::test(shared)]
    async fn switching_on_after_an_unfinished_switch_off_shows_newly_ranked() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.set(SHOWING_KEY, 1).await?;
        rank(SPAMMER).await?;

        let applied = reconcile().await;
        let scores = spammer_scores().await;
        cleanup(&[]).await?;

        applied?;
        assert_eq!(scores?, all_present());
        Ok(())
    }

    /// Switching off marks itself before it reads the ranking or shows anyone,
    /// so a run cut short leaves the marker for the next switch-on. Here the
    /// ranking can't be read.
    #[tokio_shared_rt::test(shared)]
    async fn a_failed_switch_off_leaves_the_showing_marker() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        park_ranking().await?;
        let _: () = conn.set(ranking_key(), "not a sorted set").await?;

        switch(false).await?;
        let shown = reconcile().await;
        let marked: bool = conn.exists(SHOWING_KEY).await?;
        let _: () = conn.del(ranking_key()).await?;
        restore_ranking().await?;
        cleanup(&[]).await?;

        assert!(shown.is_err(), "the ranking can't be read");
        assert!(marked, "the marker is set before the failure");
        Ok(())
    }

    /// The watcher's startup toggle stores the switch every service follows,
    /// then applies it: off shows the hidden authors, on hides them again.
    #[tokio_shared_rt::test(shared)]
    async fn toggle_stores_the_switch_and_applies_it() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let features = |on| FeaturesConfig {
            hide_unranked_authors: on,
        };

        let off = features(false).toggle().await;
        let stored_off: bool = conn.exists(ON_KEY).await?;
        let off_scores = spammer_scores().await;
        let on = features(true).toggle().await;
        let stored_on: bool = conn.exists(ON_KEY).await?;
        let on_scores = spammer_scores().await;
        cleanup(&[]).await?;

        off?;
        on?;
        assert!(!stored_off, "switched off");
        assert_eq!(off_scores?, all_present());
        assert!(stored_on, "switched on");
        assert_eq!(on_scores?, all_absent());
        Ok(())
    }

    /// Switching on records that posts may be hidden in the same step, before
    /// the gate or a reconcile hides anything.
    #[tokio_shared_rt::test(shared)]
    async fn switching_on_records_that_posts_may_be_hidden() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(FILTERED_KEY).await?;

        let switched = switch(true).await;
        let recorded: bool = conn.exists(FILTERED_KEY).await?;
        cleanup(&[]).await?;

        switched?;
        assert!(recorded);
        Ok(())
    }

    /// With the switch off a hide takes nothing out, so a reconcile still
    /// hiding authors when the switch goes off can't undo the switch-off.
    #[tokio_shared_rt::test(shared)]
    async fn hide_spares_everyone_while_the_switch_is_off() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        switch(false).await?;

        let shown = write_pages(&mut conn, SPAMMER, Write::Show, 1_000).await;
        let hidden = write_pages(&mut conn, SPAMMER, Write::Hide, 1_000).await;
        let scores = spammer_scores().await;
        // The copy still matches the ranking, so the reconcile in cleanup
        // wouldn't hide the spammer again.
        switch(true).await?;
        write_pages(&mut conn, SPAMMER, Write::Hide, 1_000).await?;
        cleanup(&[]).await?;

        shown?;
        hidden?;
        assert_eq!(scores?, all_present());
        Ok(())
    }

    /// Sets that were never filtered have neither the record nor a copy, so
    /// switching off leaves them alone, however many authors the ranking
    /// leaves out.
    #[tokio_shared_rt::test(shared)]
    async fn switching_off_skips_sets_never_filtered() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(&[applied_key(), FILTERED_KEY.to_string()]).await?;

        switch(false).await?;
        let skipped = reconcile().await;
        let scores = spammer_scores().await;
        cleanup(&[]).await?;

        skipped?;
        // The fixture hides the spammer, so a pass would have shown them.
        assert_eq!(scores?, all_absent());
        Ok(())
    }

    /// Switching on records that posts may be hidden before it reads anything,
    /// so a run that fails part way leaves the record for switching off to act
    /// on. Here the ranking can't be read.
    #[tokio_shared_rt::test(shared)]
    async fn a_failed_apply_leaves_the_filtered_record() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(FILTERED_KEY).await?;
        park_ranking().await?;
        let _: () = conn.set(ranking_key(), "not a sorted set").await?;

        let applied = reconcile().await;
        let recorded: bool = conn.exists(FILTERED_KEY).await?;
        let _: () = conn.del(ranking_key()).await?;
        restore_ranking().await?;
        cleanup(&[]).await?;

        assert!(applied.is_err(), "the ranking can't be read");
        assert!(recorded, "the record is set before the failure");
        Ok(())
    }

    /// A ranking that disappears after one was applied, as when a recompute ranks
    /// nobody, puts every author the copy left out back, and drops the copy.
    #[tokio_shared_rt::test(shared)]
    async fn reconcile_shows_everyone_once_the_ranking_is_gone() -> TestResult {
        setup(&[]).await?;
        park_ranking().await?;

        let shown = reconcile().await;
        let scores = spammer_scores().await;
        let applied = is_ranking_applied().await;
        restore_ranking().await?;
        cleanup(&[]).await?;

        shown?;
        assert_eq!(scores?, all_present());
        assert!(!applied?, "the copy is dropped");
        Ok(())
    }

    /// Switching off with the ranking gone shows every author the copy left out.
    #[tokio_shared_rt::test(shared)]
    async fn switching_off_shows_everyone_once_the_ranking_is_gone() -> TestResult {
        setup(&[]).await?;
        park_ranking().await?;

        switch(false).await?;
        let shown = reconcile().await;
        let scores = spammer_scores().await;
        restore_ranking().await?;
        cleanup(&[]).await?;

        shown?;
        assert_eq!(scores?, all_present());
        Ok(())
    }

    /// Switching off also shows an author the live ranking admits but the copy
    /// leaves out: a recompute ranked them, but its reconcile never applied it,
    /// so their posts are still hidden.
    #[tokio_shared_rt::test(shared)]
    async fn switching_off_shows_authors_only_the_live_ranking_admits() -> TestResult {
        setup(&[]).await?;
        rank(SPAMMER).await?;

        switch(false).await?;
        let shown = reconcile().await;
        let scores = spammer_scores().await;
        cleanup(&[]).await?;

        shown?;
        assert_eq!(scores?, all_present());
        Ok(())
    }

    /// Switching off also shows an author a stale copy still ranks but the live
    /// ranking dropped, whose writes the gate has been filtering.
    #[tokio_shared_rt::test(shared)]
    async fn switching_off_shows_authors_only_a_stale_copy_ranks() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(applied_key(), SPAMMER, 0).await?;

        switch(false).await?;
        let shown = reconcile().await;
        let scores = spammer_scores().await;
        cleanup(&[]).await?;

        shown?;
        assert_eq!(scores?, all_present());
        Ok(())
    }

    /// Bootstrap's timeline keeps a user's own posts when the ranking hides
    /// them from everyone else. The author of the newest post on the global
    /// timeline is taken out of the ranking for the test.
    #[tokio_shared_rt::test(shared)]
    async fn bootstrap_keeps_the_users_own_posts() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let newest: Vec<String> = conn.zrevrange(timeline_key(), 0, 0).await?;
        let newest = newest.into_iter().next().ok_or("empty timeline")?;
        let author = newest
            .split_once(':')
            .ok_or("author:post member")?
            .0
            .to_string();
        let rank: Option<f64> = conn.zscore(ranking_key(), &author).await?;
        let rank = rank.ok_or("the fixture ranks the newest post's author")?;

        let _: () = conn.zrem(ranking_key(), &author).await?;
        let hidden = reconcile().await;
        let own = Bootstrap::get_by_id(&author, ViewType::Full).await;
        let other = Bootstrap::get_by_id(D2, ViewType::Full).await;
        let _: () = conn.zadd(ranking_key(), &author, rank).await?;
        let shown = reconcile().await;

        cleanup(&[]).await?;
        hidden?;
        shown?;
        assert!(own?.ids.stream.contains(&newest), "the author's own post");
        assert!(!other?.ids.stream.contains(&newest), "hidden from others");
        Ok(())
    }

    /// An author's entries are written and taken out a page of posts at a time,
    /// every page included: the spammer's two posts in pages of one.
    #[tokio_shared_rt::test(shared)]
    async fn writes_walk_every_page_of_posts() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;

        rank(SPAMMER).await?;
        let shown = write_pages(&mut conn, SPAMMER, Write::Show, 1).await;
        let shown_scores = spammer_scores().await;
        unrank(SPAMMER).await?;
        let hidden = write_pages(&mut conn, SPAMMER, Write::Hide, 1).await;
        let hidden_scores = spammer_scores().await;

        cleanup(&[]).await?;
        shown?;
        hidden?;
        assert_eq!(shown_scores?, all_present());
        assert_eq!(hidden_scores?, all_absent());
        Ok(())
    }

    /// A rank change moves every entry of the author's posts in or out at the
    /// next reconcile, at the scores a full reindex gives them, and the copy
    /// follows the ranking.
    #[tokio_shared_rt::test(shared)]
    async fn reconcile_follows_rank_changes() -> TestResult {
        setup(&[]).await?;
        let before = spammer_scores().await?;

        rank(SPAMMER).await?;
        let shown = reconcile().await;
        let ranked = spammer_scores().await;
        unrank(SPAMMER).await?;
        let hidden = reconcile().await;
        let unranked = spammer_scores().await;

        cleanup(&[]).await?;
        shown?;
        hidden?;
        assert_eq!(before, all_absent(), "the fixture hides the spammer");
        assert_eq!(ranked?, all_present());
        assert_eq!(unranked?, all_absent());
        Ok(())
    }

    /// A ranking can gate writes without ever being applied: its first reconcile
    /// removed some authors' posts, then failed before recording the copy. The
    /// next publish applies it before moving to the new ranking, so an author
    /// the new one admits gets every post back.
    #[tokio_shared_rt::test(shared)]
    async fn publish_finishes_a_ranking_never_applied() -> TestResult {
        setup(&[]).await?;
        let entries = author_entries(D2).await?;
        let before = scores(&entries).await?;

        // A ranking without D2 goes live, and its first reconcile removes D2's
        // posts before failing.
        unrank(D2).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(applied_key()).await?;
        let failed_run = write_pages(&mut conn, D2, Write::Hide, 1_000).await;
        let hidden = scores(&entries).await;

        // The fixture graph ranks D2, so the published ranking admits them.
        let published = SocialGraphStatus::publish().await;
        let after = scores(&entries).await;

        // Puts D2 back whatever happened above. Its rank only moves when the
        // publish failed, and test_social_graph_status publishes before reading.
        let ranked: Option<f64> = conn.zscore(ranking_key(), D2).await?;
        if ranked.is_none() {
            rank(D2).await?;
        }
        write_pages(&mut conn, D2, Write::Show, 1_000).await?;
        cleanup(&[]).await?;

        failed_run?;
        published?;
        assert!(!entries.is_empty(), "the fixture gives D2 shared entries");
        assert!(before.iter().all(Option::is_some), "the fixture shows D2");
        assert_eq!(hidden?, vec![None; entries.len()]);
        assert_eq!(
            after?,
            present(&entries),
            "the publish should give D2 their posts back"
        );
        Ok(())
    }

    /// A reconcile writes posts back at the scores the full reindex gave them,
    /// every term included: tags, replies and reposts, and mentions on label
    /// sets. Not D2: other tests here rewrite D2's entries with the code under
    /// test.
    #[tokio_shared_rt::test(shared)]
    async fn reconcile_scores_posts_as_the_reindex_does() -> TestResult {
        setup(&[]).await?;
        for author in [ENGAGED, MENTIONER] {
            let entries = author_entries(author).await?;
            assert!(!entries.is_empty(), "{author} has shared entries");
            assert_eq!(scores(&entries).await?, present(&entries), "{author}");
        }
        Ok(())
    }

    /// A hide spares an author the live ranking admits, so a reconcile that
    /// lost a race with a newer publish can't take a ranked author's posts out.
    #[tokio_shared_rt::test(shared)]
    async fn hide_spares_an_author_the_live_ranking_admits() -> TestResult {
        setup(&[]).await?;
        let entries = author_entries(D2).await?;
        let mut conn = get_redis_conn().await?;

        let hidden = write_pages(&mut conn, D2, Write::Hide, 1_000).await;
        let after = scores(&entries).await;
        // Puts back whatever a hide that ignored the ranking took out.
        write_pages(&mut conn, D2, Write::Show, 1_000).await?;
        cleanup(&[]).await?;

        hidden?;
        assert!(!entries.is_empty(), "the fixture gives D2 shared entries");
        assert_eq!(after?, present(&entries), "D2 is ranked, so nothing goes");
        Ok(())
    }

    /// The keys of the wot window's short posts, which Cypher serves.
    async fn short_posts_in_wot_window() -> Result<Vec<String>, DynError> {
        let pagination = Pagination {
            start: Some(1650000000014.0),
            end: Some(1650000000001.0),
            limit: Some(50),
            ..Default::default()
        };
        let kind = Some(KindFilter::Kind(PubkyAppPostKind::Short));
        let stream = PostStream::get_post_keys(
            StreamSource::All,
            pagination,
            SortOrder::Descending,
            StreamSorting::Timeline,
            None,
            None,
            kind,
        )
        .await?;
        Ok(stream.map(|stream| stream.post_keys).unwrap_or_default())
    }

    /// Cypher-served `source=all` streams hide unranked authors only while a
    /// ranking is applied and the switch is on. With no ranking, as in
    /// production today, the rule would match nobody and empty them.
    #[tokio_shared_rt::test(shared)]
    async fn cypher_streams_filter_only_while_a_ranking_is_applied() -> TestResult {
        setup(&[]).await?;
        let spammer_post = format!("{SPAMMER}:WOTPOSTS00006");

        let applied = short_posts_in_wot_window().await;
        switch(false).await?;
        let switched_off = short_posts_in_wot_window().await;
        switch(true).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.del(applied_key()).await?;
        let unapplied = short_posts_in_wot_window().await;
        // Its reconcile records the copy again.
        cleanup(&[]).await?;

        assert!(!applied?.contains(&spammer_post), "hidden while applied");
        assert!(
            switched_off?.contains(&spammer_post),
            "shown with the switch off"
        );
        assert!(unapplied?.contains(&spammer_post), "shown without the copy");
        Ok(())
    }

    /// The spammer's hidden reply to D2's post, and D2's own reply to it.
    const SPAMMER_REPLY: &str =
        "qdsygndnk45m9ru5jseg3uxk5xg4usj9hrcraqbzgigapzweaa9o:WOTPOSTMODF01";
    const D2_REPLY: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y:WOTPOSTCYCLE1";

    /// The keys of one page of the replies to D2's tagged post, as `viewer`
    /// gets them.
    async fn d2_thread(
        viewer: Option<&str>,
        order: SortOrder,
        pagination: Pagination,
    ) -> Result<Vec<String>, DynError> {
        let source = StreamSource::PostReplies {
            author_id: D2.to_string(),
            post_id: "WOTPOSTTAGS01".to_string(),
        };
        let sorting = StreamSorting::Timeline;
        let stream =
            PostStream::get_post_keys(source, pagination, order, sorting, viewer, None, None)
                .await?;
        Ok(stream.map(|stream| stream.post_keys).unwrap_or_default())
    }

    /// The whole thread as `viewer` walks it, one reply per page.
    async fn d2_thread_walk(viewer: Option<&str>) -> Result<Vec<String>, DynError> {
        let mut keys = Vec::new();
        for _ in 0..10 {
            let pagination = Pagination {
                skip: Some(keys.len()),
                limit: Some(1),
                ..Default::default()
            };
            let page = d2_thread(viewer, SortOrder::Descending, pagination).await?;
            if page.len() > 1 {
                return Err(format!("a page of {} keys for a limit of 1", page.len()).into());
            }
            if page.is_empty() {
                return Ok(keys);
            }
            keys.extend(page);
        }
        Err("the walk never reached an empty page".into())
    }

    /// A thread hides the unranked spammer's reply from everyone but the post's
    /// author and the spammer, while the post's author's own reply stays for
    /// all. BOB, unranked with a reply elsewhere, gets their own replies merged
    /// in, and none of the hidden ones.
    #[tokio_shared_rt::test(shared)]
    async fn threads_give_hidden_replies_back_to_the_owner_and_replier() -> TestResult {
        let bob_replies = sorted_key(&[&POST_REPLIES_PER_USER_KEY_PARTS[..], &[BOB]].concat());
        setup(&[&bob_replies]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(&bob_replies, "elsewhere", 1.0).await?;
        let both = vec![SPAMMER_REPLY.to_string(), D2_REPLY.to_string()];
        let own_only = vec![D2_REPLY.to_string()];

        let anonymous = d2_thread_walk(None).await;
        let ranked = d2_thread_walk(Some(ENGAGED)).await;
        let bob_merges = may_hide_replies_of(BOB).await;
        let unranked = d2_thread_walk(Some(BOB)).await;
        let owner = d2_thread_walk(Some(D2)).await;
        let replier = d2_thread_walk(Some(SPAMMER)).await;
        cleanup(&[&bob_replies]).await?;

        assert_eq!(anonymous?, own_only, "no viewer");
        assert_eq!(ranked?, own_only, "a ranked viewer");
        assert!(bob_merges?, "BOB replied elsewhere, so their thread merges");
        assert_eq!(
            unranked?, own_only,
            "an unranked replier with no reply there"
        );
        assert_eq!(owner?, both, "the post's author");
        assert_eq!(replier?, both, "the replier");
        Ok(())
    }

    /// A replier sees their own replies while either ranking leaves them out:
    /// the live one ranked them before a reconcile showed their older replies,
    /// or dropped them while the applied copy still has them and the gate turns
    /// their new replies away. A viewer in both rankings gets no merge, nor does
    /// one who never replied.
    #[tokio_shared_rt::test(shared)]
    async fn a_replier_sees_own_replies_while_either_ranking_leaves_them_out() -> TestResult {
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let own_reply = SPAMMER_REPLY.to_string();

        rank(SPAMMER).await?;
        let newly_ranked = d2_thread_walk(Some(SPAMMER)).await;
        unrank(SPAMMER).await?;
        let _: () = conn.zadd(applied_key(), SPAMMER, 0).await?;
        let newly_dropped = d2_thread_walk(Some(SPAMMER)).await;
        let ranked_viewer = may_hide_replies_of(ENGAGED).await;
        let silent_viewer = may_hide_replies_of(BOB).await;
        cleanup(&[]).await?;

        assert!(newly_ranked?.contains(&own_reply), "ranked, not yet shown");
        assert!(
            newly_dropped?.contains(&own_reply),
            "dropped, still in the copy"
        );
        assert!(!ranked_viewer?, "a viewer in both rankings");
        assert!(!silent_viewer?, "an unranked viewer who never replied");
        Ok(())
    }

    /// The post's author pages their thread from the graph as the sorted set
    /// pages: `start` is the highest score kept and `end` the lowest, both
    /// inclusive, whatever the order.
    #[tokio_shared_rt::test(shared)]
    async fn the_owners_thread_pages_like_the_sorted_set() -> TestResult {
        setup(&[]).await?;
        let window = |start, end| Pagination {
            start,
            end,
            limit: Some(10),
            ..Default::default()
        };
        let read = |order, pagination| d2_thread(Some(D2), order, pagination);

        let ascending = read(SortOrder::Ascending, window(None, None)).await?;
        let below = read(SortOrder::Descending, window(Some(1650000000008.0), None)).await?;
        let above = read(SortOrder::Ascending, window(None, Some(1650000000013.0))).await?;

        assert_eq!(ascending, [D2_REPLY, SPAMMER_REPLY]);
        assert_eq!(below, [D2_REPLY]);
        assert_eq!(above, [SPAMMER_REPLY]);
        Ok(())
    }

    /// A ranked viewer's thread is the shared set, paged in Redis: `limit`, the
    /// order, and `start` and `end` as the highest and lowest score kept. A stray
    /// entry gives each window something to cut.
    #[tokio_shared_rt::test(shared)]
    async fn a_ranked_viewers_thread_pages_like_the_sorted_set() -> TestResult {
        let key = replies_key(D2, "WOTPOSTTAGS01");
        let stray = format!("{BOB}:stray");
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(&key, &stray, 1650000000010.0).await?;
        let page = |start, end, limit| Pagination {
            start,
            end,
            limit: Some(limit),
            ..Default::default()
        };
        let read = |order, pagination| d2_thread(Some(ENGAGED), order, pagination);

        let newest = read(SortOrder::Descending, page(None, None, 1)).await;
        let oldest = read(SortOrder::Ascending, page(None, None, 1)).await;
        let below = read(SortOrder::Descending, page(Some(1650000000009.0), None, 10)).await;
        let above = read(SortOrder::Descending, page(None, Some(1650000000009.0), 10)).await;
        let _: () = conn.zrem(&key, &stray).await?;
        cleanup(&[]).await?;

        assert_eq!(newest?, std::slice::from_ref(&stray));
        assert_eq!(oldest?, [D2_REPLY]);
        assert_eq!(below?, [D2_REPLY]);
        assert_eq!(above?, [stray]);
        Ok(())
    }

    /// The post's author reads their thread from the graph only while posts may
    /// be hidden: with the switch off and the sets shown again, it comes from
    /// the shared set, here holding an entry the graph doesn't have.
    #[tokio_shared_rt::test(shared)]
    async fn the_owners_thread_reads_the_graph_only_while_posts_may_be_hidden() -> TestResult {
        let key = replies_key(D2, "WOTPOSTTAGS01");
        let stray = format!("{BOB}:stray");
        setup(&[]).await?;
        let mut conn = get_redis_conn().await?;
        let _: () = conn.zadd(&key, &stray, 1650000000010.0).await?;

        let filtered = d2_thread_walk(Some(D2)).await;
        switch(false).await?;
        let shown = reconcile().await;
        let unfiltered = d2_thread_walk(Some(D2)).await;
        let _: () = conn.zrem(&key, &stray).await?;
        cleanup(&[]).await?;

        shown?;
        assert!(!filtered?.contains(&stray), "read from the graph");
        assert!(unfiltered?.contains(&stray), "read from the shared set");
        Ok(())
    }
}
