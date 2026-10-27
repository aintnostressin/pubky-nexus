//! `source=starter_pack`: people to follow for a set of interest tags.
//!
//! Fixtures are `wot.cypher`. Of the `bitcoiner` taggers only D1 (0.4), D2 (0.2) and D1B (0.1)
//! carry trust, and nobody tagged them with the label, so each passes on their own trust split
//! over their `bitcoiner` tags. A candidate needs two endorsers besides itself, so `bitcoiner` is
//! exactly:
//!
//! | candidate | endorsers   | trust passed on                     |
//! |-----------|-------------|-------------------------------------|
//! | BTC1      | D1, D1B, D2 | 0.4/3 + 0.1/2 + 0.2/3 = 0.25        |
//! | BTC2      | D1, D2      | 0.4/3 + 0.2/3 = 0.2                 |
//! | BTC3      | D1B, D2     | 0.1/2 + 0.2/3 = 0.117               |
//! | OBSERVER  | D1          | dropped: one endorser               |
//! | BTC5      | SPAMMER     | dropped: one endorser               |
//! | SPAMMER   | BTC5        | dropped: its own tag does not count |

use crate::utils::server::TestServiceServer;
use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::{get_redis_conn, RedisOps};
use nexus_common::models::user::StarterPackRank;
use nexus_webapi::models::ErrorResponsePayload;
use serde_json::Value;

const OBSERVER: &str = "y6apowjmcg8rocmd9jirg95fyf3yykwuhqxozzts4mjipk4n7iao";
const D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
const D2: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y";
const SPAMMER: &str = "qdsygndnk45m9ru5jseg3uxk5xg4usj9hrcraqbzgigapzweaa9o";
const BTC1: &str = "tfqnfppxtr8xei6n3zrfa1b7mmc6gqn41szgutgcccea33pqf3yo";
const BTC2: &str = "uuxjusor98rw4xdo3k3shsgdqwi844si14aaxfcnjxyczjt5eqxy";
const BTC3: &str = "qwzn6jx1gm1ziptn41dxonqy1rpuumwggdq1hu6zc334qep3kjho";
const BTC4: &str = "z5eect18reuccuwuq78da8k5re3y8si346n3bah45gad6t6b1zby";
const BTC5: &str = "wbhcz1gfz14jc4qjg74auyo5bwxd4gc3y84ic18iro17yi4bgz3y";

async fn pack(query: &str) -> Result<Vec<String>> {
    let res = get_request(&format!("/v0/stream/users?source=starter_pack&{query}")).await?;
    let users = res.as_array().expect("starter pack should be an array");
    Ok(users
        .iter()
        .map(|u| {
            u["details"]["id"]
                .as_str()
                .expect("every entry should carry an id")
                .to_string()
        })
        .collect())
}

async fn bad_request(query: &str) -> Result<String> {
    let res = invalid_get_request(
        &format!("/v0/stream/users?{query}"),
        StatusCode::BAD_REQUEST,
    )
    .await?;
    let payload: ErrorResponsePayload =
        serde_json::from_value(res).expect("Response should be a valid ErrorResponsePayload");
    Ok(payload.error)
}

/// Counting endorsers instead of trust would tie BTC2 and BTC3 and put BTC3 first on id.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_ranks_by_propagated_label_reputation() -> Result<()> {
    let ids = pack("tags=bitcoiner&limit=20").await?;

    assert_eq!(
        ids,
        [BTC1, BTC2, BTC3],
        "candidates should rank by the trust their endorsers pass on for the label"
    );

    Ok(())
}

/// BTC5 tagged SPAMMER, so its self-tag is all that would make a second endorser.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_ignores_a_self_tag() -> Result<()> {
    let ids = pack("tags=bitcoiner&limit=20").await?;

    assert!(
        !ids.contains(&SPAMMER.to_string()),
        "a self-tag should not count as an endorsement, got {ids:?}"
    );

    Ok(())
}

/// D1 carries the most trust, so a lone endorsement that counted would rank OBSERVER high.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_needs_more_than_a_lone_endorsement() -> Result<()> {
    let ids = pack("tags=bitcoiner&limit=20").await?;

    for lone in [OBSERVER, BTC5] {
        assert!(
            !ids.contains(&lone.to_string()),
            "{lone} has one endorser and should not be in the pack, got {ids:?}"
        );
    }

    Ok(())
}

/// No profile carries `wotreview`, so only the post arm can find this candidate.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_finds_authors_of_tagged_posts() -> Result<()> {
    let ids = pack("tags=wotreview&limit=20").await?;

    assert_eq!(
        ids,
        [D2],
        "the author of the tagged post should be the pack"
    );

    Ok(())
}

/// A flat ranking would spend both slots on `bitcoiner`, which has six candidates to one.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_gives_a_niche_label_a_slot() -> Result<()> {
    let ids = pack("tags=bitcoiner,btc-dev&limit=2").await?;

    assert_eq!(
        ids,
        [BTC1, BTC4],
        "each label should place its top candidate"
    );

    Ok(())
}

/// These two share their only candidate; disjoint labels could not catch a dedup regression.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_returns_each_user_once() -> Result<()> {
    let ids = pack("tags=btc-dev,packoverlap&limit=20").await?;

    assert_eq!(ids, [BTC4], "a shared candidate should appear once");

    Ok(())
}

/// The candidate is endorsed and has a post, so only the deleted check can remove it.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_excludes_deleted_users() -> Result<()> {
    let ids = pack("tags=packdeleted&limit=20").await?;

    assert!(
        ids.is_empty(),
        "a deleted account should never reach an onboarding pack, got {ids:?}"
    );

    Ok(())
}

/// The default window still asks that a candidate has posted at all.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_excludes_accounts_that_never_posted() -> Result<()> {
    let ids = pack("tags=packsilent&limit=20").await?;

    assert!(
        ids.is_empty(),
        "an account with no posts should not be recommended, got {ids:?}"
    );

    Ok(())
}

/// Pins the fallback that ships until the trust job runs, where every score is null.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_orders_unscored_endorsements_deterministically() -> Result<()> {
    let ids = pack("tags=packunscored&limit=20").await?;

    assert_eq!(
        ids,
        [BTC5, BTC4],
        "candidates endorsed alike with no trust anywhere should fall back to id order"
    );

    Ok(())
}

/// A repeat must not buy a label two turns in the round-robin, which only shows next to another.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_collapses_repeated_labels() -> Result<()> {
    let once = pack("tags=bitcoiner,btc-dev&limit=20").await?;
    let twice = pack("tags=bitcoiner,BITCOINER,btc-dev&limit=20").await?;

    assert_eq!(once, twice, "a repeated label should not change the pack");

    Ok(())
}

/// The cold-start case this endpoint exists for: no viewer, no follows, no history.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_works_with_no_viewer() -> Result<()> {
    let res = get_request("/v0/stream/users?source=starter_pack&tags=bitcoiner&limit=20").await?;
    let users = res.as_array().expect("starter pack should be an array");

    assert!(
        !users.is_empty(),
        "a viewerless pack should still return people"
    );
    for user in users {
        assert!(
            user["details"]["name"].is_string(),
            "Name should be a string"
        );
        assert!(
            user["counts"]["followers"].is_number(),
            "Counts should hydrate"
        );
        assert_eq!(
            user["relationship"]["following"],
            Value::Bool(false),
            "with no viewer the relationship flags default to false"
        );
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_excludes_the_user_and_who_they_follow() -> Result<()> {
    // D1 follows D2, the only `wotreview` candidate.
    let ids = pack(&format!("tags=wotreview&limit=20&user_id={D1}")).await?;
    assert!(
        ids.is_empty(),
        "already-followed users should be dropped, got {ids:?}"
    );

    // viewer_id is how a client identifies itself everywhere else, so it must exclude here.
    let ids = pack(&format!("tags=wotreview&limit=20&viewer_id={D1}")).await?;
    assert!(
        ids.is_empty(),
        "viewer_id alone should exclude followed users, got {ids:?}"
    );

    let ids = pack(&format!("tags=bitcoiner&limit=20&user_id={BTC1}")).await?;
    assert!(
        !ids.contains(&BTC1.to_string()),
        "the user should not be in their own pack"
    );
    assert!(
        ids.contains(&BTC2.to_string()),
        "everyone else should survive"
    );

    Ok(())
}

/// Every fixture post is from 2022, so a recent window empties the pack.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_timeframe_gates_on_activity() -> Result<()> {
    let ungated = pack("tags=bitcoiner&limit=20").await?;
    assert!(
        !ungated.is_empty(),
        "the default window should not gate on recency"
    );

    let gated = pack("tags=bitcoiner&limit=20&timeframe=this_month").await?;
    assert!(
        gated.is_empty(),
        "stale accounts should drop out of a recent window, got {gated:?}"
    );

    Ok(())
}

/// Anyone can put a moderation label on a profile; only the moderator's own tags are deleted.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_rejects_moderation_labels() -> Result<()> {
    let error = bad_request("source=starter_pack&tags=hatespeech").await?;
    assert!(
        error.contains("hatespeech") && error.contains("moderation label"),
        "error should name the rejected label and why, got: {error}"
    );

    let error = bad_request("source=starter_pack&tags=bitcoiner,violence").await?;
    assert!(
        error.contains("violence"),
        "one bad label should fail the request, got: {error}"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_requires_tags() -> Result<()> {
    let error = bad_request("source=starter_pack&limit=20").await?;
    assert!(
        error.contains("tags query param must be provided"),
        "error should say tags is required, got: {error}"
    );

    Ok(())
}

/// Silently ignoring the filter would return a plausible stream answering a different question.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_tags_are_rejected_on_other_sources() -> Result<()> {
    let error = bad_request("source=most_followed&tags=bitcoiner").await?;
    assert!(
        error.contains("only supported for source 'starter_pack'"),
        "error should explain tags is starter_pack only, got: {error}"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_caps_the_label_count() -> Result<()> {
    let error = bad_request("source=starter_pack&tags=a,b,c,d,e,f").await?;
    assert!(
        error.contains("Maximum 5"),
        "error should name the label cap rather than any other rejection, got: {error}"
    );

    Ok(())
}

/// user_id is the subject and viewer_id only shapes relationship flags, as on every other source.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_user_id_outranks_viewer_id() -> Result<()> {
    // D1 follows D2, the only `wotreview` candidate. BTC1 follows nobody.
    let ids = pack(&format!(
        "tags=wotreview&limit=20&user_id={D1}&viewer_id={BTC1}"
    ))
    .await?;
    assert!(
        ids.is_empty(),
        "user_id should drive exclusion, got {ids:?}"
    );

    let ids = pack(&format!(
        "tags=wotreview&limit=20&user_id={BTC1}&viewer_id={D1}"
    ))
    .await?;
    assert_eq!(
        ids,
        [D2],
        "viewer_id should not exclude once user_id is given"
    );

    Ok(())
}

/// A pack is an onboarding list, so paging past 100 is rejected.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_rejects_a_deep_skip() -> Result<()> {
    let error = bad_request("source=starter_pack&tags=bitcoiner&skip=101").await?;
    assert!(
        error.contains("skip must be at most 100"),
        "error should name the skip bound, got: {error}"
    );

    pack("tags=bitcoiner&skip=100&limit=20").await?;

    Ok(())
}

/// Two labels, so the page is a window on the interleaved list rather than on one ranking.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_paginates_over_the_merged_list() -> Result<()> {
    let all = pack("tags=bitcoiner,btc-dev&limit=20").await?;
    let page = pack("tags=bitcoiner,btc-dev&skip=1&limit=2").await?;

    assert_eq!(
        page,
        all[1..3],
        "a page should be a window on the same merged ranking"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_unknown_label_is_an_empty_pack() -> Result<()> {
    let ids = pack("tags=nobodyusesthis&limit=20").await?;

    assert!(
        ids.is_empty(),
        "an unused label should return no one, got {ids:?}"
    );

    Ok(())
}

/// A request reads its labels' rankings from a cache that expires within a day; a tag with the
/// label drops it sooner, which the watcher tests cover.
#[tokio_shared_rt::test(shared)]
async fn test_starter_pack_caches_each_label_for_a_day() -> Result<()> {
    TestServiceServer::get_test_server().await;
    let ids = pack("tags=btc-dev&limit=20").await?;

    let cached = StarterPackRank::try_from_index_json(&["btc-dev"], None)
        .await?
        .expect("the request should have cached the label's ranking");
    let cached_ids: Vec<&str> = cached.candidates.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        cached_ids, ids,
        "the pack should be served from that ranking"
    );

    let key = format!("{}:btc-dev", StarterPackRank::prefix().await);
    let ttl: i64 = get_redis_conn().await?.ttl(&key).await?;
    assert!(
        (1..=24 * 60 * 60).contains(&ttl),
        "the ranking should expire within a day, got a TTL of {ttl}"
    );

    Ok(())
}
