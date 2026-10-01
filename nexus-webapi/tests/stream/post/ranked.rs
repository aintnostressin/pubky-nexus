//! `source=all` with `sorting=timeline` hides posts by authors outside the
//! trust ranking, for requests without a viewer and for ranked viewers. A
//! viewer outside the ranking, or one Nexus does not know, gets the unfiltered
//! stream on every shape, Cypher included.
//!
//! Fixture: trust.cypher ranks every user except the wot on-ramp accounts, so
//! inside the wot window (`indexed_at` 1650000000001..=1650000000014, used by no
//! other fixture) only D1, D1B and D2 are ranked. Newest first, the window's root
//! posts are DELETED_USER (014), ARTIST1 (012), BTC5..BTC1 (011..007), SPAMMER
//! (006), D2 (004), D1B (003), D1 (002) and OBSERVER (001): a run of eight hidden
//! posts above the ranked ones and one below.
//!
//! These tests depend on the ranking key existing, which `test_social_graph_status`
//! briefly deletes; `.config/nextest.toml` runs them in one serial group.
use crate::utils::get_request;
use crate::utils::server::TestServiceServer;
use anyhow::Result;
use nexus_common::db::kv::SortOrder;
use nexus_common::models::post::{KindFilter, PostStream, StreamSource};
use nexus_common::types::{Pagination, StreamSorting};
use nexus_common::utils::test_utils::random_pubky_id;
use pubky_app_specs::PubkyAppPostKind;
use serde_json::Value;

use super::utils::ids_in;
use super::{KEYS_ROOT_PATH, ROOT_PATH};

const WOT_D1: &str = "qjftuwjog819ki1wktuy5tndebce36bmxxwtjjm3z1fr97jk9yuo";
const WOT_D1B: &str = "t5ixbtatg4tq5q5ixg16qqrg1bmem75ksg6cweuftuydwzw91pzy";
const WOT_D2: &str = "smf4xrqfhx7stnufkjzhbjyu3rbgb3gga64srqmzcyyoyzefse9y";
const SPAMMER: &str = "qdsygndnk45m9ru5jseg3uxk5xg4usj9hrcraqbzgigapzweaa9o";

const D1_POST: &str = "WOTPOSTD10002";
const D1B_POST: &str = "WOTPOSTD1B003";
const D2_POST: &str = "WOTPOSTD20004";
const SPAMMER_POST: &str = "WOTPOSTS00006";

const WINDOW_START: i64 = 1650000000014;
const WINDOW_END: i64 = 1650000000001;
const WINDOW: &str = "source=all&sorting=timeline&end=1650000000001";

/// The ranked root posts in the window, newest first.
fn ranked_window_keys() -> Vec<String> {
    vec![
        format!("{WOT_D2}:{D2_POST}"),
        format!("{WOT_D1B}:{D1B_POST}"),
        format!("{WOT_D1}:{D1_POST}"),
    ]
}

async fn keys(query: &str) -> Result<Value> {
    Ok(get_request(&format!("{KEYS_ROOT_PATH}?{query}")).await?)
}

async fn posts(query: &str) -> Result<Value> {
    Ok(get_request(&format!("{ROOT_PATH}?{query}")).await?)
}

fn post_keys_in(response: &Value) -> Vec<String> {
    response["post_keys"]
        .as_array()
        .expect("post_keys array")
        .iter()
        .map(|key| key.as_str().unwrap_or_default().to_string())
        .collect()
}

/// A valid pubky id Nexus has never seen.
fn unknown_viewer() -> String {
    random_pubky_id().to_string()
}

/// The wot window, newest first, one page of 50.
fn window_page() -> Pagination {
    Pagination {
        start: Some(WINDOW_START as f64),
        end: Some(WINDOW_END as f64),
        skip: Some(0),
        limit: Some(50),
    }
}

/// An unbounded first page of `limit`.
fn first_page(limit: usize) -> Pagination {
    Pagination {
        start: None,
        end: None,
        skip: Some(0),
        limit: Some(limit),
    }
}

/// The stream exactly as served with the switch off.
async fn unfiltered_keys(
    tags: Option<&[&str]>,
    kind: Option<KindFilter>,
    pagination: Pagination,
) -> Result<Vec<String>> {
    // The model call needs the stack the test server sets up.
    TestServiceServer::get_test_server().await;
    let stream = PostStream::get_post_keys(
        StreamSource::All,
        pagination,
        SortOrder::Descending,
        StreamSorting::Timeline,
        tags.map(|tags| tags.iter().map(ToString::to_string).collect()),
        kind,
        None,
    )
    .await?;
    Ok(stream.map(|stream| stream.post_keys).unwrap_or_default())
}

/// No viewer: the Redis path, its keys route, and the hydrated route all
/// serve only the ranked authors.
#[tokio_shared_rt::test(shared)]
async fn test_all_timeline_hides_unranked_authors() -> Result<()> {
    let page = keys(&format!("{WINDOW}&start={WINDOW_START}&limit=50")).await?;
    assert_eq!(post_keys_in(&page), ranked_window_keys());
    assert_eq!(page["last_post_score"], Value::from(1650000000002_u64));

    let page = posts(&format!("{WINDOW}&start={WINDOW_START}&limit=50")).await?;
    assert_eq!(ids_in(&page), [D2_POST, D1B_POST, D1_POST]);
    Ok(())
}

/// pubky-app pages with `start = last_post_score - 1` and treats a short page
/// as the end of the feed: every page is full until the ranked posts run out,
/// however many hidden posts sit in between.
#[tokio_shared_rt::test(shared)]
async fn test_all_timeline_pages_stay_full() -> Result<()> {
    let mut start = WINDOW_START;
    let mut served = Vec::new();
    for _ in 0..3 {
        let page = keys(&format!("{WINDOW}&start={start}&limit=1")).await?;
        let page_keys = post_keys_in(&page);
        assert_eq!(page_keys.len(), 1, "a full page while ranked posts remain");
        served.extend(page_keys);
        start = page["last_post_score"].as_i64().expect("cursor") - 1;
    }
    assert_eq!(served, ranked_window_keys());

    // Only the hidden observer post is left below the cursor.
    let end = keys(&format!("{WINDOW}&start={start}&limit=1")).await?;
    assert!(post_keys_in(&end).is_empty(), "end of stream: {end}");
    assert!(end["last_post_score"].is_null());

    // A head poll: posts newer than a known head, down to `end`.
    let poll = keys(&format!(
        "source=all&sorting=timeline&start={WINDOW_START}&end=1650000000003&limit=10"
    ))
    .await?;
    assert_eq!(post_keys_in(&poll), ranked_window_keys()[..2].to_vec());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_all_timeline_ascending_and_skip_are_exact() -> Result<()> {
    let page = keys(&format!(
        "{WINDOW}&start={WINDOW_START}&order=ascending&limit=50"
    ))
    .await?;
    let mut ascending = ranked_window_keys();
    ascending.reverse();
    assert_eq!(post_keys_in(&page), ascending);

    let page = keys(&format!("{WINDOW}&start={WINDOW_START}&skip=1&limit=1")).await?;
    assert_eq!(post_keys_in(&page), ranked_window_keys()[1..2].to_vec());

    let page = keys(&format!("{WINDOW}&start={WINDOW_START}&skip=3&limit=1")).await?;
    assert!(post_keys_in(&page).is_empty(), "skip past the ranked posts");
    Ok(())
}

/// A single tag reads the label's ranked set: D2's tagged reply stays (tag
/// streams carry replies), the spammer's tagged reply goes.
#[tokio_shared_rt::test(shared)]
async fn test_single_tag_reads_the_ranked_set() -> Result<()> {
    let page = keys("source=all&sorting=timeline&tags=nudity").await?;
    assert_eq!(post_keys_in(&page), [format!("{WOT_D2}:WOTPOSTREPLY1")]);

    let page = keys("source=all&sorting=timeline&tags=wmtag1").await?;
    assert!(
        post_keys_in(&page).is_empty(),
        "spammer's reply served: {page}"
    );

    let page = posts("source=all&sorting=timeline&tags=wmtag1").await?;
    assert!(ids_in(&page).is_empty(), "spammer's reply served: {page}");
    Ok(())
}

/// `kind`, `exclude_kinds` and several tags go to Cypher, which applies the
/// same membership test as the ranking.
#[tokio_shared_rt::test(shared)]
async fn test_cypher_shapes_hide_unranked_authors() -> Result<()> {
    for filter in ["kind=short", "exclude_kinds=long"] {
        let page = keys(&format!("{WINDOW}&start={WINDOW_START}&{filter}&limit=50")).await?;
        assert_eq!(post_keys_in(&page), ranked_window_keys(), "{filter}");

        let page = posts(&format!("{WINDOW}&start={WINDOW_START}&{filter}&limit=50")).await?;
        assert_eq!(ids_in(&page), [D2_POST, D1B_POST, D1_POST], "{filter}");
    }

    // Every fixture author tagging these is ranked; this pins that the rule
    // composes with the tag MATCH rather than emptying the stream.
    let tags = "tags=bitcoin,opensource&limit=50";
    let page = keys(&format!("source=all&sorting=timeline&{tags}")).await?;
    let unfiltered =
        unfiltered_keys(Some(&["bitcoin", "opensource"]), None, first_page(50)).await?;
    assert!(!unfiltered.is_empty());
    assert_eq!(post_keys_in(&page), unfiltered);
    Ok(())
}

/// A ranked viewer gets the same filtered stream as an anonymous request.
#[tokio_shared_rt::test(shared)]
async fn test_ranked_viewer_gets_the_filtered_stream() -> Result<()> {
    let viewer = format!("viewer_id={WOT_D1}");
    let page = keys(&format!("{WINDOW}&start={WINDOW_START}&limit=50&{viewer}")).await?;
    assert_eq!(post_keys_in(&page), ranked_window_keys());

    let page = keys(&format!(
        "{WINDOW}&start={WINDOW_START}&kind=short&limit=50&{viewer}"
    ))
    .await?;
    assert_eq!(post_keys_in(&page), ranked_window_keys());

    let page = keys(&format!("source=all&sorting=timeline&tags=wmtag1&{viewer}")).await?;
    assert!(post_keys_in(&page).is_empty());
    Ok(())
}

/// A viewer outside the ranking, or one Nexus does not know, gets exactly the
/// unfiltered stream on every shape, the Cypher ones included.
#[tokio_shared_rt::test(shared)]
async fn test_unranked_and_unknown_viewers_get_the_unfiltered_stream() -> Result<()> {
    let untagged = unfiltered_keys(None, None, window_page()).await?;
    let short = unfiltered_keys(
        None,
        Some(KindFilter::Kind(PubkyAppPostKind::Short)),
        window_page(),
    )
    .await?;
    // The route's default page size.
    let tagged = unfiltered_keys(Some(&["wmtag1"]), None, first_page(10)).await?;
    let spammer_post = format!("{SPAMMER}:{SPAMMER_POST}");
    assert!(untagged.contains(&spammer_post), "fixture: {untagged:?}");
    assert!(short.contains(&spammer_post), "fixture: {short:?}");
    assert_eq!(tagged, [format!("{SPAMMER}:WOTPOSTMODF01")]);

    for viewer in [SPAMMER.to_string(), unknown_viewer()] {
        let viewer = format!("viewer_id={viewer}");

        let page = keys(&format!("{WINDOW}&start={WINDOW_START}&limit=50&{viewer}")).await?;
        assert_eq!(post_keys_in(&page), untagged, "{viewer}");

        let page = posts(&format!("{WINDOW}&start={WINDOW_START}&limit=50&{viewer}")).await?;
        let expected: Vec<String> = untagged
            .iter()
            .map(|key| key.split_once(':').expect("author:post").1.to_string())
            .collect();
        assert_eq!(ids_in(&page), expected, "{viewer}");

        let page = keys(&format!(
            "{WINDOW}&start={WINDOW_START}&kind=short&limit=50&{viewer}"
        ))
        .await?;
        assert_eq!(post_keys_in(&page), short, "{viewer}");

        let page = keys(&format!("source=all&sorting=timeline&tags=wmtag1&{viewer}")).await?;
        assert_eq!(post_keys_in(&page), tagged, "{viewer}");
    }
    Ok(())
}

/// Every other source and sorting is untouched: the engagement sort still
/// serves an unranked author's post.
#[tokio_shared_rt::test(shared)]
async fn test_out_of_scope_streams_are_unfiltered() -> Result<()> {
    let page = keys(&format!(
        "source=author&author_id={SPAMMER}&sorting=timeline&limit=50"
    ))
    .await?;
    assert!(
        post_keys_in(&page).contains(&format!("{SPAMMER}:{SPAMMER_POST}")),
        "author stream: {page}"
    );

    // Walk the engagement sort to its end: hidden authors' zero-engagement posts
    // sit at its tail.
    let mut skip = 0;
    let mut found = false;
    loop {
        let page = keys(&format!(
            "source=all&sorting=total_engagement&limit=50&skip={skip}"
        ))
        .await?;
        let page_keys = post_keys_in(&page);
        found |= page_keys.contains(&format!("{SPAMMER}:{SPAMMER_POST}"));
        if page_keys.len() < 50 || found {
            break;
        }
        skip += 50;
    }
    assert!(found, "the engagement sort must keep unranked authors");
    Ok(())
}
