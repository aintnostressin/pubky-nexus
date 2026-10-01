use crate::{run_setup, streams_benches::LIMIT_20};
use criterion::Criterion;
use nexus_common::db::kv::SortOrder;
use nexus_common::models::post::{KindFilter, PostStream, StreamSource, TrustFilter};
use nexus_common::types::StreamSorting;
use pubky_app_specs::PubkyAppPostKind;
use tokio::runtime::Runtime;

/// Tagged on fixture posts by ranked authors, so its ranked set is populated.
const TAG: &str = "free";
/// Outside the trust ranking (the wot spammer): served unfiltered after the rank check.
const UNRANKED_VIEWER: &str = "qdsygndnk45m9ru5jseg3uxk5xg4usj9hrcraqbzgigapzweaa9o";

fn anonymous() -> Option<TrustFilter> {
    Some(TrustFilter::default())
}

fn bench_keys(
    label: &str,
    description: &str,
    tags: Option<Vec<String>>,
    kind: Option<KindFilter>,
    trust_filter: Option<TrustFilter>,
    c: &mut Criterion,
) {
    println!("******************************************************************************");
    println!(
        "Benchmarking the post key stream with reach 'All' sorting 'Timeline', {description}."
    );
    println!("******************************************************************************");

    run_setup();

    let rt = Runtime::new().unwrap();

    c.bench_function(label, |b| {
        b.to_async(&rt).iter(|| async {
            let post_key_stream = PostStream::get_post_keys(
                StreamSource::All,
                LIMIT_20,
                SortOrder::Descending,
                StreamSorting::Timeline,
                tags.clone(),
                kind.clone(),
                trust_filter.clone(),
            )
            .await
            .unwrap()
            .expect("expected post keys in benchmark");

            std::hint::black_box((&post_key_stream.post_keys, post_key_stream.last_post_score));
        });
    });
}

/// RANKED (TRUST-FILTERED) STREAM BENCHMARKS. Compare against the unfiltered
/// `stream_all_timeline`, `stream_post_keys_all_timeline` and `stream_tag_timeline`.
pub fn bench_stream_all_timeline_ranked(c: &mut Criterion) {
    println!("******************************************************************************");
    println!(
        "Benchmarking the post stream with reach 'All' sorting 'Timeline', ranked authors only."
    );
    println!("******************************************************************************");

    run_setup();

    let rt = Runtime::new().unwrap();

    c.bench_function("stream_all_timeline_ranked", |b| {
        b.to_async(&rt).iter(|| async {
            let post_stream = PostStream::get_posts(
                StreamSource::All,
                LIMIT_20,
                SortOrder::Descending,
                StreamSorting::Timeline,
                None,
                None,
                None,
                anonymous(),
            )
            .await
            .unwrap();
            std::hint::black_box(post_stream);
        });
    });
}

pub fn bench_stream_post_keys_all_timeline_ranked(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_all_timeline_ranked",
        "ranked authors only",
        None,
        None,
        anonymous(),
        c,
    );
}

pub fn bench_stream_post_keys_all_timeline_unranked_viewer(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_all_timeline_unranked_viewer",
        "unranked viewer (unfiltered)",
        None,
        None,
        Some(TrustFilter {
            viewer_id: Some(UNRANKED_VIEWER.to_string()),
        }),
        c,
    );
}

pub fn bench_stream_post_keys_tag_timeline_ranked(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_tag_timeline_ranked",
        "one tag, ranked authors only",
        Some(vec![TAG.to_string()]),
        None,
        anonymous(),
        c,
    );
}

pub fn bench_stream_post_keys_kind_timeline_ranked(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_kind_timeline_ranked",
        "kind=short (Cypher), ranked authors only",
        None,
        Some(KindFilter::Kind(PubkyAppPostKind::Short)),
        anonymous(),
        c,
    );
}

/// Unfiltered baselines for the keyed tag and kind benches above.
pub fn bench_stream_post_keys_tag_timeline(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_tag_timeline",
        "one tag, unfiltered",
        Some(vec![TAG.to_string()]),
        None,
        None,
        c,
    );
}

pub fn bench_stream_post_keys_kind_timeline(c: &mut Criterion) {
    bench_keys(
        "stream_post_keys_kind_timeline",
        "kind=short (Cypher), unfiltered",
        None,
        Some(KindFilter::Kind(PubkyAppPostKind::Short)),
        None,
        c,
    );
}
