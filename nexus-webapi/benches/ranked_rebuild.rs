//! Full rebuild of the ranked timeline sets, on synthetic data in a private key
//! namespace, so the fixture's sets are never touched.
//!
//! Sized with `RANKED_REBUILD_POSTS` (default 100,000 root posts),
//! `RANKED_REBUILD_AUTHORS` (default 2,000, every other one ranked) and
//! `RANKED_REBUILD_LABELS` (default 200; each post carries one label). The
//! global set takes the staged path; labels at the default size take the
//! atomic one.
use criterion::{criterion_group, criterion_main, Criterion};
use deadpool_redis::redis::{self, AsyncCommands};
use nexus_common::db::get_redis_conn;
use nexus_common::models::post::ranked::{rebuild, RankedLayout};
use setup::run_setup;
use std::time::Duration;
use tokio::runtime::Runtime;

mod setup;

const NAMESPACE: &str = "Bench:RankedRebuild";

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn clear_namespace() {
    let mut conn = get_redis_conn().await.expect("redis");
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{NAMESPACE}:*"))
            .arg("COUNT")
            .arg(1_000)
            .query_async(&mut conn)
            .await
            .expect("scan");
        if !keys.is_empty() {
            let _: () = conn.unlink(&keys).await.expect("unlink");
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
}

async fn seed(layout: &RankedLayout, posts: usize, authors: usize, labels: usize) {
    let mut conn = get_redis_conn().await.expect("redis");

    let ranked: Vec<(f64, String)> = (0..authors)
        .step_by(2)
        .enumerate()
        .map(|(rank, author)| ((rank + 1) as f64, format!("author{author:06}")))
        .collect();
    for chunk in ranked.chunks(1_000) {
        let _: () = conn
            .zadd_multiple(&layout.trust, chunk)
            .await
            .expect("ranking");
    }

    let members: Vec<(f64, String, usize)> = (0..posts)
        .map(|i| {
            let member = format!("author{:06}:P{i:08}", i % authors);
            (1_700_000_000_000.0 + i as f64, member, i % labels)
        })
        .collect();
    for chunk in members.chunks(1_000) {
        let mut pipe = redis::pipe();
        for (score, member, label) in chunk {
            pipe.zadd(&layout.global.source, member, *score).ignore();
            pipe.zadd(
                &layout.tag(&format!("label{label:04}")).source,
                member,
                *score,
            )
            .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await.expect("seed");
    }
}

fn bench_ranked_rebuild(c: &mut Criterion) {
    let posts = env_usize("RANKED_REBUILD_POSTS", 100_000);
    let authors = env_usize("RANKED_REBUILD_AUTHORS", 2_000).max(1);
    let labels = env_usize("RANKED_REBUILD_LABELS", 200).max(1);

    println!("******************************************************************************");
    println!(
        "Benchmarking a full ranked-set rebuild: {posts} root posts, {authors} authors, {labels} labels."
    );
    println!("******************************************************************************");

    run_setup();

    let rt = Runtime::new().unwrap();
    let layout = RankedLayout::namespaced(NAMESPACE);
    rt.block_on(async {
        clear_namespace().await;
        seed(&layout, posts, authors, labels).await;
    });

    c.bench_function("ranked_rebuild", |b| {
        b.to_async(&rt).iter(|| async {
            let stats = rebuild(&layout).await.expect("rebuild");
            std::hint::black_box(stats);
        });
    });

    rt.block_on(clear_namespace());
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::new(30, 0))
        .sample_size(10)
        .warm_up_time(Duration::new(1, 0))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_ranked_rebuild,
}

criterion_main!(benches);
