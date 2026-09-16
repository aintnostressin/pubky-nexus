//! Benchmarks `PostStream::sync_ranked_sets`, which keeps the ranked twins of
//! the global post sets in line with the trust ranking.
//!
//! Seeds synthetic root posts, authors and a ranking under the production
//! keys, so it replaces the fixture's global post sets and ranking while it
//! runs; the last target restores them from the graph, as the reindex
//! benchmark does. Size it with `RANKED_SYNC_POSTS` (default 100000) and
//! `RANKED_SYNC_AUTHORS` (default 2000); 85% of the authors are ranked.

use criterion::{criterion_group, criterion_main, Criterion};
use nexus_common::db::{reindex, RedisOps};
use nexus_common::models::post::{
    PostStream, POST_PER_USER_KEY_PARTS, POST_RANKED_TIMELINE_KEY_PARTS,
    POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS, POST_TIMELINE_KEY_PARTS,
    POST_TOTAL_ENGAGEMENT_KEY_PARTS,
};
use nexus_common::models::user::USER_SOCIAL_GRAPH_KEY_PARTS;
use nexus_webapi::mock::MockDb;
use setup::run_setup;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

mod setup;

/// Share of authors in the ranking, and of ranked authors swapped out and in
/// by the incremental case.
const RANKED_SHARE: f64 = 0.85;
const CHURN_SHARE: f64 = 0.01;

/// The two rankings the incremental case alternates between: the seeded one,
/// and one with `CHURN_SHARE` of its authors swapped for unranked ones.
struct Rankings {
    seeded: Vec<String>,
    churned: Vec<String>,
}

static RANKINGS: OnceLock<Rankings> = OnceLock::new();

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Deterministic pseudo-random numbers (xorshift), so every run seeds the
/// same data without a new dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

async fn write_ranking(ranked: &[String]) {
    let elements: Vec<(f64, &str)> = ranked
        .iter()
        .enumerate()
        .map(|(rank, id)| ((rank + 1) as f64, id.as_str()))
        .collect();
    PostStream::replace_index_sorted_set(&USER_SOCIAL_GRAPH_KEY_PARTS, &elements, None, None)
        .await
        .expect("write ranking");
}

/// Replaces the global post sets and the ranking with synthetic data.
async fn seed() -> Rankings {
    let posts = env_usize("RANKED_SYNC_POSTS", 100_000);
    let authors = env_usize("RANKED_SYNC_AUTHORS", 2_000);
    println!("Seeding {posts} root posts by {authors} authors.");

    MockDb::drop_cache().await;
    let author_ids: Vec<String> = (0..authors)
        .map(|i| format!("bench-author-{i:07}"))
        .collect();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);

    const BATCH: usize = 5_000;
    for start in (0..posts).step_by(BATCH) {
        let mut rows: Vec<(String, String, f64, f64)> = Vec::with_capacity(BATCH);
        for i in start..(start + BATCH).min(posts) {
            // Popular authors write more: square the draw to skew it.
            let author = &author_ids[((rng.unit().powi(2)) * authors as f64) as usize];
            let timestamp = 1_700_000_000_000.0 + (i * 3) as f64;
            // Most posts never get engagement, as on a live network.
            let engagement = if rng.unit() < 0.6 {
                0.0
            } else {
                (rng.next() % 40) as f64
            };
            rows.push((author.clone(), format!("P{i:010}"), timestamp, engagement));
        }

        let members: Vec<String> = rows.iter().map(|(a, p, _, _)| format!("{a}:{p}")).collect();
        let timeline: Vec<(f64, &str)> = rows
            .iter()
            .zip(&members)
            .map(|(r, m)| (r.2, m.as_str()))
            .collect();
        let engagement: Vec<(f64, &str)> = rows
            .iter()
            .zip(&members)
            .map(|(r, m)| (r.3, m.as_str()))
            .collect();
        PostStream::put_index_sorted_set(&POST_TIMELINE_KEY_PARTS, &timeline, None, None)
            .await
            .expect("seed timeline");
        PostStream::put_index_sorted_set(&POST_TOTAL_ENGAGEMENT_KEY_PARTS, &engagement, None, None)
            .await
            .expect("seed engagement");
        for (author, post_id, timestamp, _) in &rows {
            let author_posts = [&POST_PER_USER_KEY_PARTS[..], &[author.as_str()]].concat();
            PostStream::put_index_sorted_set(
                &author_posts,
                &[(*timestamp, post_id.as_str())],
                None,
                None,
            )
            .await
            .expect("seed author posts");
        }
    }

    let (seeded, unranked): (Vec<String>, Vec<String>) = author_ids
        .into_iter()
        .partition(|_| rng.unit() < RANKED_SHARE);
    let churn = ((seeded.len() as f64 * CHURN_SHARE).ceil() as usize).min(unranked.len());
    let mut churned: Vec<String> = seeded[churn..].to_vec();
    churned.extend(unranked.into_iter().take(churn));
    println!(
        "Ranking {} authors; the incremental case swaps {churn} out and {churn} in.",
        seeded.len()
    );

    write_ranking(&seeded).await;
    PostStream::sync_ranked_sets().await.expect("initial sync");
    Rankings { seeded, churned }
}

fn bench_ranked_sync(c: &mut Criterion) {
    println!("******************************************************************************");
    println!("Benchmarking the ranked post set sync.");
    println!("******************************************************************************");

    run_setup();
    let rt = Runtime::new().unwrap();
    let rankings = RANKINGS.get_or_init(|| rt.block_on(seed()));

    let mut group = c.benchmark_group("ranked_sync");

    // No sync record for the twins to match: stage both from the global
    // timeline and swap them in.
    group.bench_function("full_build", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                PostStream::unlink_index_sorted_sets(&[
                    &POST_RANKED_TIMELINE_KEY_PARTS,
                    &POST_RANKED_TOTAL_ENGAGEMENT_KEY_PARTS,
                ])
                .await
                .expect("drop twins");
                let started = Instant::now();
                PostStream::sync_ranked_sets().await.expect("sync");
                elapsed += started.elapsed();
            }
            elapsed
        });
    });

    // The ranking moved by CHURN_SHARE out and CHURN_SHARE in since the last
    // sync; iterations alternate between the two rankings.
    group.bench_function("incremental_churn", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;
            for i in 0..iters {
                let ranking = match i % 2 {
                    0 => &rankings.churned,
                    _ => &rankings.seeded,
                };
                write_ranking(ranking).await;
                let started = Instant::now();
                PostStream::sync_ranked_sets().await.expect("sync");
                elapsed += started.elapsed();
            }
            // Leave the seeded ranking in place for the next case.
            write_ranking(&rankings.seeded).await;
            PostStream::sync_ranked_sets().await.expect("sync");
            elapsed
        });
    });

    // Nothing changed since the last sync.
    group.bench_function("unchanged", |b| {
        b.to_async(&rt).iter(|| async {
            PostStream::sync_ranked_sets().await.expect("sync");
        });
    });

    group.finish();
}

/// Not a benchmark: puts the fixture sets and ranking back from the graph so
/// the other benchmarks see the usual data.
fn restore_fixtures(_: &mut Criterion) {
    println!("Restoring the fixture indexes from the graph.");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        MockDb::drop_cache().await;
        reindex::sync().await;
    });
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::new(20, 0))
        .sample_size(10)
        .warm_up_time(Duration::new(1, 0))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_ranked_sync, restore_fixtures,
}

criterion_main!(benches);
