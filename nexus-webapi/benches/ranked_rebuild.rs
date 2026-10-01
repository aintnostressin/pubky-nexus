//! Full rebuild of the ranked timeline sets over the mock data, as the trust
//! job and `reindex::sync` run it after every ranking publish.
use criterion::{criterion_group, criterion_main, Criterion};
use nexus_common::models::post::PostStream;
use setup::run_setup;
use std::time::Duration;
use tokio::runtime::Runtime;

mod setup;

fn bench_ranked_rebuild(c: &mut Criterion) {
    println!("******************************************************************************");
    println!("Benchmarking a full rebuild of the ranked timeline sets over the mock data.");
    println!("******************************************************************************");

    run_setup();

    let rt = Runtime::new().unwrap();

    c.bench_function("ranked_rebuild", |b| {
        b.to_async(&rt).iter(|| async {
            PostStream::rebuild_ranked_sets().await.unwrap();
        });
    });
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::new(10, 0))
        .sample_size(20)
        .warm_up_time(Duration::new(1, 0))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_ranked_rebuild,
}

criterion_main!(benches);
