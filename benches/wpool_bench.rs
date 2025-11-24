use criterion::{Criterion, criterion_group, criterion_main};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use wpool::WPool;

fn bench_submit_small_tasks(c: &mut Criterion) {
    let max_workers = 8;
    let num_jobs = 5_000;

    // Create thread pools once
    let wpool = Arc::new(WPool::new(max_workers));
    let rayon_pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(max_workers)
            .build()
            .unwrap(),
    );

    let mut group = c.benchmark_group("pool_submit");
    group.sample_size(20);

    // ---- WPool ----
    group.bench_function("WPool submit 5k small tasks", |b| {
        let pool = wpool.clone();
        b.iter(|| {
            for _ in 0..num_jobs {
                pool.submit(|| {
                    let mut x = std::hint::black_box(0u64);
                    for _ in 0..100 {
                        x = x.wrapping_add(1);
                    }
                    std::hint::black_box(x);
                });
            }
            pool.stop_wait();
        })
    });

    // ---- Rayon ----
    group.bench_function("Rayon submit 5k small tasks", |b| {
        let pool = rayon_pool.clone();
        b.iter(|| {
            pool.scope(|s| {
                for _ in 0..num_jobs {
                    s.spawn(|_| {
                        let mut x = std::hint::black_box(0u64);
                        for _ in 0..100 {
                            x = x.wrapping_add(1);
                        }
                        std::hint::black_box(x);
                    });
                }
            });
        })
    });

    group.finish();
}

criterion_group!(benches, bench_submit_small_tasks);
criterion_main!(benches);

