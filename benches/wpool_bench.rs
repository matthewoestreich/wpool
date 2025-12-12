use criterion::{Criterion, criterion_group, criterion_main};
use rayon::ThreadPoolBuilder;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    // time::Duration,
};
use wpool::WPool;

fn bench_submit_small_tasks(c: &mut Criterion) {
    let max_workers = 8;
    let num_jobs = 5_000;

    let mut group = c.benchmark_group("pool_submit");
    //group.measurement_time(Duration::from_millis(15000));
    group.sample_size(50);

    // ---- WPool ----
    group.bench_function(
        format!(
            "WPool [vs Rayon] submit {} small tasks with {} max workers",
            num_jobs, max_workers
        ),
        |b| {
            b.iter(|| {
                let pool = WPool::new(max_workers);
                let counter = Arc::new(AtomicUsize::new(0));

                for _ in 0..num_jobs {
                    let c = counter.clone();
                    pool.submit(move || {
                        let mut x = std::hint::black_box(0u64);
                        for _ in 0..100 {
                            x = x.wrapping_add(1);
                        }
                        c.fetch_add(1, Ordering::Relaxed);
                    });
                }

                pool.stop_wait();
                assert_eq!(counter.load(Ordering::Relaxed), num_jobs);
            });
        },
    );

    // ---- Rayon ----
    group.bench_function(
        format!(
            "Rayon submit {} small tasks with {} max workers",
            num_jobs, max_workers
        ),
        |b| {
            let pool = ThreadPoolBuilder::new()
                .num_threads(max_workers)
                .build()
                .unwrap();

            b.iter(|| {
                let counter = Arc::new(AtomicUsize::new(0));

                pool.scope(|s| {
                    for _ in 0..num_jobs {
                        let c = counter.clone();
                        s.spawn(move |_| {
                            let mut x = std::hint::black_box(0u64);
                            for _ in 0..100 {
                                x = x.wrapping_add(1);
                            }
                            c.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });

                assert_eq!(counter.load(Ordering::Relaxed), num_jobs);
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_submit_small_tasks);
criterion_main!(benches);
