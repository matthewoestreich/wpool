use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use threadpool::ThreadPool;
use wpool::WPool;

fn bench_submit_small_tasks(c: &mut Criterion) {
    let max_workers = 8;
    let num_jobs = 5_000;

    let wpool = Arc::new(WPool::new(max_workers));
    let tpool = Arc::new(ThreadPool::new(max_workers));

    let mut group = c.benchmark_group("pool_submit");
    group.sample_size(20);

    // ---- WPool ----
    group.bench_function(
        format!(
            "WPool [vs threadpool] submit {} small tasks with {} max workers",
            num_jobs, max_workers
        ),
        |b| {
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
        },
    );

    // ---- threadpool crate ----
    group.bench_function(
        format!(
            "ThreadPool submit {} small tasks with {} max workers",
            num_jobs, max_workers
        ),
        |b| {
            let pool = tpool.clone();
            b.iter(|| {
                for _ in 0..num_jobs {
                    pool.execute(|| {
                        let mut x = std::hint::black_box(0u64);
                        for _ in 0..100 {
                            x = x.wrapping_add(1);
                        }
                        std::hint::black_box(x);
                    });
                }
                pool.join(); // waits for all tasks, just like WPool's stop_wait
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_submit_small_tasks);
criterion_main!(benches);
