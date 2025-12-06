use criterion::{Criterion, criterion_group, criterion_main};
use threadpool::ThreadPool;
use wpool::WPool;

fn bench_submit_small_tasks(c: &mut Criterion) {
    let max_workers = 8;
    let num_jobs = 5_000;

    let mut group = c.benchmark_group("pool_submit");
    group.sample_size(20);

    // ---- WPool ----
    group.bench_function(
        format!(
            "WPool submit {} small tasks with {} max workers",
            num_jobs, max_workers
        ),
        |b| {
            b.iter(|| {
                // Create a fresh pool each iteration
                let pool = WPool::new(max_workers);

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
            b.iter(|| {
                // Fresh threadpool every iteration
                let pool = ThreadPool::new(max_workers);

                for _ in 0..num_jobs {
                    pool.execute(|| {
                        let mut x = std::hint::black_box(0u64);
                        for _ in 0..100 {
                            x = x.wrapping_add(1);
                        }
                        std::hint::black_box(x);
                    });
                }

                pool.join(); // waits for all tasks
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_submit_small_tasks);
criterion_main!(benches);
