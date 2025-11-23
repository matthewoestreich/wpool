use criterion::{Criterion, criterion_group, criterion_main};
use wpool::WPool;

fn bench_submit_small_tasks(c: &mut Criterion) {
    let max_workers = 8;
    let num_jobs = 5_000;

    let mut group = c.benchmark_group("pool_submit");
    group.sample_size(20);

    group.bench_function("submit 5k tiny tasks", |b| {
        b.iter(|| {
            let pool = WPool::new(max_workers);
            for _ in 0..num_jobs {
                pool.submit(|| {
                    // "small but non-zero work"
                    let x = std::hint::black_box(123_u64);
                    std::hint::black_box(x + 1);
                });
            }
            pool.stop_wait();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_submit_small_tasks);
criterion_main!(benches);
