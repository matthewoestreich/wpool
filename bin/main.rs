use wpool::WPool;

fn main() {
    let pool = WPool::new(5);
    let num_jobs = 1000;
    let iters_per_job = 100;

    for _ in 0..num_jobs {
        pool.submit(move || {
            let mut x = std::hint::black_box(0u64);
            for _ in 0..iters_per_job {
                x = x.wrapping_add(1);
            }
            x = x.wrapping_add(1);
            std::hint::black_box(x);
        });
    }

    pool.stop_wait();

    println!("done");
}
