use wpool::WPool;

fn main() {
    let num_workers = 12;
    let num_jobs = 2_000_000;
    let wp = WPool::new(num_workers);

    for _ in 0..num_jobs {
        wp.submit(|| {
            std::hint::black_box(1);
        });
    }

    wp.stop_wait();
    //assert!(wp.get_workers_panic_info().is_empty());
}
