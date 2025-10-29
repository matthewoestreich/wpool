# wpool

Concurrency-limiting worker pool.

```rust
// Create pool
let max_workers = 10;
let pool = WPool::new(max_workers);

// Submit as many "jobs" as you'd like
pool.submit(|| {
  // Do some work
});
pool.submit(|| {
  // Do more work
});

// Block until all workers are done working
pool.stop_wait();
```