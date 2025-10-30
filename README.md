# wpool

A thread pool that limits the number of tasks executing concurrently, without restricting how many tasks can be queued. Submitting tasks is non-blocking, so you can enqueue any number of tasks without waiting.

This library is essentially a port of [`workerpool`](https://github.com/gammazero/workerpool), an amazing Go library.

```rust
// At most 10 workers can run at once.
let max_workers = 10;
let pool = WPool::new(max_workers);

// Submit as many functions as you'd like.
pool.submit(|| {
  // Do some work.
});
pool.submit(|| {
  // Do more work.
});

// Block until all workers are done working and
// the waiting queue has been processed.
pool.stop_wait();
```