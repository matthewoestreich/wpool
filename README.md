# wpool

[![Crates.io](https://img.shields.io/crates/v/wpool.svg)](https://crates.io/crates/wpool) [![docs.rs](https://img.shields.io/docsrs/wpool?style=flat-square)](https://docs.rs/wpool/latest/wpool/)

A thread pool that limits the number of tasks executing concurrently, without restricting how many tasks can be queued. Submitting tasks is non-blocking, so you can enqueue any number of tasks without waiting.

This library is essentially a port of [`workerpool`](https://github.com/gammazero/workerpool), an amazing Go library.

## Pool with only worker maximum

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

## Pool with both worker maximum and worker minimum

`min_workers` defines (up to) the minimum number of worker threads that should always stay alive, even when the pool is idle.

**NOTE**: _We do not 'pre-spawn' workers!_ Meaning, if you set `min_workers = 3` but your pool only ever creates 2 workers, then only 2 workers will ever exist (and should always be alive).

```rust
// At most 10 workers can run at once.
let max_workers = 10;
// At minimum up to 3 workers should always exist.
let min_workers = 3;

let pool = WPool::new_with_min(max_workers, min_workers);

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

## Get results out of worker task

```rust
let max_workers = 2;
let wp = WPool::new(max_workers);
let (tx, rx) = mpsc::sync_channel::<u8>(0);

let tx_clone = tx.clone();
wp.submit(move || {
    //
    // Do work here.
    //
    thread::sleep(Duration::from_millis(500));
    //
    let result_from_doing_work = 69;
    //

    if let Err(e) = tx_clone.send(result_from_doing_work) {
        println!("error sending results to main thread from worker! : Error={e:?}");
    }
    println!("success! sent results from worker to main!");
});

// Pause until we get our result.
//
// (Note: pausing is not necessary in this example,
// as our channel can act as a pseudo pauser)
//
// If we were using an unbounded channel, we may want
// to use pause in order to wait
// for the result of any running task (like if we need
// to use the result elsewhere).
//
// wp.pause();

match rx.recv() {
    Ok(result) => assert_eq!(result, 69),
    Err(_) => {
        panic!("expected this not to fail and let us receive results from worker via channel.")
    }
};

wp.stop_wait();
```