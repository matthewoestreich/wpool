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

# Pacer

## Use with `Pacer`

Read more about `Pacer` [here](https://docs.rs/wpool/latest/wpool/pacer/index.html)

```rust
use wpool::pacer::Pacer;

let at_pace = Duration::from_secs(1);
let pacer = Pacer::new(at_pace);
let counter = Arc::new(AtomicUsize::new(0));

let c = Arc::clone(&counter);
let paced_fn = pacer.pace(move || {
    println!("Hello, world!");
    c.fetch_add(1, Ordering::SeqCst);
});

let max_workers = 5;
let wp = WPool::new(max_workers);

let wp_paced_fn = Arc::clone(&paced_fn);
wp.submit(move || { wp_paced_fn() });
wp.stop_wait();
assert_eq!(counter.load(Ordering::SeqCst), 1);
```

## User `pacer.next()` on non-`PacedFn`'s

You can still pace functions not wrapped in `PacedFn`:

````rust
let delay = Duration::from_millis(50);
let pacer = Pacer::new(delay);
let counter = AtomicUsize::new(0);
let num_calls = 10;

// For example, this is a `PacedFn`:
// `let paced_fn = pacer.pace(|| { ... });``

// Not a `PacedFn`
fn not_a_paced_fn(counter: &AtomicUsize, pacer: &Pacer) {
    pacer.next();
    counter.fetch_add(1, Ordering::SeqCst);
}


// Or if you don't want to pass in a `Pacer`
// instance to your non `PacedFn`:
// ```rust
// fn another_non_paced_fn() {
//     // ...
// }
// for _ in 0..num_calls {
//     pacer.next();
//     another_non_paced_fn();
// }
// ```

let start = Instant::now();

for _ in 0..num_calls {
    not_a_paced_fn(&counter, &pacer);
}

let elapsed = start.elapsed();
let expected_runtime = delay * num_calls;
assert!(
    elapsed >= expected_runtime,
    "pacing failed! expected elapsed ({elapsed:#.2?}) >= expected_runtime ({expected_runtime:#.2?})"
);
assert_eq!(counter.load(Ordering::SeqCst), num_calls as usize);
````
