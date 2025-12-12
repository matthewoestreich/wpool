# wpool

[![Crates.io](https://img.shields.io/crates/v/wpool.svg)](https://crates.io/crates/wpool) [![docs.rs](https://img.shields.io/docsrs/wpool?style=flat-square)](https://docs.rs/wpool/latest/wpool/)

A thread pool that limits the number of tasks executing concurrently, without restricting how many tasks can be queued. Submitting tasks is non-blocking, so you can enqueue any number of tasks without waiting.

This library is essentially a port of [`workerpool`](https://github.com/gammazero/workerpool), an amazing Go library.

# Examples

## Worker Maximum

`max_workers` must be greater than 0! If `max_workers == 0` we panic.

```rust
use wpool::WPool;

// At most 10 workers can run at once.
let max_workers = 10;

let pool = WPool::new(max_workers);

// Submit as many functions as you'd like.
pool.submit(|| {
    // Do some work.
    println!("hello from job 1");
});

pool.submit(|| {
    // Do more work.
    println!("hello from job 2");
});

// Block until all workers are done working and
// the waiting queue has been processed.
pool.stop_wait();
```

## Worker Maximum and Minimum

`min_workers` defines (up to) the minimum number of worker threads that should always stay alive, even when the pool is idle. If `min_workers` is greater than `max_workers`, we panic.

We pre-spawn `max_workers` number of workers upon instantiation.

```rust
use wpool::WPool;

// At most 10 workers can run at once.
let max_workers = 10;
// At minimum up to 3 workers should always exist.
let min_workers = 3;

let pool = WPool::new_with_min(max_workers, min_workers);

// Submit as many functions as you'd like.
pool.submit(|| {
    // Do some work.
    println!("doing the thing");
});

pool.submit(|| {
    // Do more work.
    println!("the thing is being done");
});

// Block until all workers are done working and
// the waiting queue has been processed.
pool.stop_wait();
```

## Capturing

```rust
use wpool::WPool;

#[derive(Clone, Debug)]
struct Foo {
    foo: u8,
}

let wp = WPool::new(3);
let my_foo = Foo { foo: 1 };

// You can either clone before capturing:
let my_foo_clone = my_foo.clone();
wp.submit(move || {
    println!("{my_foo_clone:?}");
});

// Or allow the closure to consume (eg. move ownership):
wp.submit(move || {
    println!("{my_foo:?}");
});

wp.stop_wait();
```

## Submit as Fast as Possible

You just want to submit jobs as fast as possible without any blocking. Does not wait for any sort of confirmation.

Does not block under any circumstance by default.

```rust
use wpool::WPool;

let wp = WPool::new(5);

for i in 1..=100 {
    wp.submit(move || {
        println!("job {i}");
    });
}

wp.stop_wait();
```

## Wait for Submission to Execute

```rust
use wpool::WPool;

let wp = WPool::new(2);

// Will block here until job is *executed*.
wp.submit_wait(|| { println!("work"); });
wp.stop_wait();
```

## Wait for Submission to be Submitted

Wait until your submission is either given to a worker or queued. **Does not wait for you submission to be executed, only submitted.**

This can be useful in loops when you need to know that everything has been submitted. Meaning, you now know workers have for sure been spawned.

```rust
use wpool::WPool;
use std::thread;
use std::time::Duration;

let max_workers = 5;
let wp = WPool::new(max_workers);

for i in 1..=max_workers {
    // Will block here until job is *submitted*.
    wp.submit_confirm(|| {
        thread::sleep(Duration::from_secs(2));
    });
    // Now you know that your job has been placed in the queue or given to a worker.
}

assert_eq!(wp.worker_count(), max_workers);
wp.stop_wait();
```

## Get Results from Job

```rust
use wpool::WPool;
use std::thread;
use std::time::Duration;
use std::sync::mpsc;

let max_workers = 2;
let wp = WPool::new(max_workers);
let expected_result = 88;
let (tx, rx) = mpsc::sync_channel::<u8>(0);

// Clone sender and pass into job.
let tx_clone = tx.clone();
wp.submit(move || {
    //
    // Do work here.
    //
    thread::sleep(Duration::from_millis(500));
    //
    let result_from_doing_work = 88;
    //

    if let Err(e) = tx_clone.send(result_from_doing_work) {
        panic!("error sending results to main thread from worker! : Error={e:?}");
    }
    println!("success! sent results from worker to main!");
});

// Pause until we get our result. This is not necessary in this case, as our channel
// is acting as a pseudo pauser. If we were using an unbounded channel, we may want
// to use pause in order to wait for the result of any running task (like if we need
// to use the result elsewhere).
//
// wp.pause();

match rx.recv() {
    Ok(result) => assert_eq!(
        result, expected_result,
        "expected {expected_result} got {result}"
    ),
    Err(e) => panic!("unexpected channel error : {e:?}"),
}

wp.stop_wait();
```

## View Tasks that Panicked

We collect panic info which can be accessed via `<instance>.get_workers_panic_info()`.

```rust
use wpool::WPool;

let wp = WPool::new(3);
wp.submit(|| panic!("something went wrong!"));
// Wait for currently running jobs to finish.
wp.pause();
println!("{:#?}", wp.get_workers_panic_info());
// [
//     PanicInfo {
//         thread_id: ThreadId(
//             9,
//         ),
//         payload: Some(
//             "something went wrong!",
//         ),
//         file: Some(
//             "src/file.rs",
//         ),
//         line: Some(
//             163,
//         ),
//         column: Some(
//             19,
//         ),
//     },
// ]
wp.stop_wait();
```

</br></br>

# Contributing

## Tests

**With `nextest`**

Preferred.

`cargo nextest run --lib`

**With `cargo`**

`cargo test --lib`

