#![allow(dead_code)]
//! Pacer provides a utility to limit the rate at which concurrent
//! threads begin execution. This addresses situations where running the
//! concurrent threads is OK, as long as their execution does not start at the
//! same time.
//!
//! # Important!
//!
//! Calling `stop()` **does NOT wait for executing functions to finish**! Any
//! function(s) that are executing when `stop()` is called will be abandoned
//! if the main thread ends prior to the function(s) completion.
//!
//! # Examples
//!
//! ## With `WPool`
//!
//! ```rust,ignore
//! use wpool::{WPool, pacer::Pacer};
//! use std::time::Duration;
//! use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
//!
//! let at_pace = Duration::from_secs(1);
//! let pacer = Pacer::new(at_pace);
//! let counter = Arc::new(AtomicUsize::new(0));
//!
//! let c = Arc::clone(&counter);
//! let paced_fn = pacer.pace(move || {
//!     println!("Hello, world!");
//!     c.fetch_add(1, Ordering::SeqCst);
//! });
//!
//! let max_workers = 5;
//! let wp = WPool::new(max_workers);
//!
//! let wp_paced_fn = Arc::clone(&paced_fn);
//! wp.submit(move || { wp_paced_fn() });
//! wp.stop_wait();
//! assert_eq!(counter.load(Ordering::SeqCst), 1);
//! ```
//!
//! ## Without `WPool`
//!
//! ```rust,ignore
//! use wpool::{WPool, pacer::Pacer};
//! use std::thread;
//! use std::time::Duration;
//! use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
//!
//! let at_pace = Duration::from_secs(1);
//! let pacer = Pacer::new(at_pace);
//! let counter = Arc::new(AtomicUsize::new(0));
//!
//! let c = Arc::clone(&counter);
//! let paced_fn = pacer.pace(move || {
//!     println!("Hello, world!");
//!     c.fetch_add(1, Ordering::SeqCst);
//! });
//!
//! let thread_paced_fn = Arc::clone(&paced_fn);
//! let handle = thread::spawn(move || { thread_paced_fn() });
//! let _ = handle.join();
//! assert_eq!(counter.load(Ordering::SeqCst), 1);
//! ```
//!
//! ## Use `next` to pace regular functions
//!
//! By "regular functions", we mean functions not wrapped in `PacedFn`.
//!
//! ```rust,ignore
//! use wpool::{WPool, pacer::Pacer};
//! use std::time::{Duration, Instant};
//! use std::sync::atomic::{Ordering, AtomicUsize};
//!
//! let delay = Duration::from_millis(50);
//! let pacer = Pacer::new(delay);
//! let counter = AtomicUsize::new(0);
//! let num_calls = 10;
//!
//! // For example, this is a `PacedFn`
//! // `let paced_fn = pacer.pace(|| { ... });``
//!
//! // Not a `PacedFn`
//! fn not_a_paced_fn(counter: &AtomicUsize, pacer: &Pacer) {
//!     pacer.next();
//!     counter.fetch_add(1, Ordering::SeqCst);
//! }
//!
//! // Or if you don't want to pass in a `Pacer`
//! // instance to your non `PacedFn`:
//! // ```rust
//! // fn another_non_paced_fn() {
//! //     // ...
//! // }
//! // for _ in 0..num_calls {
//! //     pacer.next();
//! //     another_non_paced_fn();
//! // }
//! // ```
//!
//! let start = Instant::now();
//!
//! for _ in 0..num_calls {
//!     not_a_paced_fn(&counter, &pacer);
//! }
//!
//! pacer.stop();
//!
//! let elapsed = start.elapsed();
//! let expected_runtime = delay * num_calls;
//! assert!(
//!     elapsed >= expected_runtime,
//!     "pacing failed! expected elapsed ({elapsed:#.2?}) >= expected_runtime ({expected_runtime:#.2?})"
//! );
//! assert_eq!(counter.load(Ordering::SeqCst), num_calls as usize);
//! ```
use std::{
    panic::RefUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crossbeam_channel::Receiver;

use crate::{Channel, safe_lock};

/// Type alias for a thread-safe Fn().
pub type PacedFn = Arc<dyn Fn() + Send + Sync + RefUnwindSafe + 'static>;

pub struct Pacer {
    delay: Duration,
    gate: Channel<()>,   // unbounded
    pause: Channel<()>,  // bounded
    paused: Channel<()>, // bounded
    is_paused: AtomicBool,
    run_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Pacer {
    pub fn new(delay: Duration) -> Self {
        let gate = Channel::new_unbounded();
        let pause = Channel::new_bounded(1);
        let paused = Channel::<()>::new_bounded(1);
        let run_handle = Some(Self::run(gate.receiver.clone(), pause.clone(), delay)).into();

        Self {
            delay,
            gate,
            pause,
            paused,
            is_paused: AtomicBool::new(false),
            run_handle,
        }
    }

    /// Wraps a function in a paced function. The returned paced function can
    /// then be submitted to a `WPool`, using `submit` or `submit_wait`, and
    /// starting the tasks is paced according to the pacer's delay.
    pub fn pace<F>(&self, f: F) -> PacedFn
    where
        F: Fn() + Send + Sync + RefUnwindSafe + 'static,
    {
        // Mimic self.next() function.
        let sender = self.gate.sender.clone();
        Arc::new(move || {
            let _ = sender.send(());
            f();
        })
    }

    /// Next submits a run request to the gate and returns when it is time to run.
    /// This allows you to manually pace functions non wrapped in `PacedFn`.
    pub fn next(&self) {
        // Wait for item to be read from gate.
        let _ = self.gate.sender.send(());
    }

    /// Stops the Pacer from running. Calling `stop()` **does NOT wait for executing
    /// functions to finish**! Any function(s) that are executing when `stop()` is
    /// called will be abandoned if the main thread ends prior to the function(s) completion.
    /// A `Pacer` instance is not reusable, so consider any given pacer instance to be
    /// abandoned after `stop()` has been called on it.
    pub fn stop(&mut self) {
        let old = std::mem::replace(&mut self.gate, Channel::new_unbounded());
        drop(old);
        if let Some(handle) = safe_lock(&self.run_handle).take() {
            let _ = handle.join();
        }
    }

    /// Reports whether or not Pacer is suspended.
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::SeqCst)
    }

    /// Resume continues execution after Pause.
    pub fn resume(&self) {
        let _ = self.paused.receiver.recv(); // Clear flag to indicate paused.
        let _ = self.pause.receiver.recv(); // Unblock this channel.
        self.is_paused.store(false, Ordering::SeqCst);
    }

    /// Suspends execution of any tasks by the pacer.
    pub fn pause(&self) {
        let _ = self.pause.sender.send(()); // Block this channel.
        let _ = self.paused.sender.send(()); // Set the flag to indicate paused.
        self.is_paused.store(true, Ordering::SeqCst);
    }

    /********************** Private Methods ************************/

    /// Spawns a thread that iterates over the gate and handles pacing tasks.
    fn run(gate: Receiver<()>, pause: Channel<()>, delay: Duration) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            // Read item from gate no faster than one per delay. Reading from the
            // unbounded channel serves as a "tick" and unblocks the sender.
            while gate.recv().is_ok() {
                thread::sleep(delay);
                let _ = pause.sender.send(()); // Wait here if channel is blocked.
                let _ = pause.receiver.recv(); // Clear channel.
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use crate::{WPool, WaitGroup, pacer::Pacer};

    #[test]
    #[ignore]
    fn test_pacer_works() {
        let delay_1 = Duration::from_millis(100);
        let delay_2 = Duration::from_millis(300);

        let wp = WPool::new(5);

        let pacer = Pacer::new(delay_1);
        let pacer_slow = Pacer::new(delay_2);

        let tasks_done = WaitGroup::new_with_delta(20);

        let start = Instant::now();

        let tasks_done_pacer = tasks_done.clone();
        let paced_task = pacer.pace(move || {
            tasks_done_pacer.done();
        });

        let tasks_done_pacer_slow = tasks_done.clone();
        let paced_task_slow = pacer_slow.pace(move || {
            tasks_done_pacer_slow.done();
        });

        // Cause worker to be created, and available for reuse before next task.
        for _ in 0..10 {
            let paced_task_clone = Arc::clone(&paced_task);
            let paced_task_slow_clone = Arc::clone(&paced_task_slow);
            wp.submit(move || paced_task_clone());
            wp.submit(move || paced_task_slow_clone());
        }

        thread::sleep(Duration::from_millis(500));

        pacer.pause();

        thread::sleep(Duration::from_secs(1));

        // Ensure we are paused.
        assert!(pacer.is_paused(), "expected to be paused");

        pacer.resume();

        // Ensure we are unpaused
        assert!(!pacer.is_paused(), "did not expect to be paused");

        tasks_done.wait();

        let elapsed = start.elapsed();
        // 9 times delay_2 since no wait for first task, and pacer and pacer_slow run
        // currently so only limiter is pacer_slow.
        let expected_minimum_elapsed = 9 * delay_2;

        assert!(
            elapsed < expected_minimum_elapsed,
            "Did not pace tasks correctly, finished too soon! Expected elapsed = {expected_minimum_elapsed:?} | actual elapsed = {elapsed:?}"
        );

        wp.stop_wait();
    }

    #[test]
    #[should_panic]
    #[ignore]
    fn test_stop_before_fns_finish_panics() {
        let mut pacer = Pacer::new(Duration::from_millis(100));
        let counter = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&counter);
        let paced_fn = pacer.pace(move || {
            thread::sleep(Duration::from_secs(2));
            c.fetch_add(1, Ordering::SeqCst);
        });

        let handle = thread::spawn(move || {
            paced_fn();
        });

        pacer.stop();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let _ = handle.join(); // Cleanup after assert so test is not marked as leaky.
    }

    #[test]
    #[ignore]
    fn test_stop_before_fns_finish_does_not_panic_while_main_thread_alive() {
        let mut pacer = Pacer::new(Duration::from_millis(100));
        let counter = Arc::new(AtomicUsize::new(0));
        let paced_fn_sleep_dur = Duration::from_millis(1000);
        let keep_main_thread_alive_sleep_for = Duration::from_millis(1500);

        let c = Arc::clone(&counter);
        let paced_fn = pacer.pace(move || {
            thread::sleep(paced_fn_sleep_dur);
            c.fetch_add(1, Ordering::SeqCst);
        });

        let handle = thread::spawn(move || {
            paced_fn();
        });

        pacer.stop();
        thread::sleep(keep_main_thread_alive_sleep_for);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let _ = handle.join(); // Cleanup after assert so test is not marked as leaky.
    }

    #[test]
    #[ignore]
    fn test_next_paces_non_paced_fns() {
        let num_calls = 10;
        let delay = Duration::from_millis(50);
        let mut pacer = Pacer::new(delay);

        let counter = AtomicUsize::new(0);

        // Not a `PacedFn`
        fn not_a_paced_fn(counter: &AtomicUsize, pacer: &Pacer) {
            pacer.next();
            counter.fetch_add(1, Ordering::SeqCst);
        }

        // For example, this is a `PacedFn`
        // `let paced_fn = pacer.pace(|| { ... });``

        let start = Instant::now();

        for _ in 0..num_calls {
            not_a_paced_fn(&counter, &pacer);
        }

        // Or if you don't want to pass in a `Pacer` instance
        // to your non `PacedFn`:
        // ```rust
        // fn another_non_paced_fn() {
        //     // ...
        // }
        // for _ in 0..num_calls {
        //     pacer.next();
        //     another_non_paced_fn();
        // }
        // ```

        pacer.stop();

        let elapsed = start.elapsed();
        let expected_runtime = delay * num_calls;
        assert!(
            elapsed >= expected_runtime,
            "pacing failed! expected elapsed ({elapsed:#.2?}) >= expected_runtime ({expected_runtime:#.2?})"
        );
        assert_eq!(counter.load(Ordering::SeqCst), num_calls as usize);
    }

    #[test]
    #[ignore]
    fn test_paced_task_respects_delay() {
        let num_calls = 10;
        let delay = Duration::from_millis(50);
        let mut pacer = Pacer::new(delay);

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let paced_fn = pacer.pace(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let start = Instant::now();

        for _ in 0..num_calls {
            paced_fn();
        }

        pacer.stop();

        let elapsed = start.elapsed();
        let expected_runtime = delay * num_calls;
        assert!(
            elapsed >= expected_runtime,
            "pacing failed! expected elapsed ({elapsed:#.2?}) >= expected_runtime ({expected_runtime:#.2?})"
        );
        assert_eq!(counter.load(Ordering::SeqCst), num_calls as usize);
    }

    #[test]
    #[ignore]
    fn test_without_wpool() {
        let at_pace = Duration::from_secs(1);
        let mut pacer = Pacer::new(at_pace);
        let counter = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&counter);
        let paced_fn = pacer.pace(move || {
            println!("Hello, world!");
            c.fetch_add(1, Ordering::SeqCst);
        });

        let thread_paced_fn = Arc::clone(&paced_fn);
        let handle = thread::spawn(move || thread_paced_fn());
        let _ = handle.join();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        pacer.stop();
    }

    #[test]
    #[ignore]
    fn test_with_wpool() {
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
        wp.submit(move || wp_paced_fn());
        wp.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
