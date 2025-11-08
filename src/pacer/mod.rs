#![allow(dead_code)]
//!
//! Pacer provides a utility to limit the rate at which concurrent
//! threads begin execution. This addresses situations where running the
//! concurrent threads is OK, as long as their execution does not start at the
//! same time.
//!
//! # Examples
//!
//! ## With `WPool`
//!
//! ```rust
//! use wpool::pacer::Pacer;
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
//! ```rust
//! use wpool::{WPool, pacer::Pacer};
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
//! # Important
//!
//! **NOTE: Do not call pacer.stop() until all paced tasks have completed!!!
//! Otherwise, paced tasks will hang waiting for pacer to unblock them.**
//!
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crate::{
    channel::{Channel, Receiver, bounded, unbounded},
    safe_lock,
};

/// PacerFn is a type alias for a thread-safe Fn().
pub type PacerFn = Arc<dyn Fn() + Send + Sync + 'static>;

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
        let gate = unbounded();
        let pause = bounded(1);
        let paused = bounded::<()>(1);
        let run_handle = Some(Self::run(gate.clone_receiver(), pause.clone(), delay)).into();

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
    pub fn pace<F>(&self, f: F) -> PacerFn
    where
        F: Fn() + Send + Sync + 'static,
    {
        // Mimic self.next() function.
        let sender = self.gate.clone_sender();
        Arc::new(move || {
            let _ = sender.send(());
            f();
        })
    }

    /// Next submits a run request to the gate and returns when it is time to run.
    pub fn next(&self) {
        // Wait for item to be read from gate.
        let _ = self.gate.send(());
    }

    /// Stops the Pacer from running. Do not call until all paced tasks have
    /// completed, or paced tasks will hang waiting for pacer to unblock them.
    pub fn stop(&self) {
        self.gate.drop_sender();
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
        let _ = self.paused.recv(); // Clear flag to indicate paused.
        let _ = self.pause.recv(); // Unblock this channel.
        self.is_paused.store(false, Ordering::SeqCst);
    }

    /// Suspends execution of any tasks by the pacer.
    pub fn pause(&self) {
        let _ = self.pause.send(()); // Block this channel.
        let _ = self.paused.send(()); // Set the flag to indicate paused.
        self.is_paused.store(true, Ordering::SeqCst);
    }

    /********************** Private Methods ************************/

    /// Spawns a thread that iterates over the gate and handles pacing tasks.
    fn run(gate: Receiver<()>, pause: Channel<()>, delay: Duration) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            // Read item from gate no faster than one per delay. Reading from the
            // unbounded channel serves as a "tick" and unblocks the sender.
            for _ in gate.recv_iter() {
                thread::sleep(delay);
                let _ = pause.send(()); // Wait here if channel is blocked.
                let _ = pause.recv(); //  Clear channel.
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
        time::{self, Duration},
    };

    use crate::{WPool, WaitGroup, pacer::Pacer};

    #[test]
    fn test_pacer_works() {
        let delay_1 = Duration::from_millis(100);
        let delay_2 = Duration::from_millis(300);

        let wp = WPool::new(5);

        let pacer = Pacer::new(delay_1);
        let pacer_slow = Pacer::new(delay_2);

        let tasks_done = WaitGroup::new();
        tasks_done.add(20);

        let start = time::Instant::now();

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
    }

    #[test]
    fn test_without_wpool() {
        let at_pace = Duration::from_secs(1);
        let pacer = Pacer::new(at_pace);
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
    }

    #[test]
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
