use std::sync::{
    Arc, Mutex, Once,
    atomic::{AtomicBool, Ordering},
    mpsc::{self},
};

use crate::{
    channel::{Channel, ThreadedChannel},
    dispatcher::Dispatcher,
    lock_safe,
    pauser::Pauser,
    signal::Signal,
};

pub struct WPool {
    pub(crate) dispatcher: Arc<Dispatcher>,
    is_paused: AtomicBool,
    is_stopped: AtomicBool,
    max_workers: usize,
    pauser: Arc<Pauser>,
    stop_once: Once,
    task_sender: Mutex<Option<mpsc::Sender<Signal>>>,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let task_channel = Channel::new();
        let worker_channel = ThreadedChannel::new();
        let dispatcher = Arc::new(Dispatcher::new(max_workers, worker_channel));

        Self {
            dispatcher: dispatcher.spawn(task_channel.receiver),
            is_paused: AtomicBool::new(false),
            is_stopped: AtomicBool::new(false),
            max_workers,
            pauser: Arc::new(Pauser::new()),
            stop_once: Once::new(),
            task_sender: Some(task_channel.sender).into(),
        }
    }

    // Returns total capacity of pool (eg. max workers)
    pub fn capacity(&self) -> usize {
        self.max_workers
    }

    // Enqueues the given function.
    pub fn submit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.submit_signal(Signal::Job(Box::new(f)));
    }

    // Enqueues the given function and waits for it to be executed.
    pub fn submit_wait<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        self.submit(move || {
            f();
            let _ = done_tx.send(());
        });
        let _ = done_rx.recv(); // blocks until complete
    }

    // Stop and wait for all current work to complete, as well as all signals in the dispatchers waiting_queue.
    pub fn stop_wait(&self) {
        self.shutdown(true);
    }

    // Stop lets all current work finish but disregards signals in the dispatchers waiting_queue.
    pub fn stop(&self) {
        self.shutdown(false);
    }

    // "Quality-of-life" function, easier to submit signals directly to the dispatcher.
    fn submit_signal(&self, signal: Signal) {
        if self.is_stopped.load(Ordering::Relaxed) {
            return;
        }
        if let Some(tx) = lock_safe(&self.task_sender).as_ref() {
            let _ = tx.send(signal);
        }
    }

    // Pause all possible workers.
    // - If current worker count is less than max workers, workers will be spawned,
    //   up to 'max_workers' amount, and immediately paused. This ensures every
    //   possible worker that could exist in 'this' pool is paused.
    // - Paused workers are unable to accept new signals.
    // - To unpause, you must explicitly call `.resume()` on your pool instance.
    pub fn pause(&self) {
        if self.is_stopped.load(Ordering::Relaxed) || self.is_paused.load(Ordering::Relaxed) {
            return;
        }

        let pauser = Arc::clone(&self.pauser);

        for _ in 0..self.max_workers {
            self.submit_signal(Signal::Pause(Arc::clone(&pauser)));
        }
        // Block until all workers are paused, giving them time to finish any current work.
        for _ in 0..self.max_workers {
            pauser.wait_for_ack();
        }

        self.is_paused.store(true, Ordering::Relaxed);
    }

    // Unpauses a paused pool by sending an 'unpause' message through a channel to each worker.
    // You must explicitly call resume on a paused pool to unpause it.
    pub fn resume(&self) {
        if self.is_stopped.load(Ordering::Relaxed) || !self.is_paused.load(Ordering::Relaxed) {
            return;
        }

        for _ in 0..self.max_workers {
            self.pauser.unpause();
        }

        self.is_paused.store(false, Ordering::Relaxed);
    }

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            self.is_stopped.store(true, Ordering::Relaxed);
            self.dispatcher.is_waiting.store(wait, Ordering::Relaxed);

            // Close the task channel by dropping sender.
            if let Some(task_sender) = lock_safe(&self.task_sender).take() {
                drop(task_sender);
            }

            // Block until dispatcher thread has ended.
            self.dispatcher.join();

            let mut workers = lock_safe(&self.dispatcher.workers);

            // Kill all worker threads.
            for _ in 0..workers.len() {
                let _ = self
                    .dispatcher
                    .worker_channel
                    .sender
                    .send(Signal::Terminate);
            }
            // Block until all worker threads have ended.
            for mut w in workers.drain(..) {
                w.join();
            }
        });
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
        time::Duration,
    };

    use crate::wpool::WPool;

    #[test]
    fn test_stop_wait_basic() {
        let max_workers = 3;
        let num_jobs = max_workers * max_workers;
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        for i in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                thread::sleep(Duration::from_millis(10));
                counter_clone.fetch_add(1, Ordering::Relaxed);
                println!("job {i:?} done");
            });
        }
        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), num_jobs);
    }

    #[test]
    fn test_stop_basic() {
        let max_workers = 3;
        let num_jobs = max_workers * max_workers;
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        for i in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                thread::sleep(Duration::from_millis(10));
                counter_clone.fetch_add(1, Ordering::Relaxed);
                println!("job {i:?} done");
            });
        }
        p.stop();
        assert!(counter.load(Ordering::Relaxed) < num_jobs);
    }

    #[test]
    fn test_pause_basic() {
        let max_workers = 3;
        let num_jobs = 3;
        let p = WPool::new(max_workers);

        for _ in 0..num_jobs {
            p.submit(|| {});
        }

        p.pause();
        p.resume();
        p.stop_wait();
    }

    #[test]
    fn test_pause_resume() {
        let p = Arc::new(WPool::new(3));
        let counter = Arc::new(AtomicUsize::new(0));
        // Submit long-running tasks
        for _ in 0..3 {
            let c = Arc::clone(&counter);
            p.submit(move || {
                thread::sleep(Duration::from_millis(500));
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        // Pause the pool
        let pause_handle = {
            let _p = Arc::clone(&p);
            thread::spawn(move || {
                _p.pause();
            })
        };
        // Wait for pause thread to finish
        let _ = pause_handle.join();
        // Submit tasks while paused
        let paused_counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = Arc::clone(&paused_counter);
            p.submit(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        // Wait a short time and check that paused tasks did not run
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            paused_counter.load(Ordering::SeqCst),
            0,
            "Tasks ran while paused!"
        );
        // Resume/unpause the pool
        p.resume();
        // Wait for paused tasks to complete
        thread::sleep(Duration::from_millis(600));
        assert_eq!(
            paused_counter.load(Ordering::SeqCst),
            3,
            "Paused tasks did not execute after resume"
        );
        p.stop_wait();
    }

    #[test]
    fn test_job_actually_ran() {
        let p = WPool::new(3);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        p.submit(move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_long_running_job_continues_after_stop_wait() {
        let max_workers = 3;
        let long_running_task_sleep_for = Duration::from_secs(3);
        let default_task_sleep_for = Duration::from_micros(1);
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        fn sleep_for(d: Duration, counter: &Arc<AtomicUsize>) {
            thread::sleep(d);
            counter.fetch_add(1, Ordering::Relaxed);
        }

        for i in 0..max_workers {
            let counter_clone = Arc::clone(&counter);
            p.submit(move || {
                sleep_for(
                    if i == 0 {
                        long_running_task_sleep_for
                    } else {
                        default_task_sleep_for
                    },
                    &counter_clone,
                );
            });
        }

        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), max_workers);
    }

    #[test]
    fn test_large_amount_of_jobs() {
        let max_workers = 16;
        let num_jobs = 1_000_000;
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        #[allow(unused_variables)]
        for i in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                //println!("{i}");
                counter_clone.fetch_add(1, Ordering::Relaxed);
            });
        }

        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), num_jobs);
    }

    #[test]
    fn test_more_jobs_than_max_workers() {
        let max_workers = 3;
        let num_jobs = max_workers * max_workers;
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        for _ in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(500));
            });
        }
        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), num_jobs);
    }

    #[test]
    fn test_submit_wait_actually_waits() {
        let max_workers = 3;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let p = WPool::new(max_workers);

        p.submit_wait(move || {
            thread::sleep(Duration::from_millis(500));
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "Did not wait for submit_wait job to complete"
        );
    }

    #[test]
    fn test_max_concurrent_workers() {
        let max_workers = 3;
        let num_jobs = max_workers * max_workers;
        let concurrent_count = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        for _ in 0..num_jobs {
            let concurrent_count = concurrent_count.clone();
            let max_seen = max_seen.clone();
            p.submit(move || {
                let current = concurrent_count.fetch_add(1, Ordering::SeqCst) + 1;
                // record peak concurrency
                loop {
                    let peak = max_seen.load(Ordering::SeqCst);
                    if current > peak {
                        max_seen
                            .compare_exchange(peak, current, Ordering::SeqCst, Ordering::SeqCst)
                            .ok();
                    } else {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(200));
                concurrent_count.fetch_sub(1, Ordering::SeqCst);
            });
        }

        p.stop_wait();
        assert!(
            max_seen.load(Ordering::SeqCst) <= max_workers,
            "Pool was running more than 'max_workers' number of workers. Concurrency was not limited"
        );
    }

    #[test]
    fn test_multiple_stop_wait() {
        let p = WPool::new(3);
        p.submit(|| {});
        p.stop_wait();
        p.stop_wait();
        // No need to assert anything, if this panics the test will fail.
    }
}
