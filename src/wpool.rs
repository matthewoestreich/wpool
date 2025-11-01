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
    max_workers: usize,
    paused: Mutex<bool>,
    pauser: Arc<Pauser>,
    stop_once: Once,
    stopped: AtomicBool,
    task_sender: Mutex<Option<mpsc::Sender<Signal>>>,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let task_channel = Channel::new();
        let worker_channel = ThreadedChannel::new();
        let dispatcher = Arc::new(Dispatcher::new(max_workers, worker_channel));

        Self {
            dispatcher: dispatcher.spawn(task_channel.receiver),
            max_workers,
            paused: Mutex::new(false),
            pauser: Pauser::new(),
            stop_once: Once::new(),
            stopped: AtomicBool::new(false),
            task_sender: Some(task_channel.sender).into(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.max_workers
    }

    // Enqueues the given function.
    pub fn submit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.submit_signal(Signal::NewTask(Box::new(f)));
    }

    // Enqueues the given function and waits for it to be executed.
    pub fn submit_wait<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel::<()>();
        self.submit(move || {
            f();
            let _ = sender.send(());
        });
        let _ = receiver.recv(); // blocks until complete
    }

    // Stop and wait for all current work to complete, as well as all signals in the dispatchers waiting_queue.
    pub fn stop_wait(&self) {
        self.shutdown(true);
    }

    // Stop lets all current work finish but disregards signals in the dispatchers waiting_queue.
    pub fn stop(&self) {
        self.shutdown(false);
    }

    // Pauses all workers and blocks until each worker has acknowledged they're paused.
    pub fn pause_wait(&self) {
        self.pause_pool(true);
    }

    // Pauses all workers but does not wait for each worker to acknowledge they're paused.
    pub fn pause(&self) {
        self.pause_pool(false);
    }

    // Resumes/unpauses all workers.
    pub fn resume(&self) {
        let mut is_paused = lock_safe(&self.paused);
        if self.is_stopped() || !*is_paused {
            return;
        }

        for _ in 0..self.max_workers {
            self.pauser.send_resume();
        }

        *is_paused = false;
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn set_stopped(&self, is_stopped: bool) {
        self.stopped.store(is_stopped, Ordering::Relaxed);
    }

    //
    // Pause all possible workers.
    //
    // - If `wait` is true, we block until all workers acknowledge they're paused.
    //
    // - If current worker count is less than max workers, workers will be spawned,
    //   up to 'max_workers' amount, and immediately paused. This ensures every
    //   possible worker that could exist in 'this' pool is paused.
    //
    // - Paused workers are unable to accept new signals, but you can still submit
    //   signals so workers can handle them when they are unpaused.
    //
    // - To unpause, you must explicitly call `.resume()` on your pool instance.
    //
    // - If the pool is stopped while paused, workers are unpaused and queued tasks
    //   are processed during `stop_wait()`.
    //
    // - If the pool is already paused, we ignore this call.
    //
    fn pause_pool(&self, wait: bool) {
        let mut is_paused = lock_safe(&self.paused);
        if self.is_stopped() || *is_paused {
            return;
        }

        let pauser = Arc::clone(&self.pauser);

        for _ in 0..self.max_workers {
            self.submit_signal(Signal::Pause(Arc::clone(&pauser)));
        }

        if wait {
            // Blocks until all workers tell us they're paused.
            for _ in 0..self.max_workers {
                pauser.recv_ack();
            }
        }

        *is_paused = true;
    }

    // "Quality-of-life" function, easier to submit signals directly to the dispatcher.
    fn submit_signal(&self, signal: Signal) {
        if self.is_stopped() {
            return;
        }
        if let Some(task_sender) = lock_safe(&self.task_sender).as_ref() {
            let _ = task_sender.send(signal);
        }
    }

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            // Unpause any paused workers. If we aren't paused, this is essentially a no-op.
            self.resume();

            // Acquire pause lock to wait for any pauses in progress to complete
            let pause_lock = lock_safe(&self.paused);
            self.set_stopped(true);
            drop(pause_lock);

            // Let dispatcher know if it should process it's waiting queue before exiting.
            self.dispatcher.set_is_waiting(wait);

            // Close the task channel.
            if let Some(task_sender) = lock_safe(&self.task_sender).take() {
                drop(task_sender);
            }
        });

        // Wait for the dispatcher thread to end before continuing
        self.dispatcher.join();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicU32, AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant},
    };

    use crate::{lock_safe, worker::WORKER_IDLE_TIMEOUT, wpool::WPool};

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
    fn test_pause_wait_basic() {
        let max_workers = 3;
        let num_jobs = 3;
        let p = WPool::new(max_workers);

        for _ in 0..num_jobs {
            p.submit(|| {});
        }

        p.pause_wait();
        p.resume();
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
    fn test_idle_worker() {
        let max_workers = 3;
        let num_jobs = max_workers + 1;
        let job_sleep_dur = Duration::from_millis(10);
        let counter = Arc::new(AtomicUsize::new(0));
        let p = WPool::new(max_workers);

        for _ in 0..num_jobs {
            let thread_counter = Arc::clone(&counter);
            p.submit(move || {
                thread::sleep(job_sleep_dur);
                thread_counter.fetch_add(1, Ordering::Relaxed);
            });
        }

        // Ensure all workers have passed the timeout
        thread::sleep((WORKER_IDLE_TIMEOUT * (max_workers as u32)) + Duration::from_millis(250));
        p.stop_wait();
        assert_eq!(lock_safe(&p.dispatcher.workers).len(), 0);
    }

    #[test]
    fn test_pause_wait_waits_for_worker_ack() {
        let p = Arc::new(WPool::new(3));
        let acked = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));

        for _ in 0..3 {
            let s = started_tx.clone();
            let r = Arc::clone(&release_rx);
            let a = Arc::clone(&acked);
            p.submit(move || {
                let _ = s.send(());
                // Each worker waits for the same release signal
                let _ = r.lock().unwrap().recv();
                a.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Wait until all tasks have started
        for _ in 0..3 {
            started_rx.recv().expect("worker failed to start");
        }

        // Spawn thread to pause pool (will block until workers acknowledge pause)
        let p_clone = Arc::clone(&p);
        let handle = thread::spawn(move || {
            p_clone.pause_wait();
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!handle.is_finished(), "pause_wait returned too early");

        // Release workers so they can finish and acknowledge pause
        for _ in 0..3 {
            let _ = release_tx.send(());
        }

        // Now pause_wait should complete
        let _ = handle.join();

        // Pool is now paused. Submit tasks that shouldn’t run yet.
        let paused_counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = Arc::clone(&paused_counter);
            p.submit(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            paused_counter.load(Ordering::SeqCst),
            0,
            "Tasks ran while paused"
        );

        p.resume();
        p.stop_wait();
        assert_eq!(paused_counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_pause_wait_jobs_arent_ran_while_paused() {
        fn first() {
            let max_workers = 3;
            let num_jobs = 5;
            let counter = Arc::new(AtomicUsize::new(0));

            let p = WPool::new(max_workers);

            // Batch 1.
            for _ in 0..num_jobs {
                let thread_counter = Arc::clone(&counter);
                p.submit(move || {
                    thread::sleep(Duration::from_millis(500));
                    thread_counter.fetch_add(1, Ordering::Relaxed);
                });
            }

            p.pause_wait();

            // Batch 2.
            for _ in 0..num_jobs {
                let thread_counter = Arc::clone(&counter);
                p.submit(move || {
                    thread::sleep(Duration::from_millis(500));
                    thread_counter.fetch_add(1, Ordering::Relaxed);
                });
            }

            // Even though hwe added 'num_jobs * 2' amount of jobs, only the jobs
            // called prior to p.pause_wait() should have ran.
            // Only "Batch 1" should have ran.
            assert_eq!(counter.load(Ordering::Relaxed), num_jobs);

            p.stop_wait();

            // Now Batch 2 jobs should have ran
            assert_eq!(counter.load(Ordering::Relaxed), num_jobs * 2);
        }

        fn second() {
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
            // Pause the pool from diff thread
            let pause_handle = {
                let _p = Arc::clone(&p);
                thread::spawn(move || {
                    println!("pausing from diff thread");
                    _p.pause_wait();
                })
            };
            // Wait for pause thread to finish
            let _ = pause_handle.join();
            println!("pause thread finished");
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
            println!("resuming pool");
            p.resume();
            p.stop_wait();
            assert_eq!(
                paused_counter.load(Ordering::SeqCst),
                3,
                "Paused tasks did not execute after resume"
            );
        }

        first();
        second();
    }

    #[test]
    fn test_pause_is_nonblocking() {
        let pool = WPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        // Start some ongoing tasks
        for _ in 0..4 {
            let c = counter.clone();
            pool.submit(move || {
                thread::sleep(Duration::from_millis(20));
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        let start = Instant::now();
        pool.pause(); // should not block
        let elapsed = start.elapsed();
        // Should be essentially instantaneous
        assert!(elapsed < Duration::from_millis(5));
        // Give workers a short time to process pause
        thread::sleep(Duration::from_millis(50));
        // No further tasks should be executing now
        let value = counter.load(Ordering::SeqCst);
        thread::sleep(Duration::from_millis(50));
        assert_eq!(counter.load(Ordering::SeqCst), value);
        pool.resume();
        pool.stop_wait();
    }

    #[test]
    fn test_pause_does_not_run_jobs_while_paused() {
        let max_workers = 2;
        let num_jobs = 4;
        let job_sleep_dur = Duration::from_millis(20);
        let pool = WPool::new(max_workers);
        let counter = Arc::new(AtomicU32::new(0));

        // First Batch.
        // Start some ongoing tasks
        for _ in 0..num_jobs {
            let c = counter.clone();
            pool.submit(move || {
                thread::sleep(job_sleep_dur);
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        let start = Instant::now();
        pool.pause(); // should not block
        let elapsed = start.elapsed();
        // Should be essentially instantaneous
        assert!(elapsed < Duration::from_millis(5));
        // Give workers a enough time to process pause
        thread::sleep((num_jobs + 1) * job_sleep_dur);

        // Second Batch.
        // Add more jobs - these should not be executed
        for _ in 0..num_jobs {
            let c = counter.clone();
            pool.submit(move || {
                thread::sleep(job_sleep_dur);
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        // No further tasks should be executing now
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
        pool.resume();
        pool.stop_wait();
        // After resuming and waiting NOW the second batch of jobs should have been run.
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs * 2);
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
    fn test_large_amount_of_workers_and_jobs() {
        let max_workers = 2_000;
        let num_jobs = 2_000_000;
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

    #[test]
    fn test_multiple_stop() {
        let p = WPool::new(3);
        p.submit(|| {});
        p.stop();
        p.stop();
        // No need to assert anything, if this panics the test will fail.
    }
}
