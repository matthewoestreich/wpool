use std::sync::{
    Arc, Mutex, Once,
    atomic::{AtomicBool, Ordering},
    mpsc::{self},
};

use crate::{Signal, dispatcher::Dispatcher, pauser::Pauser, safe_lock};

pub struct WPool {
    pub(crate) dispatcher: Arc<Dispatcher>,
    max_workers: usize,
    paused: Mutex<bool>,
    pauser: Arc<Pauser>,
    stop_once: Once,
    stopped: AtomicBool,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let dispatcher = Arc::new(Dispatcher::new(max_workers));

        Self {
            dispatcher: dispatcher.spawn(),
            max_workers,
            paused: false.into(),
            pauser: Pauser::new(),
            stop_once: Once::new(),
            stopped: false.into(),
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

    //
    // Pause all possible workers.
    //
    // - Blocks until all workers have acknowledged that they're paused.
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
    pub fn pause(&self) {
        // We want to hold the pause lock for the entirety of the pause operation.
        let mut is_paused = safe_lock(&self.paused);

        if self.stopped.load(Ordering::SeqCst) || *is_paused {
            return;
        }

        let pauser = Arc::clone(&self.pauser);

        for _ in 0..self.max_workers {
            self.submit_signal(Signal::Pause(Arc::clone(&pauser)));
        }

        // Blocks until all workers tell us they're paused.
        for _ in 0..self.max_workers {
            pauser.recv_ack();
        }

        *is_paused = true;
    }

    // Resumes/unpauses all workers.
    pub fn resume(&self) {
        // We want to hold the pause lock for the entirety of the resume operation.
        let mut is_paused = safe_lock(&self.paused);

        if self.stopped.load(Ordering::SeqCst) || !*is_paused {
            return;
        }

        for _ in 0..self.max_workers {
            self.pauser.send_resume();
        }

        *is_paused = false;
    }

    // "Quality-of-life" function, easier to submit signals directly to the dispatcher.
    fn submit_signal(&self, signal: Signal) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        self.dispatcher.submit(signal);
    }

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            // Unpause any paused workers. If we aren't paused, this is essentially a no-op.
            self.resume();
            // Acquire pause lock to wait for any pauses in progress to complete
            let pause_lock = safe_lock(&self.paused);
            self.stopped.store(true, Ordering::SeqCst);
            drop(pause_lock);
            // Let dispatcher know if it should process it's waiting queue before exiting.
            self.dispatcher.set_is_waiting(wait);
            // Close the task channel.
            self.dispatcher.close_task_channel();
        });

        // Wait for the dispatcher thread to end before continuing
        self.dispatcher.join();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use crate::{safe_lock, worker::WORKER_IDLE_TIMEOUT, wpool::WPool};

    #[test]
    fn test_basic() {
        let p = WPool::new(3);

        for _ in 0..3 {
            p.submit(|| {
                thread::sleep(Duration::from_millis(100));
            });
        }

        p.stop_wait();
    }

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
                counter_clone.fetch_add(1, Ordering::SeqCst);
                println!("job {i:?} done");
            });
        }
        p.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
    }

    #[test]
    fn test_stop_basic() {
        let max_workers = 2;
        let num_jobs = 20;
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        for i in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                thread::sleep(Duration::from_millis(5));
                counter_clone.fetch_add(1, Ordering::SeqCst);
                println!("job {i:?} done");
            });
        }
        println!("stop called");
        p.stop();
        let ran_jobs = counter.load(Ordering::SeqCst);
        assert!(
            ran_jobs < num_jobs,
            "expected ran jobs to be less than num_jobs : ran jobs = {ran_jobs} | num_jobs = {num_jobs}"
        );
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
    fn test_job_actually_ran() {
        let p = WPool::new(3);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        p.submit(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        p.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
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
                thread_counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Ensure all workers have passed the timeout
        thread::sleep((WORKER_IDLE_TIMEOUT * (max_workers as u32)) + Duration::from_millis(250));
        p.stop_wait();
        assert_eq!(safe_lock(&p.dispatcher.workers).len(), 0);
    }

    #[test]
    fn test_pause_waits_for_worker_ack() {
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
            p_clone.pause();
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
    fn test_pause_jobs_arent_ran_while_paused() {
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
                    thread_counter.fetch_add(1, Ordering::SeqCst);
                });
            }

            p.pause();

            // Batch 2.
            for _ in 0..num_jobs {
                let thread_counter = Arc::clone(&counter);
                p.submit(move || {
                    thread::sleep(Duration::from_millis(500));
                    thread_counter.fetch_add(1, Ordering::SeqCst);
                });
            }

            // Even though hwe added 'num_jobs * 2' amount of jobs, only the jobs
            // called prior to p.pause_wait() should have ran.
            // Only "Batch 1" should have ran.
            assert_eq!(counter.load(Ordering::SeqCst), num_jobs);

            p.resume();
            p.stop_wait();

            // Now Batch 2 jobs should have ran
            assert_eq!(counter.load(Ordering::SeqCst), num_jobs * 2);
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
                    _p.pause();
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
    fn test_shutdown_during_pause() {
        let max_workers = 3;
        let num_jobs = 300;
        let pool = Arc::new(WPool::new(max_workers));
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..num_jobs {
            let c = Arc::clone(&counter);
            pool.submit(move || {
                thread::sleep(Duration::from_millis(50));
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        let pool_clone = Arc::clone(&pool);
        // Pause pool in a separate thread
        let pause_handle = thread::spawn(move || {
            pool_clone.pause();
        });
        // Wait a tiny bit so pause starts but workers may not have acked yet
        thread::sleep(Duration::from_millis(2));
        // Call stop_wait while workers are paused
        pool.stop_wait();
        pause_handle.join().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
    }

    #[test]
    fn test_worker_timeout_during_pause() {
        let pool = Arc::new(WPool::new(2));
        let counter = Arc::new(AtomicUsize::new(0));
        let thread_counter = Arc::clone(&counter);
        // Submit one long task to make sure worker is busy initially
        pool.submit(move || {
            thread::sleep(Duration::from_secs(3));
            thread_counter.fetch_add(1, Ordering::SeqCst);
        });
        let pool_clone = Arc::clone(&pool);
        // Pause pool in a separate thread
        let pause_handle = thread::spawn(move || {
            pool_clone.pause();
        });
        // Wait enough for idle timeout to fire
        thread::sleep(Duration::from_secs(3));
        // Now resume and stop
        pool.resume();
        pool.stop_wait();
        pause_handle.join().unwrap();
        // Test didn't deadlock and tasks completed
        assert!(counter.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn test_overflow() {
        let max_workers = 2;
        let num_jobs = 64;
        let expected_len = 62;
        let (release_sender, release_receiver) = crossbeam_channel::unbounded();
        let p = WPool::new(max_workers);
        // Start workers, and have them all wait on a channel before completing.
        for _ in 0..num_jobs {
            let thread_release_receiver = release_receiver.clone();
            p.submit(move || {
                let _ = thread_release_receiver.recv();
            });
        }
        // Start a thread to free the workers after calling stop.  This way
        // the dispatcher can exit, then when this thread runs, the pool
        // can exit.
        let release_thread_sender = release_sender.clone();
        let release_handle = thread::spawn(move || {
            for _ in 0..num_jobs {
                let _ = release_thread_sender.send(());
            }
        });
        p.stop();
        // Now that the pool has exited, it is safe to inspect its waiting
        // queue without causing a race.
        let wq_len = p.dispatcher.waiting_queue_len();
        assert_eq!(
            wq_len, expected_len,
            "Expected waiting to queue to have len of '{expected_len}' but got '{wq_len}'"
        );
        let _ = release_handle.join();
    }

    #[test]
    fn test_waiting_queue_len_race_100_times() {
        let num_runs = 100;
        let mut failed_iterations: Vec<(usize, String)> = Vec::new();
        let mut max = 0;
        for i in 0..num_runs {
            let result = panic::catch_unwind(waiting_queue_len_race);
            #[allow(clippy::single_match)]
            match result {
                Err(e) => {
                    // The panic payload is a Box<dyn Any + Send>
                    if let Some(&s) = e.downcast_ref::<&str>() {
                        failed_iterations.push((i, s.to_string()));
                    } else {
                        failed_iterations.push((i, "-".to_string()));
                    }
                }
                Ok(thread_max) => {
                    if max < thread_max {
                        max = thread_max;
                    }
                }
            }
        }
        println!("\n\nMAX : {max}\n\n");
        assert!(
            failed_iterations.len() < 50,
            "expected this to pass at least half of the time, instead it failed {}/{num_runs}",
            failed_iterations.len()
        );
    }

    #[test]
    fn test_wq_race() {
        waiting_queue_len_race();
    }

    fn waiting_queue_len_race() -> usize {
        let num_threads = 10;
        let num_jobs = 20;
        let max_workers = 2;
        let mut handles = Vec::<thread::JoinHandle<()>>::new();

        let wp = Arc::new(WPool::new(max_workers));
        let (max_chan_tx, max_chan_rx) = crossbeam_channel::unbounded();

        for thread in 0..num_threads {
            let thread_pool = Arc::clone(&wp);
            let max_chan_tx_clone = max_chan_tx.clone();
            handles.push(thread::spawn(move || {
                let mut max = 0;
                for job in 0..num_jobs {
                    thread_pool.submit(move || {
                        println!("thread:{thread} job:{job} ran");
                        thread::sleep(Duration::from_micros(1));
                    });
                    //thread::sleep(Duration::from_millis(20));
                    let waiting = thread_pool.dispatcher.waiting_queue_len();
                    if waiting > max {
                        max = waiting;
                    }
                }
                let _ = max_chan_tx_clone.send(max);
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }

        let mut final_max = 0;
        for _ in 0..num_threads {
            let t_max = max_chan_rx.recv().unwrap();
            if t_max > final_max {
                final_max = t_max;
            }
        }

        wp.stop();

        println!("max_seen = {final_max}");
        assert!(
            final_max > 0,
            "expected to see waiting queue size > 0 : got {final_max}"
        );
        assert!(
            final_max < num_threads * num_jobs,
            "should not have seen all tasks on waiting queue"
        );
        final_max
    }

    #[test]
    fn test_long_running_job_continues_after_stop_wait() {
        let max_workers = 3;
        let long_running_task_sleep_for = Duration::from_secs(1);
        let default_task_sleep_for = Duration::from_micros(1);
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        fn sleep_for(d: Duration, counter: &Arc<AtomicUsize>) {
            thread::sleep(d);
            counter.fetch_add(1, Ordering::SeqCst);
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
        assert_eq!(counter.load(Ordering::SeqCst), max_workers);
    }

    #[test]
    fn test_large_amount_of_jobs() {
        let cores = {
            match std::thread::available_parallelism() {
                Ok(parallelism) => parallelism.get(),
                Err(_) => 4,
            }
        };
        let max_workers = cores * 2;
        let num_jobs = if cores <= 7 { 10 } else { 1_000_000 };
        let counter = Arc::new(AtomicUsize::new(0));

        let p = WPool::new(max_workers);

        #[allow(unused_variables)]
        for i in 0..num_jobs {
            let counter_clone = counter.clone();
            p.submit(move || {
                //println!("{i}");
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        p.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
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
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        p.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
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
                counter_clone.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(500));
            });
        }
        p.stop_wait();
        assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
    }

    #[test]
    fn test_submit_wait_actually_waits() {
        let max_workers = 3;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let p = WPool::new(max_workers);

        p.submit_wait(move || {
            thread::sleep(Duration::from_millis(500));
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(
            counter.load(Ordering::SeqCst),
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
