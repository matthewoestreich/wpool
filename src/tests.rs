#![allow(dead_code)]
use std::{
    panic::{self, RefUnwindSafe, UnwindSafe},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError, TryRecvError},
    },
    thread,
    time::Duration,
};

use crate::{
    ThreadedDeque,
    channel::{bounded, unbounded},
    safe_lock,
    wait_group::WaitGroup,
    wpool::{WORKER_IDLE_TIMEOUT, WPool},
};

// Runs a test `n_times` in a row.
// failure_threshold : If this many runs fail this test willl fail. If 'failure_threshold' = 2, if 3 jobs fail, this job fails.
fn run_test_n_times<F>(n_times: usize, failure_threshold: usize, log_job_info: bool, test_fn: F)
where
    F: FnOnce() + Send + Sync + Clone + Copy + UnwindSafe + RefUnwindSafe + 'static,
{
    let mut failed_iterations: Vec<(usize, String)> = Vec::new();
    let thread_safe_test_fn = Arc::new(test_fn);

    for i in 0..n_times {
        if log_job_info {
            println!("\n--------------------- JOB {i} ---------------------");
        }

        let thread_fn = Arc::clone(&thread_safe_test_fn);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_sender = release_tx.clone();

        let handle = thread::spawn(move || {
            let result = panic::catch_unwind(|| {
                thread_fn();
            });
            let _ = release_sender.send(result);
        });

        let result = release_rx.recv().unwrap();
        let _ = handle.join();

        if let Err(e) = result {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "-".to_string()
            };
            failed_iterations.push((i, msg));
        }
    }

    if log_job_info && !failed_iterations.is_empty() {
        println!(
            "\n\n------------------------------------------ Failed Iterations ------------------------------------------\n\n{:#?}\n\n-------------------------------------------------------------------------------------------------------",
            failed_iterations
        );
    }

    assert!(
        if failure_threshold == 0 {
            failed_iterations.is_empty()
        } else {
            failed_iterations.len() <= failure_threshold
        },
        "expected this to fail at most {failure_threshold} times, instead it failed {}/{}",
        failed_iterations.len(),
        n_times
    );
}

// Helper function...runs a closure and fails after duration.
fn run_test_with_timeout<F>(timeout: Duration, test_fn: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        test_fn();
        tx.send(()).ok();
    });

    if rx.recv_timeout(timeout).is_err() {
        panic!("test timed out after {timeout:#?}");
    }
}

//######################################################################
//###################################################################### SANITY CHECK FOR serial_test::serial
static IN_PROGRESS: AtomicBool = AtomicBool::new(false);

#[test]
#[serial_test::serial]
fn test_serial_test_sanity_1() {
    assert!(
        !IN_PROGRESS.swap(true, Ordering::SeqCst),
        "test_serial_test_sanity_1 ran concurrently!"
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    IN_PROGRESS.store(false, Ordering::SeqCst);
}

#[test]
#[serial_test::serial]
fn test_serial_test_sanity_2() {
    assert!(
        !IN_PROGRESS.swap(true, Ordering::SeqCst),
        "test_serial_test_sanity_2 ran concurrently!"
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    IN_PROGRESS.store(false, Ordering::SeqCst);
}
//###################################################################### END sanity check
//######################################################################

#[test]
fn test_overflow_stress() {
    run_test_n_times(1000, 0, false, test_overflow);
}

#[test]
#[serial_test::serial]
fn test_overflow() {
    let max_workers = 2;
    let num_jobs = 64;
    let expected_len = 62;
    let release_chan = unbounded();
    let p = WPool::new(max_workers);
    // Start workers, and have them all wait on a channel before completing.
    for _ in 0..num_jobs {
        let thread_release_receiver = release_chan.clone_receiver();
        p.submit(move || {
            let _ = thread_release_receiver.recv();
        });
    }

    // Start a thread to free the workers after calling stop.  This way
    // the dispatcher can exit, then when this thread runs, the pool
    // can exit.
    let release_thread_sender = release_chan.clone_sender();
    let release_handle = thread::spawn(move || {
        for _ in 0..num_jobs {
            let _ = release_thread_sender.send(());
        }
    });

    p.stop();

    // Now that the pool has exited, it is safe to inspect its waiting
    // queue without causing a race.
    let wq_len = p._waiting_queue_len();
    assert_eq!(
        wq_len, expected_len,
        "Expected waiting to queue to have len of '{expected_len}' but got '{wq_len}'"
    );
    let _ = release_handle.join();
}

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
fn test_stop_abandoned_waiting_queue() {
    run_test_n_times(500, 0, false, || {
        let max_workers = 10;
        let num_jobs = 20;
        let releaser_chan = unbounded::<()>();
        let work_ready = WaitGroup::new();
        work_ready.add(max_workers);

        let wp = WPool::new(max_workers);

        // Fill up our pool with jobs that are blocking while waiting to recv
        let work_ready_clone = work_ready.clone();
        let releaser_receiver_clone = releaser_chan.clone_receiver();
        for _ in 0..num_jobs {
            let ready = work_ready_clone.clone();
            let receiver = releaser_receiver_clone.clone();
            wp.submit(move || {
                ready.done();
                let _ = receiver.recv();
                thread::sleep(Duration::from_millis(1));
            });
        }

        // let wait queue fill up
        work_ready.wait();
        let mut wq_len = wp._waiting_queue_len();
        let max_iters = 100_000;
        let mut i = 0;
        while wq_len != num_jobs - max_workers && i < max_iters {
            wq_len = wp._waiting_queue_len();
            println!(
                "wq_len={wq_len} | num_jobs - max_workers={}",
                num_jobs - max_workers
            );
            i += 1;
        }

        // Release the hounds
        releaser_chan.drop_sender();
        wp.stop();
        assert_eq!(
            wp._waiting_queue_len(),
            num_jobs - max_workers,
            "Expected 0 jobs from wait queue to run after stop()"
        );
    });
}

#[test]
fn test_stop_wait_does_not_abandoned_waiting_queue() {
    run_test_n_times(500, 0, false, || {
        let max_workers = 10;
        let num_jobs = 20;
        let releaser_chan = unbounded::<()>();
        let work_ready = WaitGroup::new();
        work_ready.add(max_workers);

        let wp = WPool::new(max_workers);

        // Fill up our pool with jobs that are blocking while waiting to recv
        let work_ready_clone = work_ready.clone();
        let releaser_receiver_clone = releaser_chan.clone_receiver();
        for _ in 0..num_jobs {
            let ready = work_ready_clone.clone();
            let receiver = releaser_receiver_clone.clone();
            wp.submit(move || {
                ready.done();
                let _ = receiver.recv();
                thread::sleep(Duration::from_millis(1));
            });
        }

        // let wait queue fill up
        work_ready.wait();
        let mut wq_len = wp._waiting_queue_len();
        let max_iters = 100_000; // Just incase, set a ceiling.
        let mut i = 0; // Just incase, set a ceiling.
        while wq_len != num_jobs - max_workers && i < max_iters {
            wq_len = wp._waiting_queue_len();
            println!(
                "wq_len={wq_len} | num_jobs - max_workers={}",
                num_jobs - max_workers
            );
            i += 1;
        }

        // Release the hounds
        releaser_chan.drop_sender();
        wp.stop_wait();
        let len = wp._waiting_queue_len();
        assert_eq!(
            len, 0,
            "Expected waiting queue to be processed after calling stop_wait()! Instead, we have {len} items in wait queue!",
        );
    });
}

#[test]
fn test_min_workers_basic() {
    let max_workers = 5;
    let min_workers = 3;

    let wp = WPool::new_with_min(max_workers, min_workers);

    for _ in 0..max_workers {
        wp.submit(|| {
            thread::sleep(Duration::from_millis(1));
        });
    }

    // give pool time to process
    thread::sleep(Duration::from_millis(5));
    assert_eq!(wp.worker_count(), max_workers);
    // Wait for workers to terminate
    thread::sleep(WORKER_IDLE_TIMEOUT * ((min_workers + 1) as u32));
    assert_eq!(wp.worker_count(), min_workers);
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
    thread::sleep(WORKER_IDLE_TIMEOUT * ((max_workers + 1) as u32));
    p.stop_wait();
    assert_eq!(p.worker_count(), 0);
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
fn test_pause() {
    let max_workers = 25;
    let wp = WPool::new(max_workers);

    let (mut ran_tx, mut ran_rx) = mpsc::channel::<()>();

    wp.submit(move || {
        thread::sleep(Duration::from_millis(1));
        println!(">>>> [from test_pause] >>>> i ran.");
        drop(ran_tx);
    });

    println!(">>>> test_pause >> pausing");
    wp.pause();
    println!(">>>> test_pause >> done pausing");

    // Check that Pause waits for all previously submitted tasks to run. If the job ran, there should be something for us to recv. Otherwise, error.
    match ran_rx.try_recv() {
        Err(TryRecvError::Disconnected) => { /* we want this error, it means we tried recv on a closed chan */
        }
        _ => {
            panic!("task did not finish before Pause returned");
        }
    }

    (ran_tx, ran_rx) = mpsc::channel::<()>();

    println!(">>> test_pause -> ab to submit");
    wp.submit(move || {
        println!(">>>> [from test_pause] -> i ran 2");
        drop(ran_tx);
    });

    // Check that a new task did not run while paused
    #[allow(clippy::single_match)]
    match ran_rx.recv_timeout(Duration::from_millis(1)) {
        Ok(_) => panic!("ran while paused"),
        Err(RecvTimeoutError::Disconnected) => panic!("channel should be open here"),
        _ => {}
    }

    // Check that task was enqueued
    assert_eq!(wp._waiting_queue_len(), 1, "waiting queue size should be 1");

    println!("made it to stop");
    wp.stop();
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

    println!("\nRUNNING FIRST SUB-TEST\n");
    first();
    println!("\nRUNNING SECOND SUB-TEST\n");
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
    thread::sleep(WORKER_IDLE_TIMEOUT + Duration::from_millis(100));
    // Now resume and stop
    pool.resume();
    pool.stop_wait();
    pause_handle.join().unwrap();
    // Test didn't deadlock and tasks completed
    assert!(counter.load(Ordering::SeqCst) > 0);
}

#[test]
#[ignore]
fn test_wait_queue_len_race_2() {
    let max_workers = 5;
    let num_threads = 10;
    let num_jobs = 20;

    let wp_og = Arc::new(Mutex::new(WPool::new(max_workers)));
    let wp = Arc::clone(&wp_og);

    let releaser_og = unbounded::<()>();
    let releaser = releaser_og.clone_receiver();
    let submitter_ready_og = WaitGroup::new();
    submitter_ready_og.add(num_threads * num_jobs);
    let submitter_ready = submitter_ready_og.clone();

    let spawner_thread = thread::spawn(move || {
        for _ in 0..num_threads {
            let wp_clone = Arc::clone(&wp);
            let releaser_recv = releaser.clone();
            let submitter_ready_clone = submitter_ready.clone();

            thread::spawn(move || {
                let wp_lock = safe_lock(&wp_clone);

                for _ in 0..num_jobs {
                    let thread_ready = submitter_ready_clone.clone();
                    let thread_releaser = releaser_recv.clone();

                    wp_lock.submit(move || {
                        thread_ready.done();
                        thread::sleep(Duration::from_micros(1));
                        let _ = thread_releaser.recv();
                    });
                    //println!(
                    //    "[worker][thread={t}][job={j}] wait_queue_len={}",
                    //    wp_lock._waiting_queue_len()
                    //);
                }
            });
        }
    });

    let wp_len_checker = Arc::clone(&wp_og);
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    // thread that constantly just reads wait queue len
    let len_checker_thread = thread::spawn(move || {
        loop {
            if let Err(TryRecvError::Disconnected) = stop_rx.try_recv() {
                break;
            }
            let len = safe_lock(&wp_len_checker)._waiting_queue_len();
            //assert_ne!(len, 0, "Expected len to be > 0");
            println!("[len_checker] wait_que_len={len}");
            //thread::yield_now();
        }
    });

    submitter_ready_og.wait();
    releaser_og.drop_sender();
    drop(stop_tx);
    let _ = spawner_thread.join();
    let _ = len_checker_thread.join();
}

#[test]
#[serial_test::serial]
fn test_wq_race() {
    run_test_n_times(200, 0, true, waiting_queue_len_race);
}

fn waiting_queue_len_race() {
    let num_threads = 100;
    let num_jobs = 20;
    let max_workers = 1;
    let mut handles = Vec::<std::thread::JoinHandle<()>>::new();

    let wp = Arc::new(WPool::new(max_workers));
    let max_chan = unbounded();

    for _ in 0..num_threads {
        let thread_pool = Arc::clone(&wp);
        let max_chan_tx_clone = max_chan.clone_sender();
        handles.push(thread::spawn(move || {
            let mut max = 0;
            for _ in 0..num_jobs {
                thread_pool.submit(move || {
                    thread::sleep(Duration::from_micros(2));
                });
                let waiting = thread_pool._waiting_queue_len();
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
        let t_max = max_chan.recv().unwrap();
        if t_max > final_max {
            final_max = t_max;
        }
    }

    println!("max_seen = {final_max}");
    assert!(
        final_max > 0,
        "expected to see waiting queue size > 0 : got {final_max}"
    );
    assert!(
        final_max < num_threads * num_jobs,
        "should not have seen all tasks on waiting queue"
    );
}

#[test]
#[serial_test::serial]
fn test_stop_race() {
    run_test_n_times(500, 0, false, || {
        let max_workers = 20;
        let work_release_chan = unbounded::<()>();
        let started = WaitGroup::new();
        started.add(max_workers);

        let wp = Arc::new(WPool::new(max_workers));

        // Start workers, and have them all wait on a channel before completing.
        for _ in 0..max_workers {
            let tstarted = started.clone();
            let twork_release_receiver = work_release_chan.clone_receiver();
            wp.submit(move || {
                tstarted.done();
                let _ = twork_release_receiver.recv();
            });
        }

        started.wait();

        let done_callers = 5;
        let stop_done = bounded(done_callers);
        for _ in 0..done_callers {
            let wp_done = Arc::clone(&wp);
            let stop_done_sender = stop_done.clone_sender();
            thread::spawn(move || {
                wp_done.stop();
                let _ = stop_done_sender.send(());
            });
        }

        assert!(
            stop_done.try_recv().is_err(),
            "[we want `stop_done.try_recv()` to not be `ok()`] : stop() should not return in any thread"
        );

        // Close work_release channel to unblock workers
        work_release_chan.drop_sender();

        let timeout = Duration::from_secs(1);
        for _ in 0..done_callers {
            let timed_out = true;
            let should_timeout = false;
            if let Err(RecvTimeoutError::Timeout) = stop_done.recv_timeout(timeout) {
                wp.stop();
                // Just to give us something to assert...
                assert_eq!(
                    timed_out, should_timeout,
                    "timedout waiting for `stop()` to return"
                );
            };
        }
    });
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
    let max_workers = 16;
    let num_jobs = 2000000;
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
    let num_regular_jobs = 50;
    let counter = Arc::new(AtomicUsize::new(0));

    let p = WPool::new(max_workers);

    for _ in 0..num_regular_jobs {
        let counter = Arc::clone(&counter);
        p.submit(move || {
            thread::sleep(Duration::from_micros(1));
            counter.fetch_add(1, Ordering::SeqCst);
        });
    }

    let submit_wait_counter = Arc::clone(&counter);
    p.submit_wait(move || {
        thread::sleep(Duration::from_millis(500));
        submit_wait_counter.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(
        counter.load(Ordering::SeqCst),
        num_regular_jobs + 1,
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

#[test]
fn test_min_workers_greater_than_max_workers() {
    let max_workers = 5;
    let min_workers = 10;
    assert!(min_workers > max_workers); // in case someone changes these...
    let wp = WPool::new_with_min(max_workers, min_workers);
    assert_eq!(
        wp.max_capacity(),
        wp.min_capacity(),
        "min_workers were not truncated to max_workers!"
    );
}

#[test]
fn test_wait_group_done_wait_race() {
    // Ensure there is not a race when calling done() before wait()
    run_test_with_timeout(Duration::from_secs(5), || {
        let wg = WaitGroup::new();
        wg.add(1);
        thread::spawn({
            let wg = wg.clone();
            move || {
                // done() happens before wait() starts waiting
                wg.done();
            }
        });
        // Ensure done() happens before wait()
        thread::sleep(Duration::from_millis(750));
        wg.wait();
    });

    println!("\n-----------------------------------\n");

    run_test_with_timeout(Duration::from_secs(5), || {
        let wg = WaitGroup::new();
        wg.add(1);
        wg.done();
        wg.wait();
    });
}

#[test]
fn test_threaded_deque_push_pop_basic() {
    let deque = ThreadedDeque::new();
    assert!(deque.is_empty());
    assert_eq!(deque.len(), 0);
    deque.push_back(1);
    deque.push_back(2);
    assert!(!deque.is_empty());
    assert_eq!(deque.len(), 2);
    assert_eq!(deque.pop_front(), Some(1));
    assert_eq!(deque.pop_back(), Some(2));
    assert!(deque.is_empty());
}

#[test]
fn test_threaded_deque_test_front_clone() {
    let deque = ThreadedDeque::new();
    deque.push_back(42);
    let front = deque.front();
    assert_eq!(front, Some(42));
    // The deque itself is unchanged
    assert_eq!(deque.len(), 1);
    assert_eq!(deque.pop_front(), Some(42));
}

#[test]
fn test_threaded_deque_clone_shares_state() {
    let deque1 = ThreadedDeque::new();
    let deque2 = deque1.clone();
    deque1.push_back(10);
    // Both see the same element
    assert_eq!(deque2.front(), Some(10));
    // Popping from one affects the other
    assert_eq!(deque2.pop_front(), Some(10));
    assert!(deque1.is_empty());
}

#[test]
fn test_threaded_deque_concurrent_access() {
    let deque = ThreadedDeque::new();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let dq = deque.clone();
            thread::spawn(move || {
                dq.push_back(i);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(deque.len(), 10);

    // Pop all values concurrently
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let dq = deque.clone();
            thread::spawn(move || dq.pop_front())
        })
        .collect();

    let mut results = vec![];
    for h in handles {
        if let Some(v) = h.join().unwrap() {
            results.push(v);
        }
    }

    results.sort();
    assert_eq!(results, (0..10).collect::<Vec<_>>());
    assert!(deque.is_empty());
}

#[test]
fn test_threaded_deque_empty_pop_front_back() {
    let deque: ThreadedDeque<i32> = ThreadedDeque::new();
    assert_eq!(deque.pop_front(), None);
    assert_eq!(deque.pop_back(), None);
    assert_eq!(deque.front(), None);
}

#[test]
fn test_threaded_deque_len_is_empty_consistency() {
    let deque = ThreadedDeque::new();
    assert!(deque.is_empty());
    assert_eq!(deque.len(), 0);
    deque.push_back(1);
    assert!(!deque.is_empty());
    assert_eq!(deque.len(), 1);
    deque.pop_front();
    assert!(deque.is_empty());
    assert_eq!(deque.len(), 0);
}

#[test]
fn test_threaded_deque_front_pop_front_race() {
    run_test_n_times(100, 0, false, || {
        let num_elements = 1000;

        let deque = ThreadedDeque::new();
        for i in 0..num_elements {
            deque.push_back(i);
        }

        // thread that checks front, then if it is Some, pops front.
        // In between checking the front, and popping the front, it is
        // possible that another thread could have popped prior...
        let check_front_deque = deque.clone();
        let checker_thread = thread::spawn(move || {
            // To combat this, acquire the lock.
            let mut deque_guard = check_front_deque.lock();
            while !deque_guard.is_empty() {
                if let Some(got) = deque_guard.front().cloned() {
                    let popped = deque_guard.pop_front();
                    assert_eq!(got, popped.unwrap());
                }
            }
        });

        let pop_deque = deque.clone();
        let popper_thread = thread::spawn(move || {
            while !pop_deque.is_empty() {
                let _ = pop_deque.pop_front();
            }
        });

        checker_thread.join().unwrap();
        popper_thread.join().unwrap();
    });
}

#[test]
#[should_panic]
fn test_threaded_deque_front_pop_front_race_should_panic() {
    run_test_n_times(10, 0, false, || {
        let num_elements = 1000;

        let deque = ThreadedDeque::new();
        for i in 0..num_elements {
            deque.push_back(i);
        }

        // thread that checks front, then if it is Some, pops front.
        // In between checking the front, and popping the front, it is
        // possible that another thread could have popped prior...
        let check_front_deque = deque.clone();
        let checker_thread = thread::spawn(move || {
            while !check_front_deque.is_empty() {
                if let Some(got) = check_front_deque.front() {
                    let popped = check_front_deque.pop_front();
                    //assert_eq!(got, popped.unwrap());
                    if got != popped.unwrap() {
                        panic!("race detected!!!");
                    }
                }
            }
        });

        let pop_deque = deque.clone();
        let popper_thread = thread::spawn(move || {
            while !pop_deque.is_empty() {
                let _ = pop_deque.pop_front();
            }
        });

        checker_thread.join().unwrap();
        popper_thread.join().unwrap();
    });
}

#[test]
fn test_threaded_deque_lock() {
    let deque = ThreadedDeque::new();
    let element = 1;
    deque.push_back(element);
    let mut lock = deque.lock();
    let popped = lock.pop_front();
    drop(lock);
    assert!(deque.is_empty());
    assert_eq!(popped.unwrap(), element);
}
