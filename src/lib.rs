use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, TrySendError},
    },
    thread,
    time::Duration,
};

type Task = Box<dyn FnOnce() + Send + 'static>;

enum Signal {
    Job(Task),
    Pause,
    Terminate,
}

struct WPoolCore {
    //stop_lock: Mutex<bool>,
    //waiting_len: u32,
    max_workers: usize,
    worker_queue: mpsc::SyncSender<Signal>,
    waiting_queue: Mutex<VecDeque<Task>>,
    dispatch_handle: Mutex<Option<thread::JoinHandle<()>>>,
    worker_handles: Mutex<Vec<thread::JoinHandle<()>>>,
    is_stopped: AtomicBool,
    is_waiting: AtomicBool,
    stop_once: Once,
}

pub struct WPool {
    core: Arc<WPoolCore>,
    task_queue: Option<mpsc::Sender<Signal>>,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let (task_tx, task_rx) = mpsc::channel();
        let (worker_tx, worker_rx) = mpsc::sync_channel(max_workers);

        let core = Arc::new(WPoolCore {
            //stop_lock: Mutex::new(true),
            //waiting_len: 0,
            max_workers,
            worker_queue: worker_tx,
            waiting_queue: Mutex::new(VecDeque::new()),
            stop_once: Once::new(),
            dispatch_handle: Mutex::new(None),
            worker_handles: Mutex::new(Vec::new()),
            is_stopped: AtomicBool::new(false),
            is_waiting: AtomicBool::new(false),
        });

        let this = Self {
            core: Arc::clone(&core),
            task_queue: Some(task_tx),
        };

        *core.dispatch_handle.lock().unwrap() = Some(Self::dispatch(
            Arc::clone(&core),
            task_rx,
            Arc::new(Mutex::new(worker_rx)),
        ));

        this
    }

    pub fn size(&self) -> usize {
        self.core.max_workers
    }

    pub fn submit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.core.is_stopped.load(Ordering::Relaxed) {
            println!("submit(f) -> tried to submit to a stopped pool!");
            return;
        }
        if let Some(tx) = &self.task_queue {
            let _ = tx.send(Signal::Job(Box::new(f)));
        }
    }

    // Enqueues the given function and waits for it to be executed.
    pub fn submit_wait<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        // submit task normally (to task_queue)
        self.submit(move || {
            f();
            let _ = done_tx.send(()); // signal completion
        });
        done_rx.recv().unwrap(); // blocks until complete
    }

    // Stop and wait for all current + waiting_queue tasks.
    pub fn stop_wait(&mut self) {
        self.shutdown(true);
    }

    fn dispatch(
        core: Arc<WPoolCore>,
        task_rx: mpsc::Receiver<Signal>,
        worker_rx: Arc<Mutex<mpsc::Receiver<Signal>>>,
    ) -> thread::JoinHandle<()> {
        let worker_tx = core.worker_queue.clone();
        let max_workers = core.max_workers;

        thread::spawn(move || {
            loop {
                println!(".");
                let task = match task_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(Signal::Job(task)) => {
                        println!(
                            "dispatch() -> task_rx.recv_timeout(..) -> Ok(signal) -> signal is a task"
                        );
                        task
                    }
                    Ok(_) => {
                        println!(
                            "dispatch() -> task_rx.recv_timeout(..) -> Ok(signal) -> signal is NOT a task"
                        );
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // If pool is stopped, break loop.
                        println!(
                            "dispatch() -> task_rx.recv_timeout(..) -> Err(Timeout) -> did not get task in allowed time, continuing main loop."
                        );
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        println!("dispatch() -> task channel closed, breaking.");
                        break;
                    }
                };

                // Non blocking. If sending fails, it means workers are all busy so add task to waiting_queue, or channel was closed.
                match worker_tx.try_send(Signal::Job(task)) {
                    Ok(_) => {
                        println!(
                            "dispatch() -> worker_tx.try_send() -> successfully sent task to worker queue"
                        );
                        // Spawn new worker if we are not at max_workers
                        let mut workers = core.worker_handles.lock().unwrap();
                        if workers.len() < max_workers {
                            let worker_handle =
                                WPool::worker(workers.len(), Arc::clone(&worker_rx));
                            workers.push(worker_handle);
                            println!(
                                "dispatch() -> worker_tx.try_send() -> unable to give task to worker, all busy but not at max workers, so spawning new worker"
                            );
                        }
                    }
                    Err(TrySendError::Full(signal)) => {
                        if let Signal::Job(task) = signal {
                            println!(
                                "dispatch() -> worker_tx.try_send() -> unable to send to worker, all full -> at max workers, adding to waiting queue"
                            );
                            // Add to waiting_queue
                            core.waiting_queue.lock().unwrap().push_back(task);
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        // Worker channel closed
                        println!(
                            "dispatch() -> worker_tx.try_send() -> disconnected, worker channel closed"
                        );
                        break;
                    }
                }
            }
            println!("dispatch() -> broken out of loop");
        })
    }

    // FIXME : maybe avoid wrapping receiver in mutex and just use arc?
    fn worker(id: usize, worker_rx: Arc<Mutex<mpsc::Receiver<Signal>>>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            loop {
                println!("~w_{id}~");
                let signal = {
                    let receiver = worker_rx.lock().unwrap();
                    receiver.recv().unwrap()
                };
                match signal {
                    Signal::Job(task) => {
                        println!("worker() -> id={id} -> got task");
                        task();
                    }
                    _ => {
                        println!("worker() -> id={id} -> signalled to terminate -> exiting");
                        break;
                    }
                }
            }
        })
    }

    fn shutdown(&mut self, wait: bool) {
        self.core.stop_once.call_once(|| {
            self.core.is_stopped.store(true, Ordering::Relaxed);
            self.core.is_waiting.store(wait, Ordering::Relaxed);

            println!("shutdown() -> closing task_queue");
            if let Some(task_queue) = self.task_queue.take() {
                drop(task_queue);
            }

            if let Some(handle) = self.core.dispatch_handle.lock().unwrap().take() {
                println!("shutdown() -> got dispatch_hande, about to join()");
                let dh = handle.join();
                println!("shutdown() -> done calling join on dispatcch_handle : {dh:?}");
            }

            let mut workers = self.core.worker_handles.lock().unwrap();
            println!(
                "shutdown() -> sending terminate signal to all workers -> workers='{workers:?}'"
            );
            for _ in 0..workers.len() {
                let _ = self.core.worker_queue.send(Signal::Terminate);
            }
            println!("shutdown() -> joining worker threads");
            for w in workers.drain(..) {
                let _ = w.join();
            }

            println!("shutdown() -> done.");
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

    use crate::WPool;

    #[test]
    fn test_new() {
        let max_workers = 3;
        let num_jobs = max_workers;
        let counter = Arc::new(AtomicUsize::new(0));

        let mut p = WPool::new(max_workers);

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

    /*
    #[test]
    fn test_job_actually_ran() {
        let mut p = WPool::new(3);
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
        let long_running_task_sleep_for = Duration::from_secs(1);
        let default_task_sleep_for = Duration::from_micros(1);
        let counter = Arc::new(AtomicUsize::new(0));

        let mut p = WPool::new(max_workers);

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
        let num_jobs = 16000;
        let counter = Arc::new(AtomicUsize::new(0));

        let mut p = WPool::new(max_workers);

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

        let mut p = WPool::new(max_workers);

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

        let mut pool = WPool::new(max_workers);

        for _ in 0..num_jobs {
            let concurrent_count = concurrent_count.clone();
            let max_seen = max_seen.clone();
            pool.submit(move || {
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

        pool.stop_wait();
        assert!(
            max_seen.load(Ordering::SeqCst) <= max_workers,
            "Pool was running more than 'max_workers' number of workers. Concurrency was not limited"
        );
    }

    #[test]
    fn test_multiple_stop_wait() {
        let mut p = WPool::new(3);
        p.submit(|| {});
        p.stop_wait();
        p.stop_wait();
        // No need to assert anything, if this panics the test will fail.
    }
    */
}
