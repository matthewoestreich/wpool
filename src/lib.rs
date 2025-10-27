use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};

type Task = Box<dyn FnOnce() + Send + 'static>;

pub struct WPool {
    //stop_lock: Mutex<bool>,
    //waiting_len: u32,
    max_workers: usize,
    task_queue: mpsc::Sender<Task>,
    worker_queue: mpsc::Sender<()>,
    waiting_queue: Arc<Mutex<VecDeque<Task>>>,
    stop_once: Once,
    handles: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    dispatch_handle: Option<thread::JoinHandle<()>>,
    is_stopped: Arc<AtomicBool>,
    is_waiting: Arc<AtomicBool>,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let (task_tx, task_rx) = mpsc::channel();
        let (worker_tx, worker_rx) = mpsc::channel();

        let mut this = Self {
            //stop_lock: Mutex::new(true),
            //waiting_len: 0,
            max_workers,
            task_queue: task_tx,
            worker_queue: worker_tx,
            waiting_queue: Arc::new(Mutex::new(VecDeque::new())),
            stop_once: Once::new(),
            handles: Arc::new(Mutex::new(Vec::new())),
            dispatch_handle: None,
            is_stopped: Arc::new(AtomicBool::new(false)),
            is_waiting: Arc::new(AtomicBool::new(false)),
        };

        // Fill worker_queue with tokens (as semaphore)
        for _ in 0..max_workers {
            this.worker_queue.send(()).unwrap();
        }

        this.dispatch_handle = this.dispatch(task_rx, worker_rx);
        this
    }

    pub fn size(&self) -> usize {
        self.max_workers
    }

    pub fn submit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped.load(Ordering::Relaxed) {
            return;
        }
        let _ = self.task_queue.send(Box::new(f));
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

    /// Stop and wait for all workers
    pub fn stop_wait(&mut self) {
        self.stop(true);
    }

    fn dispatch(
        &self,
        task_rx: mpsc::Receiver<Task>,
        worker_rx: mpsc::Receiver<()>,
    ) -> Option<thread::JoinHandle<()>> {
        let worker_tx = self.worker_queue.clone();
        let waiting_queue = Arc::clone(&self.waiting_queue);
        let handles = Arc::clone(&self.handles);

        Some(thread::spawn(move || {
            loop {
                // Prefer waiting queue first
                let task_maybe = waiting_queue
                    .lock()
                    .unwrap()
                    .pop_front()
                    .or_else(|| task_rx.try_recv().ok());

                match task_maybe {
                    Some(task) => {
                        // No available worker → push to waiting queue
                        if worker_rx.recv().is_err() {
                            waiting_queue.lock().unwrap().push_front(task);
                            return;
                        }

                        // Acquire a worker token
                        let worker_tx_clone = worker_tx.clone();
                        let handle = thread::spawn(move || {
                            (task)();
                            // Release worker back to worker queue
                            let _ = worker_tx_clone.send(());
                        });

                        handles.lock().unwrap().push(handle);
                    }
                    None => {
                        // If no tasks and waiting queue is empty
                        if task_rx.try_recv().is_err() && waiting_queue.lock().unwrap().is_empty() {
                            break;
                        }
                    }
                }
            }
        }))
    }

    fn stop(&mut self, wait: bool) {
        self.stop_once.call_once(|| {
            drop(self.task_queue.clone()); // close task queue

            // Wait for dispatch thread to end
            if let Some(dh) = self.dispatch_handle.take() {
                dh.join().unwrap();
            }

            // Wait for all worker threads to end
            let mut handles = self.handles.lock().unwrap();
            for h in handles.drain(..) {
                h.join().unwrap();
            }

            self.is_waiting.store(wait, Ordering::Relaxed);
            self.is_stopped.store(true, Ordering::Relaxed);
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
        let counter = Arc::new(AtomicUsize::new(0));

        let mut p = WPool::new(max_workers);

        for i in 0..max_workers {
            let counter_clone = counter.clone();
            p.submit(move || {
                thread::sleep(Duration::from_millis(10));
                counter_clone.fetch_add(1, Ordering::Relaxed);
                println!("job {i:?} done");
            });
        }
        p.stop_wait();
        assert_eq!(counter.load(Ordering::Relaxed), max_workers);
    }

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
}
