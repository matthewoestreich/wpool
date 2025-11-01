use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, RecvError, TryRecvError},
    },
    thread,
};

use crate::{Signal, channel::ThreadedChannel, safe_lock, worker::Worker};

// Returns numbers in sequential order. Used as worker id's.
// ```rust
// let next = monotonic_counter();
// let next_number = next();
// ```
fn monotonic_counter() -> Box<dyn Fn() -> usize + Send + 'static> {
    let next = AtomicUsize::new(0);
    Box::new(move || next.fetch_add(1, Ordering::Acquire))
}

//
// Dispatcher is meant to route signals to workers, spawn and/or kil workers,
// listen for any status updates from workers, and holds the 'source-of-truth'
// list containing all active worker threads.
//
// To acheive these goals, it spawns 2 threads : a "worker status handler thread" and
// a "main dispatcher thread".
//
// The "worker status handler thread":
//   - listens for any status updates from worker threads and handles them accordingly
//
// The "main dispatcher thread":
//   - listens for incoming tasks and routes them to workers
//   - spawns new workers if needed
//   - terminates workers during pool shutdown
//   - terminates the "worker status handler thread" during pool shutdown
//
pub(crate) struct Dispatcher {
    pub(crate) waiting: AtomicBool,
    pub(crate) waiting_queue: Mutex<VecDeque<Signal>>,
    pub(crate) workers: Mutex<HashMap<usize, Worker>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    has_spawned: AtomicBool,
    worker_channel: ThreadedChannel<Signal>,
    worker_status_sender: Mutex<Option<mpsc::Sender<usize>>>,
    worker_status_receiver: Arc<Mutex<mpsc::Receiver<usize>>>,
    max_workers: usize,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize, worker_channel: ThreadedChannel<Signal>) -> Self {
        let (status_tx, status_rx) = mpsc::channel();

        Self {
            has_spawned: false.into(),
            handle: None.into(),
            waiting: false.into(),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel,
            workers: HashMap::new().into(),
            worker_status_sender: Some(status_tx).into(),
            worker_status_receiver: Mutex::new(status_rx).into(),
        }
    }

    pub(crate) fn spawn(
        self: Arc<Self>,
        task_channel_receiver: mpsc::Receiver<Signal>,
    ) -> Arc<Self> {
        if self.has_spawned.swap(true, Ordering::Relaxed) {
            return self;
        }

        let get_next_id = monotonic_counter();
        // Copy of dispatcher for the "main dispatcher thread".
        let dispatcher = Arc::clone(&self);
        // Copy of dispatcher for the "worker status handler thread".
        let handler = Arc::clone(&self);

        // This is the "worker status handler thread".
        // Since the dispatchers holds a record of all spawned worker threads, we need to know when
        // a worker terminates itself so we can update said record. A worker will terminate itself
        // if it did not receive a signal within the timeout duration.
        let worker_status_handle = thread::spawn(move || {
            // Block until we get a worker id or worker status channel is closed.
            while let Ok(worker_id) = safe_lock(&handler.worker_status_receiver).recv() {
                if let Some(mut worker) = safe_lock(&handler.workers).remove(&worker_id) {
                    worker.join();
                }
            }
        });

        // This is the "main dispatcher thread".
        // It is responsible for receiving tasks, dispatching tasks to workers, spawning
        // workers, killing workers during pool shutdown, holding the 'source of truth'
        // list for all spawned worker threads, and more.
        *safe_lock(&self.handle) = Some(thread::spawn(move || {
            loop {
                // As long as the waiting queue isn't empty, incoming signals (on task channel)
                // are put into the waiting queue and signals to run are taken from the waiting
                // queue. Once the waiting queue is empty, then go back to submitting incoming
                // signals directly to available workers.
                if !safe_lock(&dispatcher.waiting_queue).is_empty() {
                    if !dispatcher.process_waiting_queue(&task_channel_receiver) {
                        break;
                    }
                    continue;
                }

                // Blocks until we get a task or the task channel is closed.
                let signal = match task_channel_receiver.recv() {
                    Ok(signal) => signal,
                    Err(RecvError) => break,
                };

                let mut workers = safe_lock(&dispatcher.workers);

                if workers.len() < dispatcher.max_workers {
                    if let Some(ref worker_status_sender) =
                        *safe_lock(&dispatcher.worker_status_sender)
                    {
                        let id = get_next_id();
                        let worker = Worker::spawn(
                            id,
                            Arc::clone(&dispatcher.worker_channel.receiver),
                            worker_status_sender.clone(),
                        );
                        workers.insert(id, worker);
                    }

                    // Non-blocking. Send signal to workers channel, break if worker channel is closed.
                    if dispatcher.worker_channel.sender.send(signal).is_err() {
                        break;
                    }
                } else {
                    // At max workers, put signal in waiting queue.
                    safe_lock(&dispatcher.waiting_queue).push_back(signal);
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if dispatcher.is_waiting() {
                dispatcher.run_queued_tasks();
            }

            // Acquire longer-lived lock, we don't want anything messing with our workers list during shutdown.
            let mut workers_guard = safe_lock(&dispatcher.workers);
            dispatcher.kill_all_workers(&mut workers_guard);

            // Close worker status channel.
            if let Some(tx) = safe_lock(&dispatcher.worker_status_sender).take() {
                drop(tx);
            }
            // Block until worker status thread has ended.
            let _ = worker_status_handle.join();
        }));

        self
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = safe_lock(&self.handle).take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }

    pub(crate) fn set_is_waiting(&self, is_waiting: bool) {
        self.waiting.store(is_waiting, Ordering::Relaxed);
    }

    // Force the caller to already have a lock
    fn kill_all_workers(&self, workers_guard: &mut MutexGuard<'_, HashMap<usize, Worker>>) {
        let workers: Vec<Worker> = workers_guard.drain().map(|(_, worker)| worker).collect();
        // Send kill signal to all worker threads.
        for _ in 0..workers.len() {
            let _ = self.worker_channel.sender.send(Signal::Terminate);
        }
        // Block until all worker threads have ended.
        for mut worker in workers {
            worker.join();
        }
    }

    fn process_waiting_queue(&self, task_receiver: &mpsc::Receiver<Signal>) -> bool {
        match task_receiver.try_recv() {
            // Place any new signal from the task channel in the waiting queue.
            Ok(signal) => {
                safe_lock(&self.waiting_queue).push_back(signal);
                true
            }
            // If nothing came in on the task channel, try to grab something from
            // the waiting queue and send it to workers.
            Err(TryRecvError::Empty) => {
                if let Some(signal) = safe_lock(&self.waiting_queue).pop_front() {
                    let _ = self.worker_channel.sender.send(signal);
                }
                true
            }
            // Task channel was closed.
            Err(TryRecvError::Disconnected) => false,
        }
    }

    fn run_queued_tasks(&self) {
        let mut wq = safe_lock(&self.waiting_queue);
        while !wq.is_empty() {
            if let Some(signal) = wq.pop_front() {
                let _ = self.worker_channel.sender.send(signal);
            }
        }
    }
}
