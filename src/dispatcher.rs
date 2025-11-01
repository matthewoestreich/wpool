use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{RecvError, TryRecvError},
    },
    thread,
};

use crate::{
    Signal,
    channel::OptionShareChannel,
    safe_lock,
    worker::{Worker, WorkerStatus},
};

// Returns numbers in sequential order. Used as worker id's.
// ```rust
// let next = monotonic_counter();
// let next_number = next();
// ```
fn monotonic_counter() -> Box<dyn Fn() -> usize + Send + 'static> {
    let next = AtomicUsize::new(0);
    Box::new(move || next.fetch_add(1, Ordering::SeqCst))
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
    worker_channel: OptionShareChannel<Signal>,
    task_channel: OptionShareChannel<Signal>,
    worker_status_channel: OptionShareChannel<WorkerStatus>,
    available_workers: Mutex<HashSet<usize>>,
    max_workers: usize,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize) -> Self {
        Self {
            has_spawned: false.into(),
            handle: None.into(),
            waiting: false.into(),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel: OptionShareChannel::new(),
            worker_status_channel: OptionShareChannel::new(),
            task_channel: OptionShareChannel::new(),
            available_workers: HashSet::new().into(),
            workers: HashMap::new().into(),
        }
    }

    pub(crate) fn spawn(self: Arc<Self>) -> Arc<Self> {
        if self.has_spawned.swap(true, Ordering::SeqCst) {
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
            loop {
                match handler.worker_status_channel.recv() {
                    Ok(WorkerStatus::Terminating(id)) => {
                        handler.available_workers_remove(&id);
                        handler.workers_remove_and_join(&id);
                    }
                    Ok(WorkerStatus::Unavailable(id)) => {
                        handler.available_workers_remove(&id);
                    }
                    Ok(WorkerStatus::Available(id)) => {
                        handler.available_workers_insert(id);
                    }
                    Err(RecvError) => {
                        break;
                    }
                };
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
                if !dispatcher.is_waiting_queue_empty() {
                    if !dispatcher.process_waiting_queue() {
                        break;
                    }
                    continue;
                }

                // Blocks until we get a task or the task channel is closed.
                let signal = match dispatcher.task_channel.recv() {
                    Ok(signal) => signal,
                    Err(RecvError) => break,
                };

                if dispatcher.workers_len() < dispatcher.max_workers {
                    let id = get_next_id();

                    let worker = Worker::spawn(
                        id,
                        Arc::clone(&dispatcher.worker_channel.receiver),
                        dispatcher.worker_status_channel.clone_sender(),
                    );

                    dispatcher.workers_insert(id, worker);
                    dispatcher.available_workers_insert(id);

                    if dispatcher.has_available_workers() {
                        if !dispatcher.worker_channel.send(signal) {
                            break;
                        }
                    } else {
                        dispatcher.waiting_queue_push_back(signal);
                    }
                } else {
                    // At max workers, put signal in waiting queue.
                    dispatcher.waiting_queue_push_back(signal);
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if dispatcher.is_waiting() {
                dispatcher.run_queued_tasks();
            }

            dispatcher.kill_all_workers();
            dispatcher.close_worker_status_channel();
            let _ = worker_status_handle.join();
        }));

        self
    }

    pub(crate) fn submit(&self, signal: Signal) {
        self.task_channel.send(signal);
    }

    pub(crate) fn close_task_channel(&self) {
        self.task_channel.close();
    }

    pub(crate) fn close_worker_status_channel(&self) {
        self.worker_status_channel.close();
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = safe_lock(&self.handle).take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::SeqCst)
    }

    pub(crate) fn set_is_waiting(&self, is_waiting: bool) {
        self.waiting.store(is_waiting, Ordering::SeqCst);
    }

    fn waiting_queue_push_back(&self, signal: Signal) {
        safe_lock(&self.waiting_queue).push_back(signal);
    }

    fn waiting_queue_pop_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).pop_front()
    }

    fn is_waiting_queue_empty(&self) -> bool {
        safe_lock(&self.waiting_queue).is_empty()
    }

    fn has_available_workers(&self) -> bool {
        !safe_lock(&self.available_workers).is_empty()
    }

    fn available_workers_insert(&self, element: usize) {
        safe_lock(&self.available_workers).insert(element);
    }

    fn available_workers_remove(&self, element: &usize) -> bool {
        safe_lock(&self.available_workers).remove(element)
    }

    fn workers_len(&self) -> usize {
        safe_lock(&self.workers).len()
    }

    fn workers_insert(&self, id: usize, worker: Worker) -> Option<Worker> {
        safe_lock(&self.workers).insert(id, worker)
    }

    fn workers_remove(&self, element: &usize) -> Option<Worker> {
        safe_lock(&self.workers).remove(element)
    }

    fn workers_remove_and_join(&self, id: &usize) {
        if let Some(mut worker) = self.workers_remove(id) {
            worker.join();
        }
    }

    fn kill_all_workers(&self) {
        self.worker_channel.close();
        // Block until all worker threads have ended.
        for (_, mut worker) in safe_lock(&self.workers).drain() {
            worker.join();
        }
    }

    fn process_waiting_queue(&self) -> bool {
        match safe_lock(&self.task_channel.receiver).try_recv() {
            // Place any new signal from the task channel in the waiting queue.
            Ok(signal) => {
                self.waiting_queue_push_back(signal);
                true
            }
            // If nothing came in on the task channel, try to grab something from
            // the waiting queue and send it to workers.
            Err(TryRecvError::Empty) => {
                // If no available workers, return true so the main loop can continue
                if !self.has_available_workers() {
                    return true;
                }
                if let Some(signal) = self.waiting_queue_pop_front() {
                    self.worker_channel.send(signal);
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
            if wq.front().is_some() {
                self.worker_channel
                    .send(wq.pop_front().expect("already checked front"));
            }
        }
    }
}
