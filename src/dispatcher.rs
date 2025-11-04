use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::TryRecvError,
    },
    thread,
};

use crate::{
    channel::{BoundedChannel, Channel, UnboundedChannel},
    job::Signal,
    monotonic_counter, safe_lock,
    worker::{Worker, WorkerStatus},
};

//
// Dispatcher is meant to route signals to workers, spawn and/or kil workers,
// listen for any status updates from workers, and holds the 'source-of-truth'
// list containing all active worker threads.
//
// To acheive these goals, it spawns 2 threads : a "worker status handler thread" and a "main dispatcher thread".
//
// The "worker status handler thread":
//   - Listens for any status updates from worker threads and handles them accordingly
//   - For exampe, workers are responsibe for timing out themselves, which means they terminate
//     themselves. Since the dispatcher holds a list of all active threads, we need to know
//     when a worker dies, so we can update said list.
//
// The "main dispatcher thread":
//   - Listens for incoming tasks and routes them to workers
//   - Spawns new workers if needed
//   - Terminates workers during pool shutdown
//   - Terminates the "worker status handler thread" during pool shutdown
//
pub(crate) struct Dispatcher {
    waiting: AtomicBool,
    workers: Mutex<HashMap<usize, Worker>>,
    waiting_queue: Mutex<VecDeque<Signal>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    has_spawned: AtomicBool,
    worker_channel: BoundedChannel<Signal>,
    task_channel: UnboundedChannel<Signal>,
    worker_status_channel: UnboundedChannel<WorkerStatus>,
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
            worker_channel: Channel::new_bounded(0),
            worker_status_channel: Channel::new_unbounded(),
            task_channel: Channel::new_unbounded(),
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
            while let Ok(WorkerStatus::Terminating(id)) = handler.worker_status_channel.recv() {
                if let Some(mut worker) = handler.remove_worker_from_cache(&id) {
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
                if !dispatcher.is_waiting_queue_empty() {
                    if !dispatcher.process_waiting_queue() {
                        break;
                    }
                    continue;
                }

                let signal = match dispatcher.task_channel.recv() {
                    Ok(signal) => signal,
                    Err(_) => break,
                };

                // At max workers
                if dispatcher.workers_len() >= dispatcher.max_workers {
                    dispatcher.waiting_queue_push_back(signal);
                    continue;
                }

                if let Some(status_sender) = dispatcher.worker_status_channel.clone_sender() {
                    let id = get_next_id();
                    dispatcher.add_worker_to_cache(
                        id,
                        Worker::spawn(
                            id,
                            dispatcher.worker_channel.clone_receiver(),
                            status_sender,
                            signal,
                        ),
                    );
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if dispatcher.is_waiting() {
                dispatcher.run_queued_tasks();
            }

            dispatcher.kill_all_workers();
            dispatcher.worker_status_channel.close();
            let _ = worker_status_handle.join();
        }));

        self
    }

    pub(crate) fn submit(&self, signal: Signal) {
        let _ = self.task_channel.send(signal);
    }

    pub(crate) fn close_task_channel(&self) {
        self.task_channel.close();
    }

    #[allow(dead_code)]
    pub(crate) fn waiting_queue_len(&self) -> usize {
        safe_lock(&self.waiting_queue).len()
    }

    pub(crate) fn workers_len(&self) -> usize {
        safe_lock(&self.workers).len()
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

    #[allow(dead_code)]
    fn close_worker_channel(&self) {
        self.worker_channel.close();
    }

    fn waiting_queue_push_back(&self, signal: Signal) {
        safe_lock(&self.waiting_queue).push_back(signal);
    }

    // NOTE: this does not remove anything from underlying queue!!!
    fn waiting_queue_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).front().cloned()
    }

    fn waiting_queue_pop_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).pop_front()
    }

    fn is_waiting_queue_empty(&self) -> bool {
        safe_lock(&self.waiting_queue).is_empty()
    }

    fn _workers_is_empty(&self) -> bool {
        safe_lock(&self.workers).is_empty()
    }

    fn add_worker_to_cache(&self, id: usize, worker: Worker) -> Option<Worker> {
        safe_lock(&self.workers).insert(id, worker)
    }

    fn remove_worker_from_cache(&self, element: &usize) -> Option<Worker> {
        safe_lock(&self.workers).remove(element)
    }

    fn kill_all_workers(&self) {
        let mut workers = safe_lock(&self.workers);
        // Send kill signal to workers as they become available
        for _ in workers.iter() {
            // Will block until a ready worker calls recv
            let _ = self.worker_channel.send(Signal::Terminate);
        }
        // Join worker threads, will block until all workers have finished.
        for (_, mut worker) in workers.drain() {
            worker.join();
        }
    }

    fn process_waiting_queue(&self) -> bool {
        match self.task_channel.try_recv() {
            // Got something from task channel, put in wait queue.
            Ok(signal) => {
                self.waiting_queue_push_back(signal);
                true
            }
            // Task channel empty...
            Err(TryRecvError::Empty) => {
                if self.waiting_queue_front().is_some()
                    && self
                        .worker_channel
                        .send(self.waiting_queue_front().expect("already checked front"))
                        .is_ok()
                {
                    let _ = self.waiting_queue_pop_front();
                }
                true
            }
            // Task channel closed
            Err(_) => false,
        }
    }

    fn run_queued_tasks(&self) {
        // Acquire lock for entirety of this process.
        let mut wq = safe_lock(&self.waiting_queue);
        while !wq.is_empty() {
            if wq.front().is_some() {
                let _ = self
                    .worker_channel
                    .send(wq.pop_front().expect("already checked front"));
            }
        }
    }
}
