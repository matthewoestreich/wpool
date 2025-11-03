use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};

use crate::{
    Channel, Signal, monotonic_counter, safe_lock,
    worker::{Worker, WorkerStatus},
};

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
    worker_channel: Channel<Mutex<Option<Sender<Signal>>>, Receiver<Signal>>,
    task_channel: Channel<Mutex<Option<Sender<Signal>>>, Receiver<Signal>>,
    worker_status_channel: Channel<Mutex<Option<Sender<WorkerStatus>>>, Receiver<WorkerStatus>>,
    max_workers: usize,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize) -> Self {
        let (task_tx, task_rx) = unbounded();
        let (status_tx, status_rx) = unbounded();
        let (worker_tx, worker_rx) = bounded(0);

        Self {
            has_spawned: false.into(),
            handle: None.into(),
            waiting: false.into(),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel: Channel::new(Some(worker_tx).into(), worker_rx),
            worker_status_channel: Channel::new(Some(status_tx).into(), status_rx),
            task_channel: Channel::new(Some(task_tx).into(), task_rx),
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
            while let Ok(WorkerStatus::Terminating(id)) =
                handler.worker_status_channel.receiver.recv()
            {
                handler.workers_remove_and_join(&id);
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

                let signal = match dispatcher.task_channel.receiver.recv() {
                    Ok(signal) => signal,
                    Err(_) => break,
                };

                // At max workers
                if dispatcher.workers_len() >= dispatcher.max_workers {
                    println!("  dispatcher -> at max workers, putting signal in wait queue");
                    dispatcher.waiting_queue_push_back(signal);
                    continue;
                }

                println!("  dispatcher -> not at max workers, spawning");

                let status_sender_guard = safe_lock(&dispatcher.worker_status_channel.sender);
                if let Some(status_sender) = status_sender_guard.clone() {
                    let id = get_next_id();
                    dispatcher.workers_insert(
                        id,
                        Worker::spawn(
                            id,
                            dispatcher.worker_channel.receiver.clone(),
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

            println!("dispatcher -> killing all workers");
            dispatcher.kill_all_workers();
            println!("dispatcher -> closing worker status channel.");
            dispatcher.close_worker_status_channel();
            let _ = worker_status_handle.join();
            println!("exiting dispatchh thread");
        }));

        self
    }

    pub(crate) fn submit(&self, signal: Signal) {
        if let Some(task_sender) = safe_lock(&self.task_channel.sender).as_ref() {
            let _ = task_sender.send(signal);
        }
    }

    pub(crate) fn close_task_channel(&self) {
        if let Some(task_sender) = safe_lock(&self.task_channel.sender).take() {
            drop(task_sender);
        }
    }

    pub(crate) fn close_worker_status_channel(&self) {
        if let Some(status_sender) = safe_lock(&self.worker_status_channel.sender).take() {
            drop(status_sender);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn close_worker_channel(&self) {
        if let Some(worker_sender) = safe_lock(&self.worker_channel.sender).take() {
            drop(worker_sender);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn waiting_queue_len(&self) -> usize {
        safe_lock(&self.waiting_queue).len()
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

    #[allow(dead_code)]
    fn waiting_queue_pop_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).pop_front()
    }

    fn is_waiting_queue_empty(&self) -> bool {
        safe_lock(&self.waiting_queue).is_empty()
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
        println!("[kill_workers] getting workers");
        let mut workers_guard = safe_lock(&self.workers);
        let workers: Vec<Worker> = workers_guard.drain().map(|(_, worker)| worker).collect();
        println!("[kill_workers] drained workers");
        let workers_sender = safe_lock(&self.worker_channel.sender).clone().unwrap();
        // Send kill signal to all worker threads.
        for _ in 0..workers.len() {
            let _ = workers_sender.send(Signal::Terminate);
        }
        println!("[kill_workers] sent kill signal to every worker, ab to join threads");
        // Block until all worker threads have ended.
        for mut worker in workers {
            worker.join();
        }
        println!("[kill_workers] all workers should be dead");
    }

    fn process_waiting_queue(&self) -> bool {
        println!(
            "[process_waiting_queue] start : wait_queue_len={}",
            self.waiting_queue_len()
        );

        crossbeam_channel::select! {
            recv(self.task_channel.receiver) -> signal_result => {
                match signal_result {
                    Ok(signal) => {
                        self.waiting_queue_push_back(signal);
                        //let mut wq = self.waiting_queue.lock().unwrap();
                        //wq.push_back(signal);
                        //drop(wq);
                    }
                    Err(_) => {
                        println!("[process_waiting_queue] task channel closed");
                        return false;
                    },
                }
            }
            default => {
                if let Some(signal) = self.waiting_queue_pop_front() {
                    if let Some(sender) = safe_lock(&self.worker_channel.sender).clone() {
                        let _ = sender.clone().send(signal);
                    }
                }
            }
        };

        true
    }

    fn run_queued_tasks(&self) {
        let mut wq = safe_lock(&self.waiting_queue);
        while !wq.is_empty() {
            if wq.front().is_some() {
                if let Some(s) = safe_lock(&self.worker_channel.sender).as_ref() {
                    let _ = s.send(wq.pop_front().expect("already checked front"));
                }
            }
        }
    }
}
