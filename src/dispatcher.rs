use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvError, TryRecvError},
    },
    thread,
};

use crate::{
    channel::ThreadedChannel,
    lock_safe,
    signal::Signal,
    worker::{Worker, WorkerIDFactory, WorkerStatus},
};

struct WorkerStatusChannel {
    sender: Mutex<Option<mpsc::Sender<WorkerStatus>>>,
    receiver: Arc<Mutex<mpsc::Receiver<WorkerStatus>>>,
}

impl WorkerStatusChannel {
    fn new(
        sender: Mutex<Option<mpsc::Sender<WorkerStatus>>>,
        receiver: Arc<Mutex<mpsc::Receiver<WorkerStatus>>>,
    ) -> Self {
        Self { sender, receiver }
    }
}

pub(crate) struct Dispatcher {
    has_spawned: AtomicBool,
    worker_status_channel: WorkerStatusChannel,
    pub(crate) handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) waiting: AtomicBool,
    pub(crate) max_workers: usize,
    pub(crate) waiting_queue: Mutex<VecDeque<Signal>>,
    pub(crate) worker_channel: ThreadedChannel<Signal>,
    pub(crate) workers: Mutex<HashMap<usize, Worker>>,
    pub(crate) worker_id_factory: WorkerIDFactory,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize, worker_channel: ThreadedChannel<Signal>) -> Self {
        let wsc = ThreadedChannel::new();
        let worker_status_channel = WorkerStatusChannel::new(Some(wsc.sender).into(), wsc.receiver);

        Self {
            has_spawned: AtomicBool::new(false),
            handle: None.into(),
            waiting: AtomicBool::new(false),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel,
            workers: HashMap::new().into(),
            worker_status_channel,
            worker_id_factory: WorkerIDFactory::new(),
        }
    }

    pub(crate) fn spawn(
        self: Arc<Self>,
        task_channel_receiver: mpsc::Receiver<Signal>,
    ) -> Arc<Self> {
        if self.has_spawned.swap(true, Ordering::Relaxed) {
            return self;
        }

        let controller = Arc::clone(&self);
        let worker_status_handle = thread::spawn(move || {
            loop {
                let worker_status =
                    match lock_safe(&controller.worker_status_channel.receiver).recv() {
                        Ok(status) => status,
                        Err(_) => break,
                    };

                match worker_status {
                    WorkerStatus::Terminated(worker_id) => {
                        let mut workers_lock = lock_safe(&controller.workers);
                        if let Some(mut worker) = workers_lock.remove(&worker_id) {
                            worker.join();
                        }
                    }
                }
            }
        });

        let dispatcher = Arc::clone(&self);
        *lock_safe(&self.handle) = Some(thread::spawn(move || {
            loop {
                // As long as the waiting queue isn't empty, incoming signals (on task channel)
                // are put into the waiting queue and signals to run are taken from the waiting
                // queue. Once the waiting queue is empty, then go back to submitting incoming
                // signals directly to available workers.
                if !lock_safe(&dispatcher.waiting_queue).is_empty() {
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

                // Got a signal.
                let mut workers = lock_safe(&dispatcher.workers);
                if workers.len() < dispatcher.max_workers {
                    let worker_id = dispatcher.worker_id_factory.next();
                    if let Some(worker_status_sender) =
                        lock_safe(&dispatcher.worker_status_channel.sender).clone()
                    {
                        workers.entry(worker_id).or_insert(Worker::spawn(
                            worker_id,
                            Arc::clone(&dispatcher.worker_channel.receiver),
                            worker_status_sender,
                        ));
                    }
                    // Non-blocking. Send signal to workers channel, break if worker channel is closed.
                    if dispatcher.worker_channel.sender.send(signal).is_err() {
                        break;
                    }
                } else {
                    // At max workers, put signal in waiting queue.
                    lock_safe(&dispatcher.waiting_queue).push_back(signal);
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if dispatcher.is_waiting() {
                dispatcher.run_queued_tasks();
            }

            // Acquire longer-lived lock.
            let mut workers_lock = lock_safe(&dispatcher.workers);
            let workers: Vec<Worker> = workers_lock.drain().map(|(_, v)| v).collect();

            // Kill all worker threads.
            for _ in 0..workers.len() {
                let _ = dispatcher.worker_channel.sender.send(Signal::Terminate);
            }

            // Block until all worker threads have ended.
            for mut worker in workers {
                worker.join();
            }

            if let Some(worker_status_sender) =
                lock_safe(&dispatcher.worker_status_channel.sender).take()
            {
                drop(worker_status_sender);
            }
            let _ = worker_status_handle.join();
        }));

        self
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = lock_safe(&self.handle).take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }

    pub(crate) fn set_is_waiting(&self, is_waiting: bool) {
        self.waiting.store(is_waiting, Ordering::Relaxed);
    }

    fn process_waiting_queue(&self, task_receiver: &mpsc::Receiver<Signal>) -> bool {
        let mut waiting_queue = lock_safe(&self.waiting_queue);

        match task_receiver.try_recv() {
            Ok(signal) => waiting_queue.push_back(signal),
            Err(TryRecvError::Disconnected) => return false,
            Err(TryRecvError::Empty) => {
                if let Some(signal) = waiting_queue.pop_front() {
                    let _ = self.worker_channel.sender.send(signal);
                }
            }
        }

        return true;
    }

    fn run_queued_tasks(&self) {
        let mut wq = lock_safe(&self.waiting_queue);

        while !wq.is_empty() {
            if let Some(signal) = wq.pop_front() {
                let _ = self.worker_channel.sender.send(signal);
            }
        }
    }
}
