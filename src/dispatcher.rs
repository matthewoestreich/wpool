use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvError, TryRecvError, TrySendError},
    },
    thread,
};

use crate::{channel::ThreadedSyncChannel, signal::Signal, worker::Worker};

pub(crate) struct Dispatcher {
    pub(crate) handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) max_workers: usize,
    pub(crate) worker_channel: ThreadedSyncChannel<Signal>,
    pub(crate) workers: Mutex<Vec<Worker>>,
    pub(crate) waiting_queue: Mutex<VecDeque<Signal>>,
    pub(crate) is_waiting: AtomicBool,
    has_spawned: AtomicBool,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize, worker_channel: ThreadedSyncChannel<Signal>) -> Self {
        Self {
            handle: None.into(),
            max_workers,
            worker_channel,
            workers: Vec::new().into(),
            waiting_queue: VecDeque::new().into(),
            is_waiting: AtomicBool::new(false),
            has_spawned: AtomicBool::new(false),
        }
    }

    pub(crate) fn spawn(self: Arc<Self>, task_receiver: mpsc::Receiver<Signal>) -> Arc<Self> {
        if self.has_spawned.swap(true, Ordering::Relaxed) {
            return self;
        }

        let worker_channel_receiver = Arc::clone(&self.worker_channel.receiver);
        let this = Arc::clone(&self);

        *self.handle.lock().unwrap() = Some(thread::spawn(move || {
            loop {
                if !this.waiting_queue.lock().unwrap().is_empty() {
                    if !this.process_waiting_queue(&task_receiver) {
                        break;
                    }
                    continue;
                }

                // Blocks until we get a task or the task channel is closed.
                let signal = match task_receiver.recv() {
                    Ok(signal) => signal,
                    Err(RecvError) => break,
                };

                match this.worker_channel.sender.try_send(signal) {
                    Ok(_) => {
                        let mut workers = this.workers.lock().unwrap();
                        if workers.len() < this.max_workers {
                            workers.push(Worker::spawn(Arc::clone(&worker_channel_receiver)));
                        }
                    }
                    Err(TrySendError::Full(signal)) => {
                        this.waiting_queue.lock().unwrap().push_back(signal);
                    }
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }

            if this.is_waiting.load(Ordering::Relaxed) {
                this.run_queued_tasks();
            }
        }));

        self
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }

    fn process_waiting_queue(&self, task_receiver: &mpsc::Receiver<Signal>) -> bool {
        let mut waiting_queue = self.waiting_queue.lock().unwrap();
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
        let mut wq = self.waiting_queue.lock().unwrap();
        while !wq.is_empty() {
            if let Some(signal) = wq.pop_front() {
                let _ = self.worker_channel.sender.send(signal);
            }
        }
    }
}
