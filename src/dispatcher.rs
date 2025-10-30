use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvError, TryRecvError},
    },
    thread,
};

use crate::{channel::ThreadedChannel, signal::Signal, worker::Worker};

pub(crate) struct Dispatcher {
    has_spawned: AtomicBool,
    pub(crate) handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) is_waiting: AtomicBool,
    pub(crate) max_workers: usize,
    pub(crate) waiting_queue: Mutex<VecDeque<Signal>>,
    pub(crate) worker_channel: ThreadedChannel<Signal>,
    pub(crate) workers: Mutex<Vec<Worker>>,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize, worker_channel: ThreadedChannel<Signal>) -> Self {
        Self {
            has_spawned: AtomicBool::new(false),
            handle: None.into(),
            is_waiting: AtomicBool::new(false),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel,
            workers: Vec::new().into(),
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
                // As long as the waiting queue isn't empty, incoming signals (on task channel)
                // are put into the waiting queue and signals to run are taken from the waiting
                // queue. Once the waiting queue is empty, then go back to submitting incoming
                // signals directly to available workers.
                let mut waiting_queue = this.waiting_queue.lock().unwrap();
                if !waiting_queue.is_empty() {
                    if !this.process_waiting_queue(&mut waiting_queue, &task_receiver) {
                        break;
                    }
                    continue;
                }

                // Drop the lock so we don't deadlock or hold it unnecessarily.
                drop(waiting_queue);

                // Blocks until we get a task or the task channel is closed.
                let signal = match task_receiver.recv() {
                    Ok(signal) => signal,
                    Err(RecvError) => break,
                };

                // Got a signal.
                let mut workers = this.workers.lock().unwrap();
                if workers.len() < this.max_workers {
                    workers.push(Worker::spawn(Arc::clone(&worker_channel_receiver)));
                    // Non-blocking. Send signal to workers channel, break if worker channel is closed.
                    if this.worker_channel.sender.send(signal).is_err() {
                        break;
                    }
                } else {
                    this.waiting_queue.lock().unwrap().push_back(signal);
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

    fn process_waiting_queue(
        &self,
        waiting_queue: &mut VecDeque<Signal>,
        task_receiver: &mpsc::Receiver<Signal>,
    ) -> bool {
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
