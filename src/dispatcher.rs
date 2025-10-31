use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvError, TryRecvError},
    },
    thread,
};

use crate::{channel::ThreadedChannel, lock_safe, signal::Signal, worker::Worker};

pub(crate) struct Dispatcher {
    has_spawned: AtomicBool,
    pub(crate) handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) waiting: AtomicBool,
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
            waiting: AtomicBool::new(false),
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

        *lock_safe(&self.handle) = Some(thread::spawn(move || {
            loop {
                // As long as the waiting queue isn't empty, incoming signals (on task channel)
                // are put into the waiting queue and signals to run are taken from the waiting
                // queue. Once the waiting queue is empty, then go back to submitting incoming
                // signals directly to available workers.
                if !lock_safe(&this.waiting_queue).is_empty() {
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

                // Got a signal.
                let mut workers = lock_safe(&this.workers);
                if workers.len() < this.max_workers {
                    workers.push(Worker::spawn(Arc::clone(&worker_channel_receiver)));
                    // Non-blocking. Send signal to workers channel, break if worker channel is closed.
                    if this.worker_channel.sender.send(signal).is_err() {
                        break;
                    }
                } else {
                    // At max workers, put signal in waiting queue.
                    lock_safe(&this.waiting_queue).push_back(signal);
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if this.is_waiting() {
                this.run_queued_tasks();
            }

            let mut workers = lock_safe(&this.workers);

            // Kill all worker threads.
            for _ in 0..workers.len() {
                let _ = this.worker_channel.sender.send(Signal::Terminate);
            }

            // Block until all worker threads have ended.
            for mut w in workers.drain(..) {
                w.join();
            }
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
