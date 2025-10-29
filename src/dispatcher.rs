use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvError, TrySendError},
    },
    thread,
};

use crate::{Signal, Task, ThreadedSyncChannel, worker::Worker};

pub(crate) struct Dispatcher {
    pub(crate) handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) max_workers: usize,
    pub(crate) worker_channel: ThreadedSyncChannel<Signal>,
    pub(crate) workers: Mutex<Vec<Worker>>,
    pub(crate) waiting_queue: Mutex<VecDeque<Task>>,
}

impl Dispatcher {
    pub(crate) fn spawn(
        max_workers: usize,
        task_receiver: mpsc::Receiver<Signal>,
        worker_channel: ThreadedSyncChannel<Signal>,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            handle: None.into(),
            max_workers,
            worker_channel,
            workers: Vec::new().into(),
            waiting_queue: VecDeque::new().into(),
        });

        let worker_rx = Arc::clone(&this.worker_channel.receiver);
        let dispatcher = Arc::clone(&this);

        *this.handle.lock().unwrap() = Some(thread::spawn(move || {
            loop {
                println!(".");
                // Blocks until we get a task or the task channel is closed.
                let task = match task_receiver.recv() {
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
                        // Pretty sure we should just continue here (vs break). The dispatcher shouldn't
                        // care if the signal is a task or terminate or pause or whatever..
                        continue;
                    }
                    Err(RecvError) => {
                        println!("dispatch() -> task channel closed, breaking.");
                        break;
                    }
                };

                match dispatcher.worker_channel.sender.try_send(Signal::Job(task)) {
                    Ok(_) => {
                        println!(
                            "dispatch() -> worker_tx.try_send() -> successfully sent task to worker queue"
                        );
                        let mut workers = dispatcher.workers.lock().unwrap();
                        if workers.len() < dispatcher.max_workers {
                            let worker = Worker::spawn(Arc::clone(&worker_rx), Some(workers.len()));
                            workers.push(worker);
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
                            dispatcher.waiting_queue.lock().unwrap().push_back(task);
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        println!(
                            "dispatch() -> worker_tx.try_send() -> disconnected, worker channel closed"
                        );
                        break;
                    }
                }
            }
            println!("dispatch() -> broken out of loop");
        }));

        this
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }
}
