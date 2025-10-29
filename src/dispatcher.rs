use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvError, TryRecvError, TrySendError},
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
    pub(crate) is_wait: AtomicBool,
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
            is_wait: AtomicBool::new(false),
        });

        let worker_rx = Arc::clone(&this.worker_channel.receiver);
        let dispatcher = Arc::clone(&this);

        *this.handle.lock().unwrap() = Some(thread::spawn(move || {
            loop {
                println!(".");
                // If waiting queue has something in it, process it.
                if !dispatcher.waiting_queue.lock().unwrap().is_empty() {
                    println!("[dispatcher] waiting_queue has items!");
                    if !dispatcher.process_waiting_queue(&task_receiver) {
                        break;
                    }
                    continue;
                }

                // Blocks until we get a task or the task channel is closed.
                let task = match task_receiver.recv() {
                    Ok(Signal::Job(task)) => {
                        println!("[dispatcher][task_recvr.recv()] Ok(signal) -> signal is a task");
                        task
                    }
                    Ok(_) => {
                        println!(
                            "[dispatcher][task_recvr.recv()] Ok(signal) -> signal is NOT a task"
                        );
                        // Pretty sure we should just continue here (vs break). The dispatcher shouldn't
                        // care if the signal is a task or terminate or pause or whatever..
                        continue;
                    }
                    Err(RecvError) => {
                        println!("[dispatcher] task channel closed, breaking.");
                        break;
                    }
                };

                match dispatcher.worker_channel.sender.try_send(Signal::Job(task)) {
                    Ok(_) => {
                        println!(
                            "[dispatcher][worker_chan.send()] successfully sent task to worker queue"
                        );
                        let mut workers = dispatcher.workers.lock().unwrap();
                        if workers.len() < dispatcher.max_workers {
                            let worker = Worker::spawn(Arc::clone(&worker_rx), Some(workers.len()));
                            workers.push(worker);
                            println!(
                                "[dispatcher][worker_chan.send()] all workers busy, but not at max workers, so spawning new worker"
                            );
                        }
                    }
                    Err(TrySendError::Full(signal)) => {
                        if let Signal::Job(task) = signal {
                            println!(
                                "[dispatcher][worker_chan.send()] at max workers, adding task to waiting queue"
                            );
                            // Add to waiting_queue
                            dispatcher.waiting_queue.lock().unwrap().push_back(task);
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        println!(
                            "[dispatcher][worker_chan.send()] disconnected, worker channel closed"
                        );
                        break;
                    }
                }
            }

            println!("[dispatcher] broken out of loop");

            if dispatcher.is_wait.load(Ordering::Relaxed) {
                dispatcher.run_queued_tasks();
            }
        }));

        this
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }

    fn process_waiting_queue(&self, task_receiver: &mpsc::Receiver<Signal>) -> bool {
        let mut waiting_queue = self.waiting_queue.lock().unwrap();

        match task_receiver.try_recv() {
            Ok(signal) => {
                println!("[dispatcher][proc_wait_que] got signal on task channel");
                if let Signal::Job(task) = signal {
                    println!(
                        "    [dispatcher][proc_wait_que][recvd sig on task chan] got Job(task) add to wait_que'"
                    );
                    waiting_queue.push_back(task);
                }
            }
            Err(TryRecvError::Disconnected) => {
                println!("[dispatcher][proc_wait_que] task channel closed, returning false");
                return false;
            }
            Err(TryRecvError::Empty) => {
                println!(
                    "[dispatcher][proc_wait_que] task channel empty, adding task from waiting_queue to worker_channel"
                );
                if let Some(task_from_waiting) = waiting_queue.pop_front() {
                    let _ = self
                        .worker_channel
                        .sender
                        .send(Signal::Job(task_from_waiting));
                }
            }
        }

        println!("[dispatcher][proc_wait_que] done, returning true");
        return true;
    }

    fn run_queued_tasks(&self) {
        println!("[dispatcher][run_q'd_tasks] starting");
        let mut waiting_queue = self.waiting_queue.lock().unwrap();
        while !waiting_queue.is_empty() {
            if let Some(task) = waiting_queue.pop_front() {
                println!("[dispatcher][run_q'd_tasks] got task from wait_que, send to wrkr_chan");
                let _ = self.worker_channel.sender.send(Signal::Job(task));
            }
        }
    }
}
