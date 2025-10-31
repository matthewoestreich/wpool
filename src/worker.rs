use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use crate::{lock_safe, signal::Signal};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct WorkerIDFactory {
    next_id: AtomicUsize,
}

impl WorkerIDFactory {
    pub(crate) fn new() -> Self {
        Self {
            next_id: AtomicUsize::new(0),
        }
    }

    pub(crate) fn next(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) enum WorkerStatus {
    Terminated(usize), // Terminated(worker_id)
}

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        id: usize,
        worker_receiver: Arc<Mutex<mpsc::Receiver<Signal>>>,
        worker_status_sender: mpsc::Sender<WorkerStatus>,
    ) -> Self {
        let mut this = Self { handle: None };

        let status_sender = worker_status_sender.clone();

        this.handle = Some(thread::spawn(move || {
            loop {
                // Blocks until we either receive a signal or channel is closed.
                let signal = match lock_safe(&worker_receiver).recv_timeout(WORKER_IDLE_TIMEOUT) {
                    Ok(signal) => signal,
                    Err(RecvTimeoutError::Timeout) => {
                        let _ = status_sender.send(WorkerStatus::Terminated(id));
                        println!(
                            "worker {id} has not done anything in worker_idle_timeout, killing worker {id}"
                        );
                        break;
                    }
                    Err(_) => break,
                };

                match signal {
                    Signal::Task(task_fn) => task_fn(),
                    // Let them know we are paused and block until we are resumed.
                    Signal::Pause(pauser) => pauser.pause_this_thread(),
                    Signal::Terminate => break,
                }
            }
        }));

        this
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
