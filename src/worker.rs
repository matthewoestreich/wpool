use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use crate::{Signal, safe_lock};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) enum WorkerStatus {
    Terminating(usize),
    Unavailable(usize),
    Available(usize),
}

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        id: usize,
        worker_rx: Arc<Mutex<mpsc::Receiver<Signal>>>,
        worker_status_tx: mpsc::Sender<WorkerStatus>,
    ) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                loop {
                    // Blocks until we either receive a signal, channel is closed, or timeout is hit.
                    let signal = match safe_lock(&worker_rx).recv_timeout(WORKER_IDLE_TIMEOUT) {
                        Ok(signal) => signal,
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(_) => break,
                    };

                    match signal {
                        Signal::NewTask(task) => task(),
                        Signal::Pause(pauser) => {
                            // Report to dispatcher that we are unavailable
                            let _ = worker_status_tx.send(WorkerStatus::Unavailable(id));
                            // Let them know we are paused - this will block until we are resumed.
                            pauser.pause_this_thread();
                            // Report to dispatcher that we are availablle again.
                            let _ = worker_status_tx.send(WorkerStatus::Available(id));
                            continue;
                        }
                    }
                }
                // If we are outside of the loop, it means we are exiting.
                // Let the dispatcher know we are terminating.
                let _ = worker_status_tx.send(WorkerStatus::Terminating(id));
            })),
        }
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
