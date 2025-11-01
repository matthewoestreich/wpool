use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use crate::{Signal, lock_safe};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        id: usize,
        worker_rx: Arc<Mutex<mpsc::Receiver<Signal>>>,
        worker_status_tx: mpsc::Sender<usize>,
    ) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                loop {
                    // Blocks until we either receive a signal, channel is closed, or timeout is hit.
                    let signal = match lock_safe(&worker_rx).recv_timeout(WORKER_IDLE_TIMEOUT) {
                        Ok(signal) => signal,
                        Err(RecvTimeoutError::Timeout) => {
                            let _ = worker_status_tx.send(id);
                            println!("[worker:{id}] timed out - terminating");
                            break;
                        }
                        Err(_) => break,
                    };

                    match signal {
                        Signal::NewTask(task_fn) => task_fn(),
                        // Let them know we are paused and block until we are resumed.
                        Signal::Pause(pauser) => pauser.pause_this_thread(),
                        Signal::Terminate => break,
                    }
                }
            })),
        }
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
