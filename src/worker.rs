use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    thread,
    time::Duration,
};

use crate::{job::Signal, safe_lock};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) enum WorkerStatus {
    Terminating(usize),
}

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        id: usize,
        worker_channel_receiver: Arc<Mutex<Receiver<Signal>>>,
        worker_status_sender: Sender<WorkerStatus>,
        initial_signal: Signal,
    ) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                let mut maybe_signal = Some(initial_signal);

                while maybe_signal.is_some() {
                    match maybe_signal.expect("'is_some()' was checked prior to this call") {
                        Signal::NewTask(task) => task.run(),
                        Signal::Pause(pauser) => pauser.pause_this_thread(),
                        Signal::Terminate => break,
                    }
                    maybe_signal = match safe_lock(&worker_channel_receiver)
                        .recv_timeout(WORKER_IDLE_TIMEOUT)
                    {
                        Ok(signal) => Some(signal),
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(_) => break,
                    };
                }

                let _ = worker_status_sender.send(WorkerStatus::Terminating(id));
            })),
        }
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
