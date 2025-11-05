use std::{thread, time::Duration};

use crate::{
    channel::{Receiver, Sender},
    job::Signal,
};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) enum WorkerStatus {
    Terminating(usize),
}

#[derive(Debug)]
pub(crate) struct Worker {
    pub(crate) id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(
        id: usize,
        signal: Signal,
        worker_channel_receiver: Receiver<Signal>,
        worker_status_sender: Sender<WorkerStatus>,
    ) -> Self {
        Self {
            id,
            handle: Some(thread::spawn(move || {
                let mut maybe_signal = Some(signal);

                while maybe_signal.is_some() {
                    match maybe_signal.take().expect("is_some()") {
                        Signal::NewTask(task) => task.run(),
                        Signal::Pause(pauser) => pauser.pause_this_thread(),
                        Signal::Terminate => break,
                    }
                    maybe_signal = match worker_channel_receiver.recv() {
                        Ok(signal) => Some(signal),
                        Err(_) => break,
                    }
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
