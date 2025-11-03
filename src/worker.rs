use std::{thread, time::Duration};

use crate::job::Signal;

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
        worker_channel_receiver: crossbeam_channel::Receiver<Signal>,
        worker_status_sender: crossbeam_channel::Sender<WorkerStatus>,
        initial_signal: Signal,
    ) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                let mut maybe_signal = Some(initial_signal);

                while maybe_signal.is_some() {
                    match maybe_signal.unwrap() {
                        Signal::NewTask(task) => {
                            println!("worker:{id} -> got task signal to run!");
                            task.run();
                        }
                        Signal::Pause(pauser) => {
                            println!("worker:{id} -> got pause signal");
                            pauser.pause_this_thread();
                            println!("    worker:{id} -> has resumed from pause!");
                        }
                        Signal::Terminate => {
                            println!("worker:{id} -> got terminate signal");
                            break;
                        }
                    }

                    maybe_signal = match worker_channel_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                        Ok(signal) => Some(signal),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            println!("worker:{id} -> timed out, exiting.");
                            break;
                        }
                        Err(_) => {
                            println!("worker:{id} -> worker channel closed, exiting.");
                            break;
                        }
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
