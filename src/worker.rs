use std::{thread, time::Duration};

use crate::job::Signal;

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

fn log(s: &str) {
    // Orange 255, 157, 0
    // dark green 0, 102, 0
    crate::printlnc(s, colored::Color::TrueColor { r: 0, g: 102, b: 0 });
}

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
                            log(&format!("worker:{id} -> got task signal to run!"));
                            task.run();
                        }
                        Signal::Pause(pauser) => {
                            log(&format!("worker:{id} -> got pause signal"));
                            pauser.pause_this_thread();
                            log(&format!("    worker:{id} -> has resumed from pause!"));
                        }
                        Signal::Terminate => {
                            log("worker:{id} -> got terminate signal");
                            break;
                        }
                    }

                    maybe_signal = match worker_channel_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                        Ok(signal) => Some(signal),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            log(&format!("worker:{id} -> timed out, exiting."));
                            break;
                        }
                        Err(_) => {
                            log(&format!("worker:{id} -> worker channel closed, exiting."));
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
