use std::{thread, time::Duration};

use crate::Signal;

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
    ) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                loop {
                    // Blocks until we either receive a signal, channel is closed, or timeout is hit.
                    let signal = match worker_channel_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                        Ok(signal) => signal,
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => break,
                        Err(_) => break,
                    };

                    match signal {
                        Signal::NewTask(task) => task(),
                        Signal::Pause(pauser) => {
                            println!("[worker:{id}][PAUSING] got pause signal");
                            // Let them know we are paused - this will block until we are resumed.
                            pauser.pause_this_thread();
                            println!("[worker:{id}][RESUMING] got pause signal");
                        }
                    }
                }
                // If we are outside of the loop, it means we are exiting.
                // Let the dispatcher know we are terminating.
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
