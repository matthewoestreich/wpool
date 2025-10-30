use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self},
    },
    thread,
};

use crate::{lock_safe, signal::Signal};

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(receiver: Arc<Mutex<mpsc::Receiver<Signal>>>) -> Self {
        let handle = Some(thread::spawn(move || {
            loop {
                // Blocks until we either receive a signal or channel is closed.
                let signal = match lock_safe(&receiver).recv() {
                    Ok(signal) => signal,
                    Err(_) => break,
                };

                match signal {
                    Signal::Task(task_fn) => task_fn(),
                    Signal::Pause(pauser) => {
                        pauser.send_ack(); // Let them know we are paused.
                        pauser.recv_resume(); // Blocks until we are resumed.
                    }
                    Signal::Terminate => break,
                }
            }
        }));

        Self { handle }
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
