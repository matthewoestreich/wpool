use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::signal::Signal;

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(receiver: Arc<Mutex<mpsc::Receiver<Signal>>>) -> Self {
        let handle = Some(thread::spawn(move || {
            loop {
                let signal = {
                    // We put the mutex lock in this block so it is dropped immediately after.
                    let receiver = receiver.lock().unwrap();
                    receiver.recv().unwrap()
                };
                match signal {
                    Signal::Job(task) => task(),
                    Signal::Pause(pauser) => {
                        pauser.send_ack();
                        pauser.wait_for_unpause();
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
