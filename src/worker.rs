use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::Signal;

#[derive(Debug)]
pub(crate) struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn spawn(receiver: Arc<Mutex<mpsc::Receiver<Signal>>>, id: Option<usize>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                println!("~w_{id:?}~");
                let signal = {
                    let receiver = receiver.lock().unwrap();
                    receiver.recv().unwrap()
                };
                match signal {
                    Signal::Job(task) => {
                        println!("worker() -> id={id:?} -> got task");
                        task();
                    }
                    _ => {
                        println!("worker() -> id={id:?} -> signalled to terminate -> exiting");
                        break;
                    }
                }
            }
        });

        Self {
            handle: Some(handle),
        }
    }

    pub(crate) fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
