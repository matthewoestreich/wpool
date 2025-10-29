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
    pub(crate) fn spawn(receiver: Arc<Mutex<mpsc::Receiver<Signal>>>, id: Option<usize>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                println!("\tloop[worker[{id:?}]]");
                let signal = {
                    let receiver = receiver.lock().unwrap();
                    println!("[worker][{id:?}] about to block via recv");
                    receiver.recv().unwrap()
                };
                match signal {
                    Signal::Job(task) => {
                        println!("[worker][{id:?}] got task");
                        task();
                    }
                    Signal::Pause(pauser) => {
                        println!("[worker][{id:?}] got pause signal");
                        pauser.send_ack();
                        pauser.wait_for_unpause();
                    }
                    _ => {
                        println!("[worker][{id:?}] signalled to terminate -> exiting");
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
