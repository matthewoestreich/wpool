use std::sync::{
    Arc, Mutex, Once,
    atomic::{AtomicBool, Ordering},
    mpsc::{self},
};

use crate::{Signal, ThreadedSyncChannel, dispatcher::Dispatcher};

pub struct WPool {
    dispatcher: Arc<Dispatcher>,
    max_workers: usize,
    is_stopped: AtomicBool,
    stop_once: Once,
    task_sender: Option<mpsc::Sender<Signal>>,
}

impl WPool {
    pub fn new(max_workers: usize) -> Self {
        let (task_tx, task_rx) = mpsc::channel();
        let (worker_tx, worker_rx) = mpsc::sync_channel::<Signal>(max_workers);

        let worker_channel = ThreadedSyncChannel {
            sender: worker_tx,
            receiver: Arc::new(Mutex::new(worker_rx)),
        };

        Self {
            task_sender: Some(task_tx),
            max_workers,
            dispatcher: Dispatcher::spawn(max_workers, task_rx, worker_channel),
            is_stopped: AtomicBool::new(false),
            stop_once: Once::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.max_workers
    }

    pub fn submit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped.load(Ordering::Relaxed) {
            println!("submit(f) -> tried to submit to a stopped pool!");
            return;
        }
        if let Some(tx) = &self.task_sender {
            let _ = tx.send(Signal::Job(Box::new(f)));
        }
    }

    // Enqueues the given function and waits for it to be executed.
    pub fn submit_wait<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        self.submit(move || {
            f();
            let _ = done_tx.send(());
        });
        done_rx.recv().unwrap(); // blocks until complete
    }

    // Stop and wait for all current + waiting_queue tasks.
    pub fn stop_wait(&mut self) {
        self.shutdown(true);
    }

    fn shutdown(&mut self, wait: bool) {
        self.stop_once.call_once(|| {
            self.is_stopped.store(true, Ordering::Relaxed);
            self.dispatcher.is_wait.store(wait, Ordering::Relaxed);

            println!("[shutdown] closing task_queue");
            if let Some(task_queue) = self.task_sender.take() {
                drop(task_queue);
            }

            // Join dispatcher thread. Blocks until dispatcher thread has ended.
            self.dispatcher.join();

            let mut workers = self.dispatcher.workers.lock().unwrap();
            println!("[shutdown] sending terminate signal to all workers");
            for _ in 0..workers.len() {
                let _ = self
                    .dispatcher
                    .worker_channel
                    .sender
                    .send(Signal::Terminate);
            }

            println!("[shutdown] joining worker threads");
            for mut w in workers.drain(..) {
                w.join();
            }

            println!("[shutdown] done.");
        });
    }
}
