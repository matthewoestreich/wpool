use std::{
    collections::VecDeque,
    sync::{Mutex, mpsc::TryRecvError},
    thread::{self},
};

use crate::{
    Signal, Task, ThreadGuardian, WPoolStatus,
    channel::{Channel, Receiver, Sender, bounded},
    state::{QueryFn, StateOps},
};

pub(crate) trait DispatchStrategy {
    fn task_receiver(&self) -> &Receiver<Signal>;
    fn is_waiting_queue_empty(&self) -> bool;
    fn process_waiting_queue(&mut self) -> bool;
    fn on_signal(&mut self, signal: Signal);
    fn on_worker_timeout(&mut self);
    fn on_shutdown(&mut self);
}

pub(crate) struct Dispatcher {
    min_workers: usize,
    max_workers: usize,
    waiting_queue: VecDeque<Signal>,
    is_idle: bool,
    state: StateOps,
    task_receiver: Receiver<Signal>,
    worker_channel: Channel<Signal>,
}

impl Dispatcher {
    pub(crate) fn new(
        min_workers: usize,
        max_workers: usize,
        task_receiver: Receiver<Signal>,
        state: StateOps,
    ) -> Self {
        Self {
            min_workers,
            max_workers,
            waiting_queue: VecDeque::new(),
            is_idle: false,
            state,
            task_receiver,
            worker_channel: bounded(0),
        }
    }

    /// Spawns a new thread that runs signall tasks. A worker thread will run
    /// the signal task that was given to it during creation, then listen for
    /// new tasks on the worker channel. ThreadGuardian is responsible for
    /// respawning a thread if one happens to panic.
    pub(crate) fn spawn_worker(
        signal: Signal,
        worker_receiver: &Receiver<Signal>,
        state_sender: Sender<QueryFn>,
    ) {
        let worker_receiver_clone = worker_receiver.clone();
        let tg_worker_receiver = worker_receiver.clone();
        let tg_state_sender = state_sender.clone();

        let handle = thread::spawn(move || {
            let tg = ThreadGuardian::new((
                Signal::NewTask(Task::noop(), Mutex::new(None).into()),
                tg_worker_receiver,
                tg_state_sender,
            ));

            tg.on_panic(move |(signal, worker, state)| {
                Self::spawn_worker(signal, &worker, state);
            });

            let mut signal_opt = Some(signal);

            while signal_opt.is_some() {
                match signal_opt.take().expect("is_some()") {
                    Signal::Terminate => break,
                    Signal::NewTask(task, _) => task.run(),
                }
                signal_opt = match worker_receiver_clone.recv() {
                    Ok(signal) => Some(signal),
                    Err(_) => break,
                }
            }
        });

        let _ = state_sender.send(Box::new(move |state| {
            state
                .worker_handles
                .insert(handle.thread().id(), Some(handle));
        }));
    }
}

impl DispatchStrategy for Dispatcher {
    fn is_waiting_queue_empty(&self) -> bool {
        self.waiting_queue.is_empty()
    }

    fn task_receiver(&self) -> &Receiver<Signal> {
        &self.task_receiver
    }

    fn process_waiting_queue(&mut self) -> bool {
        match self.task_receiver.try_recv() {
            Ok(signal) => {
                self.waiting_queue.push_back(signal);
                self.state.set_waiting_queue_len(self.waiting_queue.len());
            }
            Err(TryRecvError::Empty) => {
                if let Some(signal) = self.waiting_queue.front()
                    && self.worker_channel.try_send(signal.clone()).is_ok()
                {
                    // Only pop off (modify) waitiing queue once we know the
                    // signal was successfully passed into the worker channel.
                    self.waiting_queue.pop_front();
                    self.state.set_waiting_queue_len(self.waiting_queue.len());
                }
            }
            // Task channel closed.
            Err(_) => return false,
        };
        true
    }

    fn on_signal(&mut self, signal: Signal) {
        // Take ownership of confirmation from signal.
        // This is for when `pool.submit_confirm(...)` is called.
        let signal_confirmation = signal.take_confirm();

        // Process received signal by placing in wait queue or handing to worker.
        if self.state.worker_count() >= self.max_workers {
            self.waiting_queue.push_back(signal);
            self.state.set_waiting_queue_len(self.waiting_queue.len());
        } else {
            Self::spawn_worker(
                signal,
                &self.worker_channel.clone_receiver(),
                self.state.clone_sender(),
            );
            self.state.inc_worker_count();
        }

        // [[ IMPORTANT ]] : we only want to send confirmation AFTER we know the worker
        // count has been updated. This is for when `.submit_confirm(...)` is called.
        if let Some(confirmation) = signal_confirmation {
            confirmation.drop();
        }

        self.is_idle = false;
    }

    fn on_worker_timeout(&mut self) {
        if self.is_idle
            && self.state.worker_count() > self.min_workers
            && self.worker_channel.try_send(Signal::Terminate).is_ok()
        {
            self.state.dec_worker_count();
        }
        self.is_idle = true;
    }

    fn on_shutdown(&mut self) {
        // If `stop_wait()` was called run tasks and waiting queue.
        if self.state.pool_status() == WPoolStatus::Stopped(true) {
            while !self.waiting_queue.is_empty() {
                // Get a reference to element at the front of waiting queue, if exists.
                if let Some(signal) = self.waiting_queue.front()
                    && self.worker_channel.try_send(signal.clone()).is_ok()
                {
                    // Only pop off (modify) waitiing queue once we know the
                    // signal was successfully passed into the worker channel.
                    self.waiting_queue.pop_front();
                    self.state.set_waiting_queue_len(self.waiting_queue.len());
                }
            }
        }

        // Terminate workers as they become available.
        for _ in 0..self.state.worker_count() {
            let _ = self.worker_channel.send(Signal::Terminate); // Blocking.
            self.state.dec_worker_count();
        }

        // Wait for all workers to finish working before exiting.
        self.state.join_worker_handles();
    }
}
