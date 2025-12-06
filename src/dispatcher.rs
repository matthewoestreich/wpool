use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError, TrySendError};
use std::{collections::VecDeque, thread};

use crate::{Channel, Signal, WPoolStatus, state::State, worker, wpool::WORKER_IDLE_TIMEOUT};

/******************** Dispatcher *************************************/

pub(crate) struct Dispatcher<S>
where
    S: DispatchStrategy + Send + 'static,
{
    strategy: S,
}

impl<S> Dispatcher<S>
where
    S: DispatchStrategy + Send + 'static,
{
    pub(crate) fn new(strat: S) -> Self {
        Self { strategy: strat }
    }

    pub(crate) fn spawn(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut strategy = self.strategy;

            loop {
                if !strategy.is_waiting_queue_empty() {
                    if !strategy.process_waiting_queue() {
                        break;
                    }
                    continue;
                }

                match strategy.task_receiver().recv_timeout(WORKER_IDLE_TIMEOUT) {
                    Ok(signal) => strategy.on_signal(signal),
                    Err(RecvTimeoutError::Timeout) => strategy.on_worker_timeout(),
                    Err(_) => break,
                }
            }

            strategy.on_shutdown();
        })
    }
}

/******************** DispatcherStrategy *****************************/

pub(crate) trait DispatchStrategy {
    fn task_receiver(&self) -> &Receiver<Signal>;
    fn is_waiting_queue_empty(&self) -> bool;
    fn process_waiting_queue(&mut self) -> bool;
    fn on_signal(&mut self, signal: Signal);
    fn on_worker_timeout(&mut self);
    fn on_shutdown(&mut self);
}

/******************** DefaultDispatchStrategy ************************/

pub(crate) struct DefaultDispatchStrategy {
    min_workers: usize,
    max_workers: usize,
    waiting_queue: VecDeque<Signal>,
    is_idle: bool,
    state: State,
    task_receiver: Receiver<Signal>,
    worker_channel: Channel<Signal>,
}

impl DefaultDispatchStrategy {
    pub(crate) fn new(
        min_workers: usize,
        max_workers: usize,
        task_receiver: Receiver<Signal>,
        state: State,
    ) -> Self {
        Self {
            min_workers,
            max_workers,
            waiting_queue: VecDeque::new(),
            is_idle: false,
            state,
            task_receiver,
            worker_channel: Channel::new_bounded(0),
        }
    }
}

impl DispatchStrategy for DefaultDispatchStrategy {
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
                if let Some(signal) = self.waiting_queue.pop_front() {
                    if let Err(TrySendError::Full(s) | TrySendError::Disconnected(s)) =
                        self.worker_channel.sender.try_send(signal)
                    {
                        self.waiting_queue.push_front(s);
                    }
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
            worker::spawn(
                signal,
                self.worker_channel.receiver.clone(),
                self.state.clone(),
            );
            self.state.inc_worker_count();
        }

        // [[ IMPORTANT ]] : we only want to send confirmation AFTER we know the worker
        // count has been updated. This is for when `.submit_confirm(...)` is called.
        if let Some(confirmation) = signal_confirmation {
            drop(confirmation);
        }

        self.is_idle = false;
    }

    fn on_worker_timeout(&mut self) {
        if self.is_idle
            && self.state.worker_count() > self.min_workers
            && self
                .worker_channel
                .sender
                .try_send(Signal::Terminate)
                .is_ok()
        {
            self.state.dec_worker_count();
        }
        self.is_idle = true;
    }

    fn on_shutdown(&mut self) {
        // If `stop_wait()` was called run tasks and waiting queue.
        if self.state.pool_status() == WPoolStatus::Stopped(true) {
            while !self.waiting_queue.is_empty() {
                if let Some(signal) = self.waiting_queue.pop_front() {
                    if let Err(TrySendError::Full(s) | TrySendError::Disconnected(s)) =
                        self.worker_channel.sender.try_send(signal)
                    {
                        self.waiting_queue.push_front(s)
                    }
                    self.state.set_waiting_queue_len(self.waiting_queue.len());
                }
            }
        }

        // Terminate workers as they become available.
        for _ in 0..self.state.worker_count() {
            let _ = self.worker_channel.sender.send(Signal::Terminate); // Blocking.
            self.state.dec_worker_count();
        }

        // Wait for all workers to finish working before exiting.
        self.state.join_worker_handles();
    }
}
