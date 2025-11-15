use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        mpsc::{RecvTimeoutError, TryRecvError},
    },
    thread,
};

use crate::{
    Action, AsWPoolStatus, Signal, State, Task, ThreadGuardian, WPoolStatus, WaitGroup,
    channel::{Receiver, Sender, bounded},
    wpool::WORKER_IDLE_TIMEOUT,
};

pub(crate) struct Dispatcher {
    max_workers: usize,
    min_workers: usize,
    task_receiver: Receiver<Signal>,
}

impl Dispatcher {
    pub(crate) fn new(
        max_workers: usize,
        min_workers: usize,
        task_receiver: Receiver<Signal>,
    ) -> Self {
        Self {
            max_workers,
            min_workers,
            task_receiver,
        }
    }

    pub(crate) fn spawn(&self, state_sender: Sender<State>) -> thread::JoinHandle<()> {
        let max_workers = self.max_workers;
        let min_workers = self.min_workers;
        let task_receiver = self.task_receiver.clone();
        let worker_channel = bounded(0);

        thread::spawn(move || {
            let mut is_idle = false;
            let mut waiting_queue = VecDeque::new();
            let wait_group = WaitGroup::new();

            loop {
                // See `process_waiting_queue` comments for more info.
                if !waiting_queue.is_empty() {
                    if !Self::process_waiting_queue(
                        &mut waiting_queue,
                        &state_sender,
                        &task_receiver,
                        worker_channel.sender_ref(),
                    ) {
                        break;
                    }
                    continue;
                }

                // Get signal from task channel, handles killing idle workers.
                let signal = match task_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                    Ok(signal) => signal,
                    Err(RecvTimeoutError::Timeout) => {
                        if is_idle
                            && get_state!(WorkerCount, state_sender) > min_workers // Keep min workers alive.
                            && worker_channel.try_send(Signal::Terminate).is_ok()
                        {
                            set_state!(WorkerCount, Action::Decrement, state_sender);
                        }
                        is_idle = true;
                        // No signal, just loop.
                        continue;
                    }
                    Err(_) => break,
                };

                // Got a signal. Process it by placing in wait queue or handing to worker.
                if get_state!(WorkerCount, state_sender) >= max_workers {
                    signal.confirm_submit();
                    waiting_queue.push_back(signal);
                    set_state!(
                        WaitingQueueLength,
                        Action::Store(waiting_queue.len()),
                        state_sender
                    );
                } else {
                    wait_group.add(1);
                    Self::spawn_worker(signal, wait_group.clone(), worker_channel.clone_receiver());
                    set_state!(WorkerCount, Action::Increment, state_sender);
                }
                is_idle = false;
            }

            // If `stop_wait()` was called run tasks and waiting queue.
            if get_state!(PoolStatus, state_sender).as_wpool_status() == WPoolStatus::Stopped(true)
            {
                Self::run_queued_tasks(
                    &mut waiting_queue,
                    &state_sender,
                    worker_channel.sender_ref(),
                );
            }

            // Terminate workers as they become available.
            for _ in 0..get_state!(WorkerCount, state_sender) {
                let _ = worker_channel.send(Signal::Terminate); // Blocking.
                set_state!(WorkerCount, Action::Decrement, state_sender);
            }

            wait_group.wait();
        })
    }

    /// Processes tasks within the waiting queue.
    /// As long as the waiting queue isn't empty, incoming signals (on task channel)
    /// are put into the waiting queue and signals to run are taken from the waiting
    /// queue. Once the waiting queue is empty, then go back to submitting incoming
    /// signals directly to available workers.
    fn process_waiting_queue(
        waiting_queue: &mut VecDeque<Signal>,
        state_manager: &Sender<State>,
        task_receiver: &Receiver<Signal>,
        worker_sender: &Sender<Signal>,
    ) -> bool {
        match task_receiver.try_recv() {
            Ok(signal) => {
                waiting_queue.push_back(signal);
                set_state!(
                    WaitingQueueLength,
                    Action::Store(waiting_queue.len()),
                    state_manager
                );
            }
            Err(TryRecvError::Empty) => {
                if let Some(signal) = waiting_queue.front()
                    && let Ok(_) = worker_sender.try_send(signal.clone())
                {
                    // Only pop off (modify) waitiing queue once we know the
                    // signal was successfully passed into the worker channel.
                    waiting_queue.pop_front();
                    set_state!(
                        WaitingQueueLength,
                        Action::Store(waiting_queue.len()),
                        state_manager
                    );
                }
            }
            // Task channel closed.
            Err(_) => return false,
        };
        true
    }

    /// Essentially drains the wait_queue.
    fn run_queued_tasks(
        waiting_queue: &mut VecDeque<Signal>,
        state_manager: &Sender<State>,
        worker_sender: &Sender<Signal>,
    ) {
        while !waiting_queue.is_empty() {
            // Get a **reference** to the element at the front of waiting queue, if exists.
            if let Some(signal) = waiting_queue.front()
                && let Ok(_) = worker_sender.try_send(signal.clone())
            {
                // Only pop off (modify) waitiing queue once we know the
                // signal was successfully passed into the worker channel.
                waiting_queue.pop_front();
                set_state!(
                    WaitingQueueLength,
                    Action::Store(waiting_queue.len()),
                    state_manager
                );
            }
        }
    }

    /// `spawn_worker` spawns a new thread that acts as a worker. Unless the pool using `min_workers`,
    /// a worker will timeout after an entire cycle of being idle. The idle timeout cycle is ~4 seconds.
    fn spawn_worker(signal: Signal, wait_group: WaitGroup, worker_receiver: Receiver<Signal>) {
        thread::spawn(move || {
            let tg = ThreadGuardian::new((
                Signal::NewTask(Task::noop(), Mutex::new(None).into()),
                wait_group.clone(),
                worker_receiver.clone(),
            ));
            tg.on_panic(|(signal, wait_group, worker_receiver)| {
                Self::spawn_worker(signal, wait_group, worker_receiver);
            });

            signal.confirm_submit();
            let mut signal_opt = Some(signal);

            while signal_opt.is_some() {
                match signal_opt.take().expect("is_some()") {
                    Signal::Terminate => break,
                    Signal::NewTask(task, _) => task.run(),
                }
                signal_opt = match worker_receiver.recv() {
                    Ok(signal) => Some(signal),
                    Err(_) => break,
                }
            }

            wait_group.done();
        });
    }
}
