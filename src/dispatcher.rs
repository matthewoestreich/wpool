use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        mpsc::{RecvTimeoutError, TryRecvError},
    },
    thread::{self},
};

use crate::{
    Signal, Task, ThreadGuardian, WPoolStatus,
    channel::{Receiver, Sender, bounded},
    state,
    wpool::WORKER_IDLE_TIMEOUT,
};

/// The dispatcher thread is responsible for receiving signals on the tasks
/// channel, delegating those signals to workers or the waiting queue, spawning
/// workers, handling idle worker termination, and more.
pub(crate) fn spawn(
    min_workers: usize,
    max_workers: usize,
    task_receiver: Receiver<Signal>,
    state_sender: Sender<state::QueryFn>,
) -> thread::JoinHandle<()> {
    let worker_channel = bounded(0);
    thread::spawn(move || {
        let mut is_idle = false;
        let mut waiting_queue = VecDeque::new();

        loop {
            if !waiting_queue.is_empty() {
                if !process_waiting_queue(
                    &mut waiting_queue,
                    &state_sender,
                    &task_receiver,
                    worker_channel.sender_ref(),
                ) {
                    break;
                }
                continue;
            }

            // Get signal from task channel or timeout was hitt. Also handles killing idle workers.
            let signal = match task_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                Ok(signal) => signal,
                Err(RecvTimeoutError::Timeout) => {
                    if is_idle
                        && state::get_worker_count(&state_sender) > min_workers
                        && worker_channel.try_send(Signal::Terminate).is_ok()
                    {
                        state::decrement_worker_count(&state_sender);
                    }
                    is_idle = true;
                    continue;
                }
                Err(_) => break,
            };

            // Take ownership of confirmation from signal.
            // This is for when `pool.submit_confirm(...)` is called.
            let signal_confirmation = signal.take_confirm();

            // Process received signal by placing in wait queue or handing to worker.
            if state::get_worker_count(&state_sender) >= max_workers {
                waiting_queue.push_back(signal);
                state::set_waiting_queue_len(&state_sender, waiting_queue.len());
            } else {
                spawn_worker(
                    signal,
                    worker_channel.clone_receiver(),
                    state_sender.clone(),
                );
                state::increment_worker_count(&state_sender);
            }

            // If we have confirmation, drop it (which IS the confirmation).
            // [[ IMPORTANT ]] : we only want to send confirmation AFTER we
            // know the worker count has been updated.
            // This is for when `.submit_confirm(...)` is called.
            if let Some(confirmation) = signal_confirmation {
                confirmation.drop();
            }
            is_idle = false;
        }

        // If `stop_wait()` was called run tasks and waiting queue.
        if state::get_pool_status(&state_sender) == WPoolStatus::Stopped(true) {
            run_queued_tasks(
                &mut waiting_queue,
                &state_sender,
                worker_channel.sender_ref(),
            );
        }

        // Terminate workers as they become available.
        for _ in 0..state::get_worker_count(&state_sender) {
            let _ = worker_channel.send(Signal::Terminate); // Blocking.
            state::decrement_worker_count(&state_sender);
        }

        // Wait for all workers to finish working before exiting.
        state::join_all_worker_handles(&state_sender);
    })
}

/// Processes tasks within the waiting queue.
/// As long as the waiting queue isn't empty, incoming signals (on task channel)
/// are put into the waiting queue and signals to run are taken from the waiting
/// queue. Once the waiting queue is empty, then go back to submitting incoming
/// signals directly to available workers.
fn process_waiting_queue(
    waiting_queue: &mut VecDeque<Signal>,
    state_sender: &Sender<state::QueryFn>,
    task_receiver: &Receiver<Signal>,
    worker_sender: &Sender<Signal>,
) -> bool {
    match task_receiver.try_recv() {
        Ok(signal) => {
            waiting_queue.push_back(signal);
            state::set_waiting_queue_len(state_sender, waiting_queue.len());
        }
        Err(TryRecvError::Empty) => {
            if let Some(signal) = waiting_queue.front()
                && worker_sender.try_send(signal.clone()).is_ok()
            {
                // Only pop off (modify) waitiing queue once we know the
                // signal was successfully passed into the worker channel.
                waiting_queue.pop_front();
                state::set_waiting_queue_len(state_sender, waiting_queue.len());
            }
        }
        // Task channel closed.
        Err(_) => return false,
    };
    true
}

/// If `stop_wait()` is called, this function handles draining the wait queue
/// during shutdown.
fn run_queued_tasks(
    waiting_queue: &mut VecDeque<Signal>,
    state_sender: &Sender<state::QueryFn>,
    worker_sender: &Sender<Signal>,
) {
    while !waiting_queue.is_empty() {
        // Get a reference to element at the front of waiting queue, if exists.
        if let Some(signal) = waiting_queue.front()
            && worker_sender.try_send(signal.clone()).is_ok()
        {
            // Only pop off (modify) waitiing queue once we know the
            // signal was successfully passed into the worker channel.
            waiting_queue.pop_front();
            state::set_waiting_queue_len(state_sender, waiting_queue.len());
        }
    }
}

/// Spawns a new thread that runs signall tasks. A worker thread will run
/// the signal task that was given to it during creation, then listen for
/// new tasks on the worker channel. ThreadGuardian is responsible for
/// respawning a thread if one happens to panic.
fn spawn_worker(
    signal: Signal,
    worker_receiver: Receiver<Signal>,
    state_sender: Sender<state::QueryFn>,
) {
    let state_sender_clone = state_sender.clone();

    let handle = thread::spawn(move || {
        let tg = ThreadGuardian::new((
            Signal::NewTask(Task::noop(), Mutex::new(None).into()),
            worker_receiver.clone(),
            state_sender_clone,
        ));

        tg.on_panic(|(signal, worker_receiver, tg_state_sender)| {
            spawn_worker(signal, worker_receiver, tg_state_sender);
        });

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
    });

    state::insert_worker_handle(&state_sender, handle.thread().id(), handle);
}
