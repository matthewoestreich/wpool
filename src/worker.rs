use std::{panic::catch_unwind, thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::{PanicReport, Signal, WPoolStatus, state::State};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// Spawns a new thread that runs signal tasks.
pub(crate) fn spawn(task_receiver: Receiver<Signal>, state: State, min_workers: usize) {
    let t_receiver = task_receiver.clone();
    let t_state = state.clone();

    let handle = thread::spawn(move || {
        loop {
            match t_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                Ok(signal) => handle_signal(signal, &t_state),
                Err(RecvTimeoutError::Timeout) => {
                    if !handle_recv_timeout(&t_state, min_workers) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };

            // `.stop()` was called on the pool, which means do not drain queue, shutdown immediately.
            if t_state.pool_status() == (WPoolStatus::Stopped { now: true }) {
                break;
            }
        }

        t_state.join_worker(thread::current().id());
    });

    state.insert_worker_handle(handle);
}

fn handle_signal(signal: Signal, state: &State) {
    // Confirm signal if needed.
    if let Some(confirmation) = signal.take_confirm() {
        drop(confirmation);
    }

    let task = match signal {
        Signal::Task(t) | Signal::TaskWithConfirmation(t, _) => t,
    };

    state.dec_waiting_queue_len();

    // If we were pending timeout, clear it bc we got work.
    if let Ok(mut pending) = state.pending_timeout()
        && pending
            .as_ref()
            .is_some_and(|&thread_id| thread_id == thread::current().id())
    {
        *pending = None;
    }

    // Run the actuall task.
    let task_result = catch_unwind(|| task.run());
    if let Ok(panic_report) = PanicReport::try_from(task_result) {
        state.insert_panic_report(panic_report);
    }
}

// Return false to break out of the worker loop.
fn handle_recv_timeout(state: &State, min_workers: usize) -> bool {
    // We want to hold the lock for the duration of this block.
    let thread_id = thread::current().id();

    let Ok(mut pending) = state.pending_timeout() else {
        return true;
    };

    match pending.as_ref() {
        Some(&id) => {
            if id == thread_id {
                *pending = None;
                if state.worker_count() > min_workers {
                    return false;
                }
            }
        }
        None => {
            *pending = Some(thread_id);
        }
    }

    true
}
