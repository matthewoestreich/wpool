use std::{panic::catch_unwind, thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::{PanicReport, Signal, state::State};

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

            if t_state.shutdown_now() {
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

    match signal {
        Signal::NewTask(task) | Signal::NewTaskWithConfirmation(task, _) => {
            state.dec_waiting_queue_len();

            // If we were pending termination, remove it since we got work.
            if let Ok(mut pending_timeout) = state.pending_timeout()
                && pending_timeout
                    .as_ref()
                    .is_some_and(|&thread_id| thread_id == thread::current().id())
            {
                *pending_timeout = None;
            }

            // Run the actual task.
            let task_result = catch_unwind(|| task.run());
            if let Ok(panic_report) = PanicReport::try_from(task_result) {
                state.insert_panic_report(panic_report);
            }
        }
    }
}

fn handle_recv_timeout(state: &State, min_workers: usize) -> bool {
    // We want to hold the lock for the duration of this block.
    if let Ok(mut pending_timeout) = state.pending_timeout() {
        match pending_timeout.as_ref() {
            Some(&thread_id) => {
                if thread_id == thread::current().id() {
                    // Clear pending timeout.
                    *pending_timeout = None;
                    // Only kill worker if we are above min_workers.
                    if state.worker_count() > min_workers {
                        return false;
                    }
                }
            }
            None => {
                // Worker now pending timeout.
                *pending_timeout = Some(thread::current().id());
            }
        }
    }

    true
}
