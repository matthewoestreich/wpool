use std::{panic::catch_unwind, thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::{PanicReport, Signal, state::State};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// Spawns a new thread that runs signal tasks.
pub(crate) fn spawn(worker_receiver: Receiver<Signal>, state: State, min_workers: usize) {
    let thread_receiver = worker_receiver.clone();
    let thread_state = state.clone();

    let handle = thread::spawn(move || {
        loop {
            match thread_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                Ok(signal) => {
                    if !handle_signal(signal, &thread_state) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !handle_recv_timeout(&thread_state, min_workers) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };
        }

        thread_state.handle_worker_terminating(thread::current().id());
    });

    state.insert_worker_handle(handle);
}

fn handle_signal(signal: Signal, state: &State) -> bool {
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
            if let Ok(pr) = PanicReport::try_from(task_result) {
                state.insert_panic_report(pr);
            }
        }
    }

    // If shutdown_now is true, we want to return false.
    !state.shutdown_now()
}

fn handle_recv_timeout(state: &State, min_workers: usize) -> bool {
    // We want to hold the lock for the duration of this block.
    if let Ok(mut pending_timeout) = state.pending_timeout() {
        match pending_timeout.as_ref() {
            Some(&thread_id) => {
                if thread_id == thread::current().id() && state.worker_count() > min_workers {
                    // Clear pending timeout and kill worker as it timed out.
                    *pending_timeout = None;
                    return false;
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
