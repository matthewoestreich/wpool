use std::{panic::catch_unwind, thread};

use crossbeam_channel::Receiver;

use crate::{PanicReport, Signal, state::State};

/// Spawns a new thread that runs signal tasks. A worker thread will run
/// the signal task that was given to it during creation, then listen for
/// new tasks on the worker channel.
pub(crate) fn spawn(signal: Signal, worker_receiver: Receiver<Signal>, state: State) {
    let worker_receiver_clone = worker_receiver.clone();
    let thread_state = state.clone();

    let handle = thread::spawn(move || {
        let mut signal_opt = Some(signal);

        while signal_opt.is_some() {
            if let Some(confirmation) = signal_opt
                .as_ref()
                .expect("called is_some()")
                .take_confirm()
            {
                drop(confirmation);
            }

            match signal_opt.take().expect("is_some()") {
                Signal::Terminate => {
                    break;
                }
                Signal::NewTask(task) | Signal::NewTaskWithConfirmation(task, _) => {
                    thread_state.dec_waiting_queue_len();
                    let task_result = catch_unwind(|| task.run());
                    if let Ok(pr) = PanicReport::try_from(task_result) {
                        thread_state.insert_panic_report(pr);
                    }
                }
            }

            if thread_state.shutdown_now() {
                break;
            }

            signal_opt = match worker_receiver_clone.recv() {
                Ok(signal) => Some(signal),
                Err(_) => break,
            };
        }

        thread_state.handle_worker_terminating(thread::current().id());
    });

    state.insert_worker_handle(handle.thread().id(), handle);
}
