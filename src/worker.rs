use std::{panic::catch_unwind, thread};

use crossbeam_channel::Receiver;

use crate::{PanicReport, Signal, state::SharedData};

/// Spawns a new thread that runs signal tasks. A worker thread will run
/// the signal task that was given to it during creation, then listen for
/// new tasks on the worker channel.
pub(crate) fn spawn(signal: Signal, worker_receiver: Receiver<Signal>, state: SharedData) {
    let worker_receiver_clone = worker_receiver.clone();
    let thread_state = state.clone();

    let handle = thread::spawn(move || {
        let mut signal_opt = Some(signal);

        while signal_opt.is_some() {
            match signal_opt.take().expect("is_some()") {
                Signal::Terminate => break,
                Signal::NewTask(task) => {
                    let task_result = catch_unwind(|| task.run());
                    if let Ok(pr) = PanicReport::try_from(task_result) {
                        thread_state.insert_panic_report(pr);
                    }
                }
            }
            signal_opt = match worker_receiver_clone.recv() {
                Ok(signal) => Some(signal),
                Err(_) => break,
            }
        }

        thread_state.handle_worker_terminating(thread::current().id());
    });

    state.insert_worker_handle(handle.thread().id(), handle);
}
