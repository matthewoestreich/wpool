use std::{any::Any, backtrace::Backtrace, thread};

use crate::{
    PanicReport, Signal,
    channel::{Receiver, Sender},
    state::Message,
};

/// Spawns a new thread that runs signall tasks. A worker thread will run
/// the signal task that was given to it during creation, then listen for
/// new tasks on the worker channel. ThreadGuardian is responsible for
/// respawning a thread if one happens to panic.
pub(crate) fn spawn(
    signal: Signal,
    worker_receiver: Receiver<Signal>,
    state_sender: Sender<Message>,
) {
    let worker_receiver_clone = worker_receiver.clone();
    let thread_state_sender = state_sender.clone();

    let handle = thread::spawn(move || {
        let mut signal_opt = Some(signal);

        while signal_opt.is_some() {
            match signal_opt.take().expect("is_some()") {
                Signal::Terminate => break,
                Signal::NewTask(task, _) => {
                    let task_result = std::panic::catch_unwind(|| task.run());
                    if let Some(panic_report) = parse_task_result_errors(task_result) {
                        let _ = thread_state_sender.send(Message::TaskPanic(panic_report));
                    }
                }
            }
            signal_opt = match worker_receiver_clone.recv() {
                Ok(signal) => Some(signal),
                Err(_) => break,
            }
        }

        let _ = thread_state_sender.send(Message::WokerTerminating(thread::current().id()));
    });

    let _ = state_sender.send(Message::InsertWorker(handle.thread().id(), Some(handle)));
}

fn parse_task_result_errors(task_result: Result<(), Box<dyn Any + Send>>) -> Option<PanicReport> {
    if let Err(task_err) = task_result {
        let panic_report = PanicReport {
            thread_id: thread::current().id(),
            message: if let Some(s) = task_err.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = task_err.downcast_ref::<String>() {
                s.clone()
            } else {
                "-".to_string()
            },
            backtrace: Backtrace::force_capture().to_string(),
        };
        return Some(panic_report);
    }
    None
}
