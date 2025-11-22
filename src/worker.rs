use std::{panic::catch_unwind, thread};

use crate::{
    PanicReport, Signal,
    channel::{Receiver, Sender},
    state::Message,
};

/// Spawns a new thread that runs signal tasks. A worker thread will run
/// the signal task that was given to it during creation, then listen for
/// new tasks on the worker channel.
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
                    let task_result = catch_unwind(|| task.run());
                    if let Ok(pr) = PanicReport::try_from(task_result) {
                        let _ = thread_state_sender.send(Message::TaskPanic(pr));
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
