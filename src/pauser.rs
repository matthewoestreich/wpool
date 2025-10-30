use std::sync::Arc;

use crate::{channel::ThreadedChannel, lock_safe};

// Pauser is a 'quality-of-life' wrapper to simplify thread sync between
// a controller thread and one (or many) non-controller (worker) threads.
// It is meant to be a thread-safe shared struct.
//
// You still need to handle the 'paused' logic on your non-controller threads.
// This doesn't just magically pause a thread.
//
// Example usage on non-controller thread: (you can also see `worker.rs` to see how we use it)
//
// ```rust
// fn spawn_worker(pauser: Arc<Pauser>) -> thread::JoinHandle<()> {
//     thread::spawn(move || {
//         loop {
//             // Do work here.
//             pauser.send_ack(); // Let them know we are paused.
//             pauser.recv_resume(); // Blocks until we are resumed.
//         }
//     })
// }
// ```
//
#[derive(Clone)]
pub(crate) struct Pauser {
    ack_channel: ThreadedChannel<()>,
    unpause_channel: ThreadedChannel<()>,
}

impl Pauser {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            ack_channel: ThreadedChannel::new(),
            unpause_channel: ThreadedChannel::new(),
        })
    }

    // Call from non-controller thread, like a worker thread.
    // Lets the controller thread know we are paused.
    pub(crate) fn send_ack(&self) {
        let _ = self.ack_channel.sender.send(());
    }

    // Call from non-controller thread, like a worker thread.
    // Blocks until controller thread sends the resume message.
    pub(crate) fn recv_resume(&self) {
        let _ = lock_safe(&self.unpause_channel.receiver).recv();
    }

    // Call from controller thread.
    // Blocks until non-controller thread tells us they are paused.
    pub(crate) fn recv_ack(&self) {
        let _ = lock_safe(&self.ack_channel.receiver).recv();
    }

    // Call from controller thread.
    // Sends resume message to non-controller thread.
    pub(crate) fn send_resume(&self) {
        let _ = self.unpause_channel.sender.send(());
    }
}
