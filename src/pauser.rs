use crate::{channel::ThreadedChannel, lock_safe};

// Pauser is a 'quality-of-life' wrapper to help make it
// easier to send a pause signal to workers.
//
// Put's workers in a blocking 'paused' state, meaning they
// cannot accept new signals while paused.
#[derive(Clone)]
pub(crate) struct Pauser {
    ack_channel: ThreadedChannel<()>,
    unpause_channel: ThreadedChannel<()>,
}

impl Pauser {
    pub(crate) fn new() -> Self {
        Self {
            ack_channel: ThreadedChannel::new(),
            unpause_channel: ThreadedChannel::new(),
        }
    }

    // A worker (or some other thread) is meant to call send_ack.
    // This is the worker "braodcasting" that they have been paused.
    // A worker is meant to call 'send_ack()' first and then call
    // 'wait_for_unpause()', which is what ultimately puts it in a paused state.
    pub(crate) fn send_ack(&self) {
        let _ = self.ack_channel.sender.send(());
    }

    // This is meant to block each worker. The worker should call 'send_ack()'
    // and then 'wait_for_unpause', effectivey putting the worker in a paused,
    // blocking state.
    pub(crate) fn wait_for_unpause(&self) {
        let _ = lock_safe(&self.unpause_channel.receiver).recv();
    }

    // The thread that needs acknowledgement would call this.
    // It blocks until acknowledgement.
    pub(crate) fn wait_for_ack(&self) {
        let _ = lock_safe(&self.ack_channel.receiver).recv();
    }

    // Unpauses a worker by sending a message to the unpause channel.
    pub(crate) fn unpause(&self) {
        let _ = self.unpause_channel.sender.send(());
    }
}
