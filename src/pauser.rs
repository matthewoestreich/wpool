use crate::channel::ThreadedChannel;

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

    pub(crate) fn send_ack(&self) {
        let _ = self.ack_channel.sender.send(());
    }

    pub(crate) fn wait_for_ack(&self) {
        let _ = self.ack_channel.receiver.lock().unwrap().recv();
    }

    pub(crate) fn wait_for_unpause(&self) {
        let _ = self.unpause_channel.receiver.lock().unwrap().recv();
    }

    pub(crate) fn unpause(&self) {
        let _ = self.unpause_channel.sender.send(());
    }
}
