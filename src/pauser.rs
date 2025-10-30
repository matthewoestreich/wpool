use crate::{channel::ThreadedChannel, lock_safe};

// PauserDestination is the thread you want to pause.
#[derive(Clone)]
pub(crate) struct PauserDestination {
    ack_channel: ThreadedChannel<()>,
    unpause_channel: ThreadedChannel<()>,
}

impl PauserDestination {
    fn new(ack_channel: ThreadedChannel<()>, unpause_channel: ThreadedChannel<()>) -> Self {
        Self {
            ack_channel,
            unpause_channel,
        }
    }

    pub(crate) fn send_ack(&self) {
        let _ = self.ack_channel.sender.send(());
    }

    pub(crate) fn wait_for_resume(&self) {
        let _ = lock_safe(&self.unpause_channel.receiver).recv();
    }
}

// PauserSource is the "calling thread" that is telling another thread to pause.
#[derive(Clone)]
pub(crate) struct PauserSource {
    ack_channel: ThreadedChannel<()>,
    unpause_channel: ThreadedChannel<()>,
}

impl PauserSource {
    fn new(ack_channel: ThreadedChannel<()>, unpause_channel: ThreadedChannel<()>) -> Self {
        Self {
            ack_channel,
            unpause_channel,
        }
    }

    pub(crate) fn wait_for_ack(&self) {
        let _ = lock_safe(&self.ack_channel.receiver).recv();
    }

    pub(crate) fn send_resume(&self) {
        let _ = self.unpause_channel.sender.send(());
    }
}

// Pauser is a 'quality-of-life' wrapper to help make it
// easier to send a pause signal to workers.
//
// Put's workers in a blocking 'paused' state, meaning they
// cannot accept new signals while paused.
#[derive(Clone)]
pub(crate) struct Pauser {
    pub(crate) destination: PauserDestination,
    pub(crate) source: PauserSource,
}

impl Pauser {
    pub(crate) fn new() -> Self {
        let ack = ThreadedChannel::new();
        let pause = ThreadedChannel::new();
        Self {
            destination: PauserDestination::new(ack.clone(), pause.clone()),
            source: PauserSource::new(ack.clone(), pause.clone()),
        }
    }
}
