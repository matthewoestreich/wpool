#![allow(dead_code)]
use std::sync::{
    Arc, Mutex,
    mpsc::{self, RecvError},
};

use crate::safe_lock;

/// Does not modify either the sending or receiving end of
/// the channel. This is a convenience wrapper - you get both
/// the raw `mpsc::Sender<T>` and raw `mpsc::Receiver<T>`.
pub(crate) struct Channel<T> {
    pub(crate) sender: mpsc::Sender<T>,
    pub(crate) receiver: mpsc::Receiver<T>,
}

impl<T> Channel<T> {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self { sender, receiver }
    }
}

/// Does NOT modify the sender half of the channel, you get
/// the raw `mpsc::SyncSender<T>`.
/// Wraps the receiving half of the channel in `Arc<Mutex<mpsc::Receiver<T>>>`.
pub(crate) struct ShareSyncChannel<T> {
    pub(crate) sender: mpsc::SyncSender<T>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ShareSyncChannel<T> {
    pub(crate) fn new(bound: usize) -> Self {
        let (sender, rx) = mpsc::sync_channel(bound);
        Self {
            sender,
            receiver: Mutex::new(rx).into(),
        }
    }
}

/// Does NOT modify the sender half of the channel, you get
/// the raw `mpsc::Sender<T>`.
/// Wraps the receiving half of the channel in `Arc<Mutex<mpsc::Receiver<T>>>`.
#[derive(Clone)]
pub(crate) struct ShareChannel<T> {
    pub(crate) sender: mpsc::Sender<T>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ShareChannel<T> {
    pub(crate) fn new() -> Self {
        let (sender, rx) = mpsc::channel();
        Self {
            sender,
            receiver: Mutex::new(rx).into(),
        }
    }
}

/// Wraps sender half of channel in `Mutex<Option<mpsc::Sender<T>>>`.
/// Wraps receiver half of channel in `Arc<Mutex<mpsc::Receiver<T>>>`.
pub(crate) struct OptionShareChannel<T> {
    pub(crate) sender: Mutex<Option<mpsc::Sender<T>>>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> OptionShareChannel<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            sender: Some(tx).into(),
            receiver: Mutex::new(rx).into(),
        }
    }

    pub(crate) fn send(&self, element: T) -> bool {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            return sender.send(element).is_ok();
        }
        true
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.receiver).recv()
    }

    pub(crate) fn close(&self) {
        if let Some(sender) = safe_lock(&self.sender).take() {
            drop(sender);
        }
    }

    pub(crate) fn clone_sender(&self) -> mpsc::Sender<T> {
        safe_lock(&self.sender).clone().expect("Clone to work")
    }
}
