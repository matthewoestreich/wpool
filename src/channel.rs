use std::sync::{
    Arc, Mutex,
    mpsc::{self, RecvError},
};

use crate::safe_lock;

/// Does NOT modify the sender half of the channel, you get
/// the raw `mpsc::Sender<T>`.
/// Wraps the receiving half of the channel in `Arc<Mutex<mpsc::Receiver<T>>>`.
#[derive(Clone)]
pub(crate) struct ThreadSafeChannel<T> {
    pub(crate) sender: mpsc::Sender<T>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ThreadSafeChannel<T> {
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
pub(crate) struct ThreadSafeOptionChannel<T> {
    pub(crate) sender: Mutex<Option<mpsc::Sender<T>>>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ThreadSafeOptionChannel<T> {
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
        false
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
