use std::sync::{Arc, Mutex, mpsc};

pub(crate) struct ThreadedSyncChannel<T> {
    pub(crate) sender: mpsc::SyncSender<T>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ThreadedSyncChannel<T> {
    pub(crate) fn new(bound: usize) -> Self {
        let (sender, rx) = mpsc::sync_channel(bound);
        Self {
            sender,
            receiver: Mutex::new(rx).into(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ThreadedChannel<T> {
    pub(crate) sender: mpsc::Sender<T>,
    pub(crate) receiver: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> ThreadedChannel<T> {
    pub(crate) fn new() -> Self {
        let (sender, rx) = mpsc::channel();
        Self {
            sender,
            receiver: Arc::new(Mutex::new(rx)),
        }
    }
}
