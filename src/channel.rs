#![allow(dead_code)]
use std::sync::{
    Arc, Mutex,
    mpsc::{self, Receiver, RecvError, SendError, Sender, SyncSender, TryRecvError},
};

use crate::safe_lock;

pub(crate) type BoundedChannel<T> = crate::channel::Channel<
    std::sync::Mutex<Option<std::sync::mpsc::SyncSender<T>>>,
    std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<T>>>,
>;
pub(crate) type UnboundedChannel<T> = crate::channel::Channel<
    std::sync::Mutex<Option<std::sync::mpsc::Sender<T>>>,
    std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<T>>>,
>;

pub(crate) struct ThreadSafeReceiver<T> {
    inner: Arc<Mutex<Receiver<T>>>,
}

impl<T> ThreadSafeReceiver<T> {
    pub(crate) fn new(inner: Arc<Mutex<Receiver<T>>>) -> Self {
        Self { inner }
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.inner).recv()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        safe_lock(&self.inner).try_recv()
    }
}

#[derive(Default)]
pub(crate) struct Channel<S, R> {
    sender: S,
    receiver: R,
}

impl<S, R> Channel<S, R> {
    pub(crate) fn new(sender: S, receiver: R) -> Self {
        Self { sender, receiver }
    }
}

impl<T> Channel<Sender<T>, Arc<Mutex<Receiver<T>>>> {
    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.sender.send(msg)
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.receiver).recv()
    }
}

impl<T> Channel<Mutex<Option<Sender<T>>>, Arc<Mutex<Receiver<T>>>> {
    pub(crate) fn new_unbounded() -> Self {
        let (tx, rx) = mpsc::channel::<T>();
        Self {
            sender: Some(tx).into(),
            receiver: Arc::new(Mutex::new(rx)),
        }
    }

    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            sender.send(msg)
        } else {
            Err(SendError(msg))
        }
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.receiver).recv()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        safe_lock(&self.receiver).try_recv()
    }

    pub(crate) fn sender_is_some(&self) -> bool {
        safe_lock(&self.sender).is_some()
    }

    pub(crate) fn clone_sender(&self) -> Option<Sender<T>> {
        safe_lock(&self.sender).clone()
    }

    pub(crate) fn clone_receiver(&self) -> ThreadSafeReceiver<T> {
        ThreadSafeReceiver::new(Arc::clone(&self.receiver))
    }

    pub(crate) fn close(&self) -> bool {
        if let Some(sender) = safe_lock(&self.sender).take() {
            drop(sender);
            return true;
        }
        false
    }
}

impl<T> Channel<Mutex<Option<SyncSender<T>>>, Arc<Mutex<Receiver<T>>>> {
    pub(crate) fn new_bounded(capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel::<T>(capacity);
        Self {
            sender: Mutex::new(Some(tx)),
            receiver: Arc::new(Mutex::new(rx)),
        }
    }

    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            sender.send(msg)
        } else {
            Err(SendError(msg))
        }
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.receiver).recv()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        safe_lock(&self.receiver).try_recv()
    }

    pub(crate) fn clone_sender(&self) -> Option<SyncSender<T>> {
        safe_lock(&self.sender).clone()
    }

    pub(crate) fn clone_receiver(&self) -> Arc<Mutex<Receiver<T>>> {
        Arc::clone(&self.receiver)
    }

    pub(crate) fn close(&self) -> bool {
        if let Some(sender) = safe_lock(&self.sender).take() {
            drop(sender);
            return true;
        }
        false
    }
}
