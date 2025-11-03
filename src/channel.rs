#![allow(dead_code)]
use std::sync::Mutex;

use crossbeam_channel::{Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError};

use crate::safe_lock;

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

impl<T> Channel<Sender<T>, Receiver<T>> {
    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.sender.send(msg)
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }
}

impl<T> Channel<Mutex<Option<Sender<T>>>, Receiver<T>> {
    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            sender.send(msg)
        } else {
            Err(SendError(msg))
        }
    }

    pub(crate) fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            sender.try_send(msg)
        } else {
            Err(TrySendError::Disconnected(msg))
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        if let Some(sender) = safe_lock(&self.sender).as_ref() {
            return sender.is_full();
        }
        true
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    pub(crate) fn clone_sender(&self) -> Option<Sender<T>> {
        safe_lock(&self.sender).clone()
    }

    pub(crate) fn clone_receiver(&self) -> Receiver<T> {
        self.receiver.clone()
    }

    pub(crate) fn close(&self) -> bool {
        if let Some(sender) = safe_lock(&self.sender).take() {
            drop(sender);
            return true;
        }
        false
    }
}
