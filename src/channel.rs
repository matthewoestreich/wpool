#![allow(dead_code)]
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{
            self, RecvError, RecvTimeoutError, SendError, SyncSender, TryRecvError, TrySendError,
        },
    },
    time::Duration,
};

use crate::safe_lock;

pub(crate) fn bounded<T>(capacity: usize) -> Channel<T> {
    let (tx, rx) = mpsc::sync_channel::<T>(capacity);
    Channel {
        kind: ChannelKind::Bounded {
            sender: Sender::Bounded(Arc::new(Mutex::new(Some(tx)))),
            receiver: Receiver::new(rx),
        },
    }
}

pub(crate) fn unbounded<T>() -> Channel<T> {
    let (tx, rx) = mpsc::channel::<T>();
    Channel {
        kind: ChannelKind::Unbounded {
            sender: Sender::Unbounded(Arc::new(Mutex::new(Some(tx)))),
            receiver: Receiver::new(rx),
        },
    }
}

#[derive(Debug)]
pub(crate) enum Sender<T> {
    Unbounded(Arc<Mutex<Option<mpsc::Sender<T>>>>),
    Bounded(Arc<Mutex<Option<SyncSender<T>>>>),
}

impl<T> Sender<T> {
    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self {
            Self::Unbounded(tx) => {
                if let Some(inner) = safe_lock(tx).as_ref() {
                    inner.send(msg)
                } else {
                    Err(SendError(msg))
                }
            }
            Self::Bounded(tx) => {
                if let Some(inner) = safe_lock(tx).as_ref() {
                    inner.send(msg)
                } else {
                    Err(SendError(msg))
                }
            }
        }
    }

    pub(crate) fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        match self {
            Sender::Bounded(tx) => {
                if let Some(inner) = safe_lock(tx).as_ref() {
                    inner.try_send(msg)
                } else {
                    Err(TrySendError::Disconnected(msg))
                }
            }
            Sender::Unbounded(tx) => {
                if let Some(inner) = safe_lock(tx).as_ref() {
                    match inner.send(msg) {
                        Ok(_) => Ok(()),
                        Err(SendError(msg)) => Err(TrySendError::Disconnected(msg)),
                    }
                } else {
                    Err(TrySendError::Disconnected(msg))
                }
            }
        }
    }

    pub(crate) fn drop(&self) {
        match self {
            Self::Unbounded(tx) => drop(safe_lock(tx).take()),
            Self::Bounded(tx) => drop(safe_lock(tx).take()),
        }
    }

    pub(crate) fn is_some(&self) -> bool {
        match self {
            Self::Unbounded(tx) => safe_lock(tx).is_some(),
            Self::Bounded(tx) => safe_lock(tx).is_some(),
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Unbounded(inner) => Self::Unbounded(Arc::clone(inner)),
            Self::Bounded(inner) => Self::Bounded(Arc::clone(inner)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Receiver<T> {
    inner: Arc<Mutex<mpsc::Receiver<T>>>,
}

impl<T> Receiver<T> {
    fn new(inner: mpsc::Receiver<T>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        safe_lock(&self.inner).recv()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        safe_lock(&self.inner).try_recv()
    }

    pub(crate) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        safe_lock(&self.inner).recv_timeout(timeout)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub(crate) enum ChannelKind<T> {
    Unbounded {
        sender: Sender<T>,
        receiver: Receiver<T>,
    },
    Bounded {
        sender: Sender<T>,
        receiver: Receiver<T>,
    },
}

impl<T> std::fmt::Debug for ChannelKind<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ChannelKind::Bounded { .. } => write!(f, "ChannelKind::Bounded"),
            ChannelKind::Unbounded { .. } => write!(f, "ChannelKind::Unbounded"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Channel<T> {
    kind: ChannelKind<T>,
}

impl<T> Channel<T> {
    pub(crate) fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match &self.kind {
            ChannelKind::Unbounded { sender, .. } => sender.send(msg),
            ChannelKind::Bounded { sender, .. } => sender.send(msg),
        }
    }

    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        match &self.kind {
            ChannelKind::Unbounded { receiver, .. } => receiver.recv(),
            ChannelKind::Bounded { receiver, .. } => receiver.recv(),
        }
    }

    pub(crate) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match &self.kind {
            ChannelKind::Unbounded { receiver, .. } => receiver.recv_timeout(timeout),
            ChannelKind::Bounded { receiver, .. } => receiver.recv_timeout(timeout),
        }
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        match &self.kind {
            ChannelKind::Unbounded { receiver, .. } => receiver.try_recv(),
            ChannelKind::Bounded { receiver, .. } => receiver.try_recv(),
        }
    }

    pub(crate) fn clone_sender(&self) -> Sender<T> {
        match &self.kind {
            ChannelKind::Unbounded { sender, .. } => sender.clone(),
            ChannelKind::Bounded { sender, .. } => sender.clone(),
        }
    }

    pub(crate) fn clone_receiver(&self) -> Receiver<T> {
        match &self.kind {
            ChannelKind::Unbounded { receiver, .. } => receiver.clone(),
            ChannelKind::Bounded { receiver, .. } => receiver.clone(),
        }
    }

    pub(crate) fn drop_sender(&self) {
        match &self.kind {
            ChannelKind::Unbounded { sender, .. } => sender.drop(),
            ChannelKind::Bounded { sender, .. } => sender.drop(),
        }
    }

    pub(crate) fn is_sender_some(&self) -> bool {
        match &self.kind {
            ChannelKind::Unbounded { sender, .. } => sender.is_some(),
            ChannelKind::Bounded { sender, .. } => sender.is_some(),
        }
    }
}
