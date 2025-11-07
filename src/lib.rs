//! A thread pool that limits the number of tasks executing concurrently, without restricting how many tasks can be queued. Submitting tasks is non-blocking, so you can enqueue any number of tasks without waiting.
//!
//! This library is essentially a port of [`workerpool`](https://github.com/gammazero/workerpool), an amazing Go library.
//!
//! # Examples
//!
//! ## Pool with only worker maximum
//!
//! ```rust
//! // At most 10 workers can run at once.
//! let max_workers = 10;
//!
//! let pool = WPool::new(max_workers);
//!
//! // Submit as many functions as you'd like.
//! pool.submit(|| {
//!   // Do some work.
//! });
//! pool.submit(|| {
//!   // Do more work.
//! });
//!
//! // Block until all workers are done working and
//! // the waiting queue has been processed.
//! pool.stop_wait();
//! ```
//!
//! ## Pool with both worker maximum and worker minimum
//!
//! `min_workers` defines (up to) the minimum number of worker threads that should always stay alive, even when the pool is idle.
//!
//! **NOTE**: *We do not 'pre-spawn' workers!* Meaning, if you set `min_workers = 3` but your pool only ever creates 2 workers, then only 2 workers will ever exist (and should always be alive).
//!
//! ```rust
//! // At most 10 workers can run at once.
//! let max_workers = 10;
//! // At minimum up to 3 workers should always exist.
//! let min_workers = 3;
//!
//! let pool = WPool::new_with_min(max_workers, min_workers);
//!
//! // Submit as many functions as you'd like.
//! pool.submit(|| {
//!   // Do some work.
//! });
//! pool.submit(|| {
//!   // Do more work.
//! });
//!
//! // Block until all workers are done working and
//! // the waiting queue has been processed.
//! pool.stop_wait();
//! ```
#![allow(clippy::too_many_arguments)]
#[cfg(test)]
mod tests;

mod channel;
mod wait_group;
mod wpool;

pub use wpool::WPool;

use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

// One-liner that allows us to easily lock a Mutex while handling possible poison.
pub(crate) fn safe_lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/************************************* WPoolStatus ***************************************/

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WPoolStatus {
    Running,
    Paused,
    Stopped(bool),
}

impl WPoolStatus {
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Paused => 1,
            Self::Stopped(false) => 2,
            Self::Stopped(true) => 3,
        }
    }

    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            1 => Self::Paused,
            2 => Self::Stopped(false),
            3 => Self::Stopped(true),
            _ => unreachable!(),
        }
    }
}

impl Display for WPoolStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            WPoolStatus::Running => write!(f, "WPoolStatus::Running"),
            WPoolStatus::Paused => write!(f, "WPoolStatus::Paused"),
            WPoolStatus::Stopped(wait) => write!(f, "WPoolStatus::Stopped(wait={wait})"),
        }
    }
}

impl std::fmt::Debug for WPoolStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/*********************************** Signal **********************************************/

#[derive(Clone)]
pub(crate) enum Signal {
    NewTask(Task),
    Terminate,
}

impl Display for Signal {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Signal::NewTask(_) => write!(f, "Signal::NewTask(Task)"),
            Signal::Terminate => write!(f, "Signal::Terminate"),
        }
    }
}

impl std::fmt::Debug for Signal {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/*********************************** Task ***********************************************/

pub(crate) type TaskFn = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct Task {
    inner: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Task {
    pub fn new(f: TaskFn) -> Self {
        let f = Mutex::new(Some(f));
        let inner = Arc::new(move || {
            if let Some(f) = safe_lock(&f).take() {
                f();
            }
        });
        Task { inner }
    }

    pub fn run(&self) {
        (self.inner)();
    }
}

impl Clone for Task {
    fn clone(&self) -> Self {
        Task {
            inner: Arc::clone(&self.inner),
        }
    }
}

/*********************************** ThreadedDeque **************************************/

// ThreadedDeque is a threadsafe VecDeque.
pub(crate) struct ThreadedDeque<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
}

impl<T> Clone for ThreadedDeque<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[allow(dead_code)]
impl<T> ThreadedDeque<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// If you want to get the MutexGuard for the inner VecDeque<T>
    /// This is not needed to use any methods!
    pub(crate) fn lock(
        &self,
    ) -> Result<MutexGuard<'_, VecDeque<T>>, PoisonError<MutexGuard<'_, VecDeque<T>>>> {
        self.inner.lock()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        safe_lock(&self.inner).len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        safe_lock(&self.inner).is_empty()
    }

    #[inline]
    pub(crate) fn push_back(&self, element: T) {
        safe_lock(&self.inner).push_back(element);
    }

    #[inline]
    pub(crate) fn pop_back(&self) -> Option<T> {
        safe_lock(&self.inner).pop_back()
    }

    #[inline]
    pub(crate) fn pop_front(&self) -> Option<T> {
        safe_lock(&self.inner).pop_front()
    }

    /// Returns a clone of the front. **DOES NOT MODIFY THE UNDERLYING VecDeque**
    #[inline]
    pub(crate) fn front(&self) -> Option<T>
    where
        T: Clone,
    {
        safe_lock(&self.inner).front().cloned()
    }
}
