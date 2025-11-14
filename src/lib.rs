//! A thread pool that limits the number of tasks executing concurrently, without restricting how many tasks can be queued. Submitting tasks is non-blocking, so you can enqueue any number of tasks without waiting.
//!
//! This library is essentially a port of [`workerpool`](https://github.com/gammazero/workerpool), an amazing Go library.
//!
//! # Examples
//!
//! ## Worker Maximum
//!
//! `max_workers` must be greater than 0! If `max_workers == 0` we panic.
//!
//! ```rust
//! use wpool::WPool;
//!
//! // At most 10 workers can run at once.
//! let max_workers = 10;
//!
//! let pool = WPool::new(max_workers);
//!
//! // Submit as many functions as you'd like.
//! pool.submit(|| {
//!     // Do some work.
//!     println!("hello from job 1");
//! });
//!
//! pool.submit(|| {
//!     // Do more work.
//!     println!("hello from job 2");
//! });
//!
//! // Block until all workers are done working and
//! // the waiting queue has been processed.
//! pool.stop_wait();
//! ```
//!
//! ## Worker Maximum and Minimum
//!
//! `min_workers` defines (up to) the minimum number of worker threads that should always stay alive, even when the pool is idle. If `min_workers` is greater than `max_workers`, we panic.
//!
//! **NOTE**: *We do not 'pre-spawn' workers!* Meaning, if you set `min_workers = 3` but your pool only ever creates 2 workers, then only 2 workers will ever exist (and should always be alive).
//!
//! ```rust
//! use wpool::WPool;
//!
//! // At most 10 workers can run at once.
//! let max_workers = 10;
//! // At minimum up to 3 workers should always exist.
//! let min_workers = 3;
//!
//! let pool = WPool::new_with_min(max_workers, min_workers);
//!
//! // Submit as many functions as you'd like.
//! pool.submit(|| {
//!     // Do some work.
//!     println!("doing the thing");
//! });
//!
//! pool.submit(|| {
//!     // Do more work.
//!     println!("the thing is being done");
//! });
//!
//! // Block until all workers are done working and
//! // the waiting queue has been processed.
//! pool.stop_wait();
//! ```
//!
//! ## Capturing
//!
//! ```rust
//! use wpool::WPool;
//!
//! #[derive(Clone, Debug)]
//! struct Foo {
//!     foo: u8,
//! }
//!
//! let wp = WPool::new(3);
//! let my_foo = Foo { foo: 1 };
//!
//! // You can either clone before capturing:
//! let my_foo_clone = my_foo.clone();
//! wp.submit(move || {
//!     println!("{my_foo_clone:?}");
//! });
//!
//! // Or allow the closure to consume (eg. move ownership):
//! wp.submit(move || {
//!     println!("{my_foo:?}");
//! });
//!
//! wp.stop_wait();
//! ```
//!
//! ## Submit as Fast as Possible
//!
//! You just want to submit jobs as fast as possible without any blocking. Does not wait for any sort of confirmation.
//!
//! Does not block under any circumstance by default.
//!
//! ```rust
//! use wpool::WPool;
//!
//! let wp = WPool::new(5);
//!
//! for i in 1..=100 {
//!     wp.submit(move || {
//!         println!("job {i}");
//!     });
//! }
//! ```
//!
//! ## Wait for Submission to Execute
//!
//! ```rust
//! use wpool::WPool;
//!
//! let wp = WPool::new(2);
//!
//! // Will block here until job is *executed*.
//! wp.submit_wait(|| { println!("work"); });
//! wp.stop_wait();
//! ```
//!
//! ## Wait for Submission to be Submitted
//!
//! Wait until your submission is either given to a worker or queued. **Does not wait for you submission to be executed, only submitted.**
//!
//! This can be useful in loops when you need to know that everything has been submitted. Meaning, you now know workers have for sure been spawned.
//!
//! ```rust
//! use wpool::WPool;
//! use std::thread;
//! use std::time::Duration;
//!
//! let max_workers = 5;
//! let wp = WPool::new(max_workers);
//!
//! for i in 1..=max_workers {
//!     // Will block here until job is *submitted*.
//!     wp.submit_confirm(|| {
//!         thread::sleep(Duration::from_secs(2));
//!     });
//!     // Now you know that a worker has been spawned, or job placed in queue (which means we are already at max workers).
//!     assert_eq!(wp.worker_count(), i);
//! }
//!
//! assert_eq!(wp.worker_count(), max_workers);
//! wp.stop_wait();
//! ```
//!
//! ## Get Results from Job
//!
//! ```rust
//! use wpool::WPool;
//! use std::thread;
//! use std::time::Duration;
//! use std::sync::mpsc;
//!
//! let max_workers = 2;
//! let wp = WPool::new(max_workers);
//! let expected_result = 88;
//! let (tx, rx) = mpsc::sync_channel::<u8>(0);
//!
//! // Clone sender and pass into job.
//! let tx_clone = tx.clone();
//! wp.submit(move || {
//!     //
//!     // Do work here.
//!     //
//!     thread::sleep(Duration::from_millis(500));
//!     //
//!     let result_from_doing_work = 88;
//!     //
//!
//!     if let Err(e) = tx_clone.send(result_from_doing_work) {
//!         panic!("error sending results to main thread from worker! : Error={e:?}");
//!     }
//!     println!("success! sent results from worker to main!");
//! });
//!
//! // Pause until we get our result. This is not necessary in this case, as our channel
//! // is acting as a pseudo pauser. If we were using an unbounded channel, we may want
//! // to use pause in order to wait for the result of any running task (like if we need
//! // to use the result elsewhere).
//! //
//! // wp.pause();
//!
//! match rx.recv() {
//!     Ok(result) => assert_eq!(
//!         result, expected_result,
//!         "expected {expected_result} got {result}"
//!     ),
//!     Err(e) => panic!("unexpected channel error : {e:?}"),
//! }
//!
//! wp.stop_wait();
//! ```
//!
//! ## View Tasks that Panicked
//!
//! We collect panic info which can be accessed via `<instance>.get_workers_panic_info()`.
//!
//! ```rust
//! use wpool::WPool;
//!
//! let wp = WPool::new(3);
//! wp.submit(|| panic!("something went wrong!"));
//! // Wait for currently running jobs to finish.
//! wp.pause();
//! println!("{:#?}", wp.get_workers_panic_info());
//! // [
//! //     PanicInfo {
//! //         thread_id: ThreadId(
//! //             9,
//! //         ),
//! //         payload: Some(
//! //             "something went wrong!",
//! //         ),
//! //         file: Some(
//! //             "src/file.rs",
//! //         ),
//! //         line: Some(
//! //             163,
//! //         ),
//! //         column: Some(
//! //             19,
//! //         ),
//! //     },
//! // ]
//! wp.stop_wait();
//! ```
//!
#![allow(clippy::too_many_arguments)]
#[cfg(test)]
mod tests;

mod channel;
mod dispatcher;
mod shared;
mod wpool;

pub mod pacer;
pub use wpool::WPool;

use std::{
    fmt::{self, Display, Formatter},
    panic::PanicHookInfo,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, Once,
        atomic::{AtomicUsize, Ordering},
        mpsc::SyncSender,
    },
    thread::{self, ThreadId},
};

/************************************* [PUBLIC] PanicInfo ********************************/

/// `PanicInfo` displays panic information.
#[derive(Clone, Debug)]
pub struct PanicInfo {
    pub thread_id: ThreadId,
    pub payload: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

impl From<&PanicHookInfo<'_>> for PanicInfo {
    fn from(info: &PanicHookInfo) -> Self {
        let mut this = Self {
            thread_id: thread::current().id(),
            payload: None,
            file: None,
            line: None,
            column: None,
        };

        if let Some(location) = info.location() {
            this.file = Some(location.file().to_string());
            this.line = Some(location.line());
            this.column = Some(location.column());
        }

        this.payload = info.payload_as_str().map(|s| s.to_string());
        this
    }
}

/************************************* safe_lock *****************************************/

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

impl fmt::Debug for WPoolStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/*********************************** Signal **********************************************/

#[derive(Clone)]
pub(crate) enum Signal {
    NewTask(Task, Arc<Mutex<Option<SyncSender<()>>>>),
    Terminate,
}

impl Signal {
    /// If the Signal is `Signal::NewTask(Task, Some(confirm));` then we take `Some(confirm)`
    /// and drop it. Effectively sending "confirmation" to the receiving end.
    /// ## This is the function body of `confirm_submit`
    /// ```rust,ignore
    /// if let Signal::NewTask(_, confirm) = self {
    ///     drop(safe_lock(confirm).take());
    /// }
    /// ```
    pub(crate) fn confirm_submit(&self) {
        if let Signal::NewTask(_, confirm) = self
            && let Some(confirmation) = safe_lock(confirm).take()
        {
            drop(confirmation);
        }
    }
}

impl Display for Signal {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Signal::NewTask(_, opt) => write!(
                f,
                "Signal::NewTask(Task{})",
                if safe_lock(opt).is_some() {
                    ", Confirm"
                } else {
                    ""
                }
            ),
            Signal::Terminate => write!(f, "Signal::Terminate"),
        }
    }
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/*********************************** Task ***********************************************/

pub(crate) struct Task {
    inner: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Clone for Task {
    fn clone(&self) -> Self {
        Task {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Task {
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let f = Mutex::new(Some(f));
        Self {
            inner: Arc::new(move || {
                if let Some(f) = safe_lock(&f).take() {
                    f();
                }
            }),
        }
    }

    pub fn noop() -> Self {
        Self {
            inner: Arc::new(|| {}),
        }
    }

    pub fn run(&self) {
        (self.inner)();
    }
}

/*********************************** WaitGroup ******************************************/

#[derive(Clone)]
pub(crate) struct WaitGroup {
    inner: Arc<WaitGroupInner>,
}

struct WaitGroupInner {
    count: AtomicUsize,
    cvar: Condvar,
    lock: Mutex<()>,
}

impl WaitGroup {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WaitGroupInner {
                count: AtomicUsize::new(0),
                cvar: Condvar::new(),
                lock: Mutex::new(()),
            }),
        }
    }

    /// ## Combines `new()` and `.add(delta)` into a single call.
    /// ### Equivalent to:
    /// ```rust
    /// #[cfg(test)]
    /// {
    ///     use crate::WaitGroup;
    ///     let wg = WaitGroup::new();
    ///     wg.add(delta);
    /// }
    /// ```
    pub(crate) fn new_with_delta(delta: usize) -> Self {
        Self {
            inner: Arc::new(WaitGroupInner {
                count: AtomicUsize::new(delta),
                cvar: Condvar::new(),
                lock: Mutex::new(()),
            }),
        }
    }

    #[inline]
    pub(crate) fn add(&self, delta: usize) {
        let _guard = safe_lock(&self.inner.lock);
        self.inner.count.fetch_add(delta, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn done(&self) {
        let _guard = safe_lock(&self.inner.lock);
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.cvar.notify_all();
        }
    }

    #[inline]
    pub(crate) fn wait(&self) {
        let mut guard = safe_lock(&self.inner.lock);
        while self.inner.count.load(Ordering::SeqCst) > 0 {
            guard = self.inner.cvar.wait(guard).unwrap();
        }
    }
}

/************************************* ThreadGuardian ************************************/

/// `ThreadGuardian` allows registering a one-time panic handler that consumes `args`.
/// `on_panic` can only be called once. The handler will run if the thread panics.
///
/// You can pass any args you want to ThreadGuardian during instantiation, just wrap them
/// in a tuple and you will have access to them in the `on_panic` method.
///
/// You must tie your instance to a variable that will be dropped when the thread ends!
/// Even if it is not used, othrwise this will not work!
///
/// # Examples
///
/// ## Use any args in `on_panic`
///
/// ```rust
/// #[cfg(test)]
/// {
///     use std::thread;
///     use wpool::ThreadGuardian;
///
///     #[derive(Clone)]
///     struct Foo { foo: u8 };
///     #[derive(Clone)]
///     struct Bar { bar: u8 };
///     #[derive(Clone)]
///     struct Baz { baz: u8 };
///
///     let foo = Foo { foo: 1 };
///     let bar = Bar { bar: 2 };
///     let baz = Baz { baz: 3 };
///
///     thread::spawn(move || {
///         // Any args needed in `on_panic` should be passed to `new()` as a tuple.
///         // The order of args is the same order you will receive them!
///         let tg_args = (foo.clone(), bar.clone(), baz.clone());
///     
///         // Pass them to `ThreadGuardian::new()`:
///         let tg = ThreadGuardian::new(tg_args);
///     
///         // Use them in `on_panic(...)` handler: (args are passed back to you in the same order we received them)
///         tg.on_panic(|(foo, bar, baz)| {
///             // Handle thread panic:
///             println!("{foo:?} {bar:?} {baz:?}");
///         });
///
///         // If this thread panicked, `tg.on_panic` would be called.
///         /* panic!("uh-oh!"); */
///     });
/// }
/// ```
///
/// ## Without args
///
/// ```rust
/// #[cfg(test)]
/// {
///     use wpool::ThreadGuardian;
///     // Pass empty tuple:
///     let tg = ThreadGuardian::new(());
///     // Underscore for args:
///     tg.on_panic(|_| {
///         println!("do work!");
///     });
/// }
/// ```
///
/// ## Must tie instance to variable!
///
/// ```rust,ignore
/// // This won't work:
/// ThreadGuardian::new((...)).on_panic(|_| { ... });
/// // You must tie your instance to a variable that is dropped
/// // when the thread ends:
/// let tg = ThreadGuardian::new((...));
/// tg.on_panic(|_| { ... });
/// ```
pub(crate) struct ThreadGuardian<T>
where
    T: Send + 'static,
{
    once: Once,
    args: Mutex<Option<T>>,
    run_on_panic_fn: Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
}

impl<T> ThreadGuardian<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(args: T) -> Self {
        Self {
            once: Once::new(),
            args: Mutex::new(Some(args)),
            run_on_panic_fn: Mutex::new(None),
        }
    }

    /// Can only be called one time per instance.
    pub(crate) fn on_panic<F>(&self, handler: F)
    where
        F: FnOnce(T) + Send + 'static,
    {
        self.once.call_once(|| {
            let args = safe_lock(&self.args)
                .take()
                .expect("on_panic to be called once");
            *safe_lock(&self.run_on_panic_fn) = Some(Box::new(move || handler(args)));
        });
    }
}

impl<T> Drop for ThreadGuardian<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        if thread::panicking()
            && let Some(f) = safe_lock(&self.run_on_panic_fn).take()
        {
            f();
        }
    }
}
