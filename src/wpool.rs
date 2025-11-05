use std::sync::{
    Arc, Mutex, Once,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

use crate::{
    dispatcher::Dispatcher,
    job::{Signal, Task},
    safe_lock,
    thread::Pauser,
};

/// `WPool` is a thread pool that limits the number of tasks executing concurrently,
/// without restricting how many tasks can be queued. Submitting tasks is non-blocking,
/// so you can enqueue any number of tasks without waiting.
pub struct WPool {
    pub(crate) dispatcher: Arc<Dispatcher>,
    max_workers: usize,
    paused: Mutex<bool>,
    pauser: Arc<Pauser>,
    stop_once: Once,
    stopped: AtomicBool,
}

impl WPool {
    // Private "quality-of-life" helper. Makes it so we don't have to update struct fields in multiple places.
    fn new_base(max_workers: usize, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher: dispatcher.spawn(),
            max_workers,
            paused: false.into(),
            pauser: Pauser::new(),
            stop_once: Once::new(),
            stopped: false.into(),
        }
    }

    /// `new` creates and starts a pool of worker threads.
    ///
    /// The `max_workers` parameter specifies the maximum number of workers that can
    /// execute tasks concurrently. When there are no incoming tasks, workers are
    /// gradually stopped until there are no remaining workers.
    pub fn new(max_workers: usize) -> Self {
        let dispatcher = Arc::new(Dispatcher::new(max_workers, 0));
        Self::new_base(max_workers, dispatcher)
    }

    /// `new` creates and starts a pool of worker threads.
    ///
    /// The `max_workers` parameter specifies the maximum number of workers that can
    /// execute tasks concurrently. When there are no incoming tasks, workers are
    /// gradually stopped until there are no remaining workers.
    ///
    /// The `min_workers` parameter specifies up to the minimum amount of workers that
    /// should aways exist, even when the pool is idle. This is designed to help
    /// eliminate the overhead of spinning up threads from scratch.
    /// If `min_workers` is greater than `max_workers` then we change `min_workers` to
    /// equal `max_workers`.
    pub fn new_with_min(max_workers: usize, min_workers: usize) -> Self {
        let dispatcher = Arc::new(Dispatcher::new(max_workers, min_workers));
        Self::new_base(max_workers, dispatcher)
    }

    /// The number of `max_workers`.
    pub fn max_capacity(&self) -> usize {
        self.max_workers
    }

    /// The number of `min_workers`.
    pub fn min_capacity(&self) -> usize {
        self.dispatcher.min_workers
    }

    /// Enqueues the given function.
    ///
    /// Any external values needed by the task function must be captured in a closure.
    /// Any return values should be sent over a channel, or by similar means.
    ///
    /// `submit` is non-blocking, regardless of the number of tasks submitted. Each task
    /// is immediately given to an available worker. If there are no available workers, and
    /// the maximum number of workers are already created, the task will be put in a wait queue.
    ///
    /// As long as there are tasks in the wait queue, any additional new tasks are put in the
    /// wait queue. Tasks are removed from the wait queue as workers become available.
    pub fn submit<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let t = Task::new(Box::new(task));
        self.submit_signal(Signal::NewTask(t));
    }

    /// Enqueues the given function and blocks until it has been executed.
    pub fn submit_wait<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(0);
        self.submit(move || {
            task();
            let _ = sender.send(());
        });
        let _ = receiver.recv(); // blocks until complete
    }

    /// Stops the pool and waits for currently running tasks, as well as any tasks
    /// in the wait queue, to complete. Task submission is disallowed after
    /// `stop_wait()` has been called.
    ///
    /// **Since creating the pool starts at least one thread, for the dispatcher,
    /// `stop()` or `stop_wait()` should only be called when the worker pool is no
    /// longer needed**.
    pub fn stop_wait(&self) {
        self.shutdown(true);
    }

    /// `stop` stops the worker pool and waits for only currently running tasks to
    /// complete. Pending tasks that are not currently running are abandoned. **Tasks
    /// must not be submitted to the pool after calling stop.**
    ///
    /// **Since creating the pool starts at least one thread, for the dispatcher,
    /// `stop()` or `stop_wait()` should only be called when the worker pool is no
    /// longer needed**.
    pub fn stop(&self) {
        self.shutdown(false);
    }

    /// Pause all possible workers and block until each worker has acknowledged that
    /// they're paused.
    ///
    /// You must explicity call `resume()`, `stop()`, or `stop_wait()` to unpause the
    /// pool. If you want to unpause without any side-effects, call `resume()`.
    ///
    /// Paused workers are unable to accept new tasks, although you can still submit
    /// tasks, which will be picked up by workers once they're resumed.
    ///
    /// If the number of active workers is less than the pool maximum, workers will
    /// be spawned, up to the pool maximum, and immediately paused. This ensures that
    /// every worker that could possibly exist in the pool is paused.
    pub fn pause(&self) {
        // We want to hold the pause lock for the entirety of the pause operation.
        let mut is_paused = safe_lock(&self.paused);
        if self.stopped.load(Ordering::SeqCst) || *is_paused {
            return;
        }

        let pauser = Arc::clone(&self.pauser);
        for _ in 0..self.max_workers {
            self.submit_signal(Signal::Pause(Arc::clone(&pauser)));
        }
        // Blocks until all workers tell us they're paused.
        for _ in 0..self.max_workers {
            pauser.recv_ack();
        }

        *is_paused = true;
    }

    /// Resumes/unpauses all paused workers.
    pub fn resume(&self) {
        // We want to hold the pause lock for the entirety of the resume operation.
        let mut is_paused = safe_lock(&self.paused);
        if self.stopped.load(Ordering::SeqCst) || !*is_paused {
            return;
        }
        for _ in 0..self.max_workers {
            self.pauser.send_resume();
        }
        *is_paused = false;
    }

    /************************* Private methods ***************************************/

    // "Quality-of-life" function, easier to submit signals directly to the dispatcher.
    fn submit_signal(&self, signal: Signal) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        self.dispatcher.submit(signal);
    }

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            self.resume();
            // Acquire pause lock to wait for any pauses in progress to complete
            let pause_lock = safe_lock(&self.paused);
            self.stopped.store(true, Ordering::SeqCst);
            drop(pause_lock);
            self.dispatcher.set_is_wait(wait);
            self.dispatcher.close_task_channel();
        });
        self.dispatcher.join();
    }
}
