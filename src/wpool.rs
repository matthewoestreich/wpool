use std::{
    collections::VecDeque,
    panic::{self},
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicU8, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError, TryRecvError},
    },
    thread,
    time::Duration,
};

use crate::{
    PanicInfo, Signal, Task, ThreadGuardian, WPoolStatus, WaitGroup,
    channel::{Channel, Receiver, Sender, bounded, unbounded},
    safe_lock,
};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// `WPool` is a thread pool that limits the number of tasks executing concurrently,
/// without restricting how many tasks can be queued. Submitting tasks is non-blocking,
/// so you can enqueue any number of tasks without waiting.
pub struct WPool {
    dispatch_handle: Mutex<Option<thread::JoinHandle<()>>>,
    is_dispatch_ready: WaitGroup,
    max_workers: usize,
    min_workers: usize,
    panics: Arc<Mutex<Vec<PanicInfo>>>,
    shutdown_lock: Mutex<Channel<()>>,
    status: Arc<AtomicU8>,
    stop_once: Once,
    task_sender: Sender<Signal>,
    worker_count: Arc<AtomicUsize>,
    #[allow(dead_code)]
    pub(crate) waiting_queue_len: Arc<AtomicUsize>,
}

impl WPool {
    // Private "quality-of-life" helper. Makes it so we don't have to update struct fields in multiple places.
    fn new_base(max_workers: usize, min_workers: usize) -> Self {
        assert!(max_workers > 0, "max_workers == 0");
        assert!(max_workers >= min_workers, "min_workers > max_workers");

        let panics = Arc::new(Mutex::new(Vec::new()));
        let status = Arc::new(AtomicU8::new(WPoolStatus::Running.as_u8()));
        let worker_count = Arc::new(AtomicUsize::new(0));
        let waiting_queue_len = Arc::new(AtomicUsize::new(0));
        let worker_channel = bounded(0);
        let task_channel = unbounded();
        let is_dispatch_ready = WaitGroup::new_with_delta(1);

        // Hook all panics.
        let panics_clone = Arc::clone(&panics);
        panic::set_hook(Box::new(move |info| {
            safe_lock(&panics_clone).push(PanicInfo::from(info));
        }));

        let dispatch_handle = Self::dispatch(
            max_workers,
            min_workers,
            Arc::clone(&status),
            Arc::clone(&worker_count),
            Arc::clone(&waiting_queue_len),
            task_channel.clone_receiver(),
            worker_channel.clone(),
            is_dispatch_ready.clone(),
        );

        Self {
            dispatch_handle: Mutex::new(Some(dispatch_handle)),
            is_dispatch_ready,
            max_workers,
            min_workers,
            panics,
            shutdown_lock: Mutex::new(unbounded()),
            status,
            stop_once: Once::new(),
            task_sender: task_channel.clone_sender(),
            waiting_queue_len,
            worker_count,
        }
    }

    /// `new` creates and starts a pool of worker threads.
    ///
    /// The `max_workers` parameter specifies the maximum number of workers that can
    /// execute tasks concurrently. When there are no incoming tasks, workers are
    /// gradually stopped until there are no remaining workers.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let max_workers = 5;
    /// let wp = WPool::new(max_workers);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn new(max_workers: usize) -> Self {
        Self::new_base(max_workers, 0)
    }

    /// `new_with_min` creates and starts a pool of worker threads.
    ///
    /// The `max_workers` parameter specifies the maximum number of workers that can
    /// execute tasks concurrently. When there are no incoming tasks, workers are
    /// gradually stopped until there are no remaining workers.
    ///
    /// The `min_workers` parameter specifies up to the minimum amount of workers that
    /// should aways exist, even when the pool is idle. This is designed to help
    /// eliminate the overhead of spinning up threads from scratch.
    ///
    /// If `min_workers` is greater than `max_workers` we panic.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let max_workers = 5;
    /// let min_workers = 3;
    /// let wp = WPool::new_with_min(max_workers, min_workers);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn new_with_min(max_workers: usize, min_workers: usize) -> Self {
        Self::new_base(max_workers, min_workers)
    }

    /// The number of possible `max_workers`. This does not reflect active workers.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let max_workers = 5;
    /// let wp = WPool::new(max_workers);
    /// assert_eq!(wp.max_capacity(), max_workers);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn max_capacity(&self) -> usize {
        self.max_workers
    }

    /// The number of possible `min_workers`. This does not reflect active workers.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let max_workers = 5;
    /// let min_workers = 3;
    /// let wp = WPool::new_with_min(max_workers, min_workers);
    /// assert_eq!(wp.max_capacity(), max_workers);
    /// assert_eq!(wp.min_capacity(), min_workers);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn min_capacity(&self) -> usize {
        self.min_workers
    }

    /// The number of active workers.
    ///
    /// ```rust
    /// use wpool::WPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let max_workers = 5;
    /// let wp = WPool::new(max_workers);
    ///
    /// // Should have 0 workers here.
    /// assert_eq!(wp.worker_count(), 0);
    ///
    /// for _ in 0..max_workers {
    ///     wp.submit(move || {
    ///         thread::sleep(Duration::from_secs(1));
    ///     });
    /// }
    ///
    /// // Give some time for worker to spawn.
    /// thread::sleep(Duration::from_millis(5));
    ///
    /// // Should have `max_workers` amount of workers.
    /// assert_eq!(wp.worker_count(), max_workers);
    ///
    /// wp.stop_wait();
    ///
    /// // Should have 0 workers now.
    /// assert_eq!(wp.worker_count(), 0);
    /// ```
    ///
    pub fn worker_count(&self) -> usize {
        self.worker_count.load(Ordering::SeqCst)
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
    ///
    pub fn submit<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let t = Task::new(Box::new(task));
        self.submit_signal(Signal::NewTask(t));
    }

    /// Enqueues the given function and blocks until it has been executed.
    /// Unlike `submit_confirm(...)`, this method waits until the job has finished executing.
    /// `submit_confirm(...)` only blocks until the task is either assigned to a worker or queued.
    ///
    /// ```rust
    /// use wpool::WPool;
    /// use std::time::Duration;
    /// use std::thread;
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    ///
    /// let wp = WPool::new(2);
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// let c = Arc::clone(&counter);
    ///
    /// // Block here until submitted job finishes.
    /// wp.submit_wait(move || {
    ///     // If we fail to wait for this job to finish,
    ///     // our assert will run before this `Duration`.
    ///     thread::sleep(Duration::from_secs(1));
    ///     c.fetch_add(1, Ordering::SeqCst);
    /// });
    ///
    /// // Verify we waited for execution.
    /// assert_eq!(counter.load(Ordering::SeqCst), 1);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn submit_wait<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(0);
        self.submit(move || {
            task();
            let _ = tx.send(());
        });
        let _ = rx.recv(); // blocks until complete
    }

    /// Enqueues the given function and blocks until it has been either given to a worker or queued.
    /// Unlike `submit_wait(...)`, this method only blocks until the task is either assigned to
    /// a worker or queued.
    ///
    /// ```rust
    /// use wpool::WPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let max_workers = 5;
    /// let wp = WPool::new(max_workers);
    ///
    /// for i in 1..=max_workers {
    ///     // Will block here until job is *submitted*.
    ///     wp.submit_confirm(|| {
    ///         thread::sleep(Duration::from_secs(2));
    ///     });
    ///     // Now you know that a worker has been spawned, or job
    ///     // placed in queue (which means we are already at max workers).
    ///     assert_eq!(wp.worker_count(), i);
    /// }
    ///
    /// assert_eq!(wp.worker_count(), max_workers);
    /// wp.stop_wait();
    /// ```
    ///
    pub fn submit_confirm<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(0);
        let t = Task::new(Box::new(task));
        let s = Signal::NewTaskWithConfirmation(t, Some(tx));
        self.submit_signal(s);
        let _ = rx.recv();
    }

    /// Stops the pool and waits for currently running tasks, as well as any tasks
    /// in the wait queue, to complete. Task submission is disallowed after
    /// `stop_wait()` has been called.
    ///
    /// **Since creating the pool starts at least one thread, for the dispatcher,
    /// `stop()` or `stop_wait()` should only be called when the worker pool is no
    /// longer needed**.
    ///
    /// ```rust
    /// use wpool::WPool;
    /// use std::time::Duration;
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// use std::thread;
    ///
    /// let max_workers = 3;
    /// // `num_jobs` is greater than `max_workers` so we can get jobs into wait queue.
    /// let num_jobs = 5;
    /// let wp = WPool::new(max_workers);
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// for _ in 0..num_jobs {
    ///     let c = Arc::clone(&counter);
    ///     wp.submit(move || {
    ///         // Sleep for a while so jobs are put into wait queue.
    ///         thread::sleep(Duration::from_secs(1));
    ///         // Increment counter to prove job executed.
    ///         c.fetch_add(1, Ordering::SeqCst);
    ///     });
    /// }
    ///
    /// // Allow currently executing jobs to complete PLUS the wait queue.
    /// wp.stop_wait();
    /// // Verify that all jobs executed.
    /// assert_eq!(counter.load(Ordering::SeqCst), num_jobs);
    /// ```
    ///
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
    ///
    /// ```rust
    /// use wpool::WPool;
    /// use std::time::Duration;
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// use std::thread;
    ///
    /// let max_workers = 3;
    /// // `num_jobs` is greater than `max_workers` so we can get jobs into wait queue.
    /// let num_jobs = 5;
    /// let wp = WPool::new(max_workers);
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// for _ in 0..num_jobs {
    ///     let c = Arc::clone(&counter);
    ///     wp.submit(move || {
    ///         // Sleep for a while so jobs are put into wait queue.
    ///         thread::sleep(Duration::from_secs(1));
    ///         // Increment counter to prove job executed.
    ///         c.fetch_add(1, Ordering::SeqCst);
    ///     });
    /// }
    ///
    /// // Allow currently executing jobs to complete BUT abandoned the wait queue.
    /// wp.stop();
    /// // Verify that only up to `max_workers` jobs were complete.
    /// assert_eq!(counter.load(Ordering::SeqCst), max_workers);
    /// ```
    ///
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
    ///
    /// ```rust
    /// use wpool::WPool;
    /// let wp = WPool::new(2);
    ///
    /// // Suppose you had a long running job that you need to wait for...
    /// wp.submit(|| { /* Doing lots of work */ });
    ///
    /// //
    /// // ...but you had other tasks to do.
    /// //
    /// // Doing unrelated work here...
    /// //
    ///
    /// // Now you need to ensure your long running job is finished.
    /// // `pause()` will block until all currently executing jobs have finished.
    /// wp.pause();
    ///
    /// // Now you know it has finished.
    ///
    /// wp.resume();
    /// wp.stop_wait();
    /// ```
    ///
    pub fn pause(&self) {
        // Acquire lock for duration of this process, so we aren't interrupted by a shutdown.
        let resume_signal = safe_lock(&self.shutdown_lock);

        let status = self.status();
        if matches!(status, WPoolStatus::Stopped(_)) || status == WPoolStatus::Paused {
            return;
        }

        let is_ready = WaitGroup::new_with_delta(self.max_workers);

        for _ in 0..self.max_workers {
            let thread_ready = is_ready.clone();
            let thread_resume_signal = resume_signal.clone_receiver();
            // Inject our pause semantics into a 'regular task' and submit to pool.
            self.submit(move || {
                thread_ready.done();
                let _ = thread_resume_signal.recv();
            });
        }

        is_ready.wait();
        self.set_status(WPoolStatus::Paused);
    }

    /// Resumes/unpauses all paused workers.
    /// If the pool is already stopped or is not paused we return early.
    ///
    /// ```rust
    /// use wpool::WPool;
    /// let wp = WPool::new(2);
    /// wp.pause();
    /// wp.resume();
    /// wp.stop_wait();
    /// ```
    ///
    pub fn resume(&self) {
        // Acquire lock for duration of this process, so we aren't interrupted by a shutdown.
        let mut resume_signal = safe_lock(&self.shutdown_lock);

        let status = self.status();
        if status != WPoolStatus::Paused || matches!(status, WPoolStatus::Stopped(_)) {
            return;
        }

        // Close 'unpause signal' channel to unblock all workers.
        resume_signal.drop_sender();
        // Need to reset resume signal, so if pause is called again, it works.
        *resume_signal = unbounded::<()>();
        self.set_status(WPoolStatus::Running);
    }

    /// Waits for dispatcher thread to spawn.
    /// This is mostly used in tests but I figured it may be useful to expose to the public.
    /// 99.9999999999% of the time you won't use this.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let wp = WPool::new(2);
    /// // Block until dispatch thread has spawned.
    /// wp.wait_ready();
    /// // Dispatch has confirmed spawn.
    /// wp.submit(|| { /* ...work... */ });
    /// wp.stop_wait();
    /// ```
    ///
    pub fn wait_ready(&self) {
        self.is_dispatch_ready.wait()
    }

    /// Returns all PanicInfo for workers that have panicked.
    ///
    /// ```rust
    /// use wpool::WPool;
    ///
    /// let wp = WPool::new(3);
    /// wp.submit(|| panic!("something went wrong!"));
    /// // Wait for currently running jobs to finish.
    /// wp.pause();
    /// println!("{:#?}", wp.get_workers_panic_info());
    /// // [
    /// //     PanicInfo {
    /// //         thread_id: ThreadId(
    /// //             9,
    /// //         ),
    /// //         payload: Some(
    /// //             "something went wrong!",
    /// //         ),
    /// //         file: Some(
    /// //             "src/file.rs",
    /// //         ),
    /// //         line: Some(
    /// //             163,
    /// //         ),
    /// //         column: Some(
    /// //             19,
    /// //         ),
    /// //     },
    /// // ]
    /// wp.stop_wait();
    /// ```
    ///
    pub fn get_workers_panic_info(&self) -> Vec<PanicInfo> {
        safe_lock(&self.panics).to_vec()
    }

    /************************* Private methods ***************************************/

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            self.resume();
            // Acquire lock so we can wait for any in-progress pauses.
            let shutdown_lock = safe_lock(&self.shutdown_lock);
            self.set_status(WPoolStatus::Stopped(wait));
            drop(shutdown_lock);
            // Close tasks channel.
            self.task_sender.drop();
        });

        if let Some(handle) = safe_lock(&self.dispatch_handle).take() {
            let _ = handle.join();
        }
    }

    /// Submit a Signal to the task channel.
    fn submit_signal(&self, signal: Signal) {
        if matches!(self.status(), WPoolStatus::Stopped(_)) {
            return;
        }
        let _ = self.task_sender.send(signal);
    }

    fn status(&self) -> WPoolStatus {
        WPoolStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    fn set_status(&self, status: WPoolStatus) {
        self.status.store(status.as_u8(), Ordering::SeqCst);
    }

    /************************* Private Static methods ********************************/

    /// `dispatch` starts our dispatcher thread. It is responsible for receiving
    /// tasks, dispatching tasks to workers, spawning workers, killing workers
    /// during pool shutdown, and more.
    fn dispatch(
        max_workers: usize,
        min_workers: usize,
        status: Arc<AtomicU8>,
        worker_count: Arc<AtomicUsize>,
        waiting_queue_len: Arc<AtomicUsize>,
        task_receiver: Receiver<Signal>,
        worker_channel: Channel<Signal>,
        is_spawned: WaitGroup,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut is_idle = false;
            let wait_group = WaitGroup::new();
            let mut waiting_queue = VecDeque::new();

            // Set ready flag after thread has spawned but JUST prior to main loop.
            is_spawned.done();

            loop {
                // See `process_waiting_queue` comments for more info.
                if !waiting_queue.is_empty() {
                    if !Self::process_waiting_queue(
                        &mut waiting_queue,
                        &waiting_queue_len,
                        &task_receiver,
                        worker_channel.sender_ref(),
                    ) {
                        break;
                    }
                    continue;
                }

                // Get signal from task channel, handles killing idle workers.
                let mut signal = match task_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                    Ok(signal) => signal,
                    Err(RecvTimeoutError::Timeout) => {
                        if is_idle
                            && worker_count.load(Ordering::SeqCst) > min_workers // Keep min workers alive.
                            && worker_channel.try_send(Signal::Terminate).is_ok()
                        {
                            worker_count.fetch_sub(1, Ordering::SeqCst);
                        }
                        is_idle = true;
                        // No signal, just loop.
                        continue;
                    }
                    Err(_) => break,
                };

                // Got a signal. Process it by placing in wait queue or handing to worker.
                if worker_count.load(Ordering::SeqCst) >= max_workers {
                    Self::confirm_signal_if_needed(&mut signal);
                    waiting_queue.push_back(signal);
                    waiting_queue_len.store(waiting_queue.len(), Ordering::SeqCst);
                } else {
                    wait_group.add(1);
                    Self::spawn_worker(signal, wait_group.clone(), worker_channel.clone_receiver());
                    worker_count.fetch_add(1, Ordering::SeqCst);
                }
                is_idle = false;
            }

            // If `stop_wait()` was called run tasks and waiting queue.
            if WPoolStatus::from_u8(status.load(Ordering::SeqCst)) == WPoolStatus::Stopped(true) {
                Self::run_queued_tasks(
                    &mut waiting_queue,
                    &waiting_queue_len,
                    worker_channel.sender_ref(),
                );
            }

            // Terminate workers as they become available.
            for _ in 0..worker_count.load(Ordering::SeqCst) {
                let _ = worker_channel.send(Signal::Terminate); // Blocking.
                worker_count.fetch_sub(1, Ordering::SeqCst);
            }

            wait_group.wait();
        })
    }

    /// Processes tasks within the waiting queue.
    /// As long as the waiting queue isn't empty, incoming signals (on task channel)
    /// are put into the waiting queue and signals to run are taken from the waiting
    /// queue. Once the waiting queue is empty, then go back to submitting incoming
    /// signals directly to available workers.
    fn process_waiting_queue(
        waiting_queue: &mut VecDeque<Signal>,
        waiting_queue_len: &AtomicUsize,
        task_receiver: &Receiver<Signal>,
        worker_sender: &Sender<Signal>,
    ) -> bool {
        match task_receiver.try_recv() {
            Ok(signal) => {
                waiting_queue.push_back(signal);
                waiting_queue_len.store(waiting_queue.len(), Ordering::SeqCst);
            }
            Err(TryRecvError::Empty) => {
                if let Some(signal) = waiting_queue.front()
                    && let Ok(_) = worker_sender.try_send(signal.clone())
                {
                    // Only pop off (modify) waitiing queue once we know the
                    // signal was successfully passed into the worker channel.
                    waiting_queue.pop_front();
                    waiting_queue_len.store(waiting_queue.len(), Ordering::SeqCst);
                }
            }
            // Task channel closed.
            Err(_) => return false,
        };
        true
    }

    /// Essentially drains the wait_queue.
    fn run_queued_tasks(
        waiting_queue: &mut VecDeque<Signal>,
        waiting_queue_len: &AtomicUsize,
        worker_sender: &Sender<Signal>,
    ) {
        while !waiting_queue.is_empty() {
            // Get a **reference** to the element at the front of waiting queue, if exists.
            if let Some(signal) = waiting_queue.front()
                && let Ok(_) = worker_sender.try_send(signal.clone())
            {
                // Only pop off (modify) waitiing queue once we know the
                // signal was successfully passed into the worker channel.
                waiting_queue.pop_front();
                waiting_queue_len.store(waiting_queue.len(), Ordering::SeqCst);
            }
        }
    }

    /// `spawn_worker` spawns a new thread that acts as a worker. Unless the pool using `min_workers`,
    /// a worker will timeout after an entire cycle of being idle. The idle timeout cycle is ~4 seconds.
    fn spawn_worker(signal: Signal, wait_group: WaitGroup, worker_receiver: Receiver<Signal>) {
        thread::spawn(move || {
            let tg = ThreadGuardian::new((
                Signal::NewTask(Task::noop()),
                wait_group.clone(),
                worker_receiver.clone(),
            ));

            tg.on_panic(|(signal, wait_group, worker_receiver)| {
                Self::spawn_worker(signal, wait_group, worker_receiver);
            });

            let mut signal_maybe = Some(signal);
            while signal_maybe.is_some() {
                match signal_maybe.take().expect("is_some()") {
                    Signal::NewTask(task) => task.run(),
                    Signal::NewTaskWithConfirmation(task, confirm) => {
                        drop(confirm);
                        task.run();
                    }
                    Signal::Terminate => break,
                }
                signal_maybe = match worker_receiver.recv() {
                    Ok(signal) => Some(signal),
                    Err(_) => break,
                }
            }

            wait_group.done();
        });
    }

    /// If `submit_confirm(...)` was called send confirmation, extract the task,
    /// and wrap the extracted task in a plain NewTask.
    fn confirm_signal_if_needed(signal: &mut Signal) {
        if let Signal::NewTaskWithConfirmation(task, confirm) = signal {
            drop(confirm.take());
            *signal = Signal::NewTask(task.to_owned());
        }
    }
}
