use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicU8, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError, TryRecvError, TrySendError},
    },
    thread,
    time::Duration,
};

use crate::{
    Signal, Task, WPoolStatus,
    channel::{Channel, Receiver, Sender, bounded, unbounded},
    safe_lock,
    wait_group::WaitGroup,
};

pub(crate) static WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// `WPool` is a thread pool that limits the number of tasks executing concurrently,
/// without restricting how many tasks can be queued. Submitting tasks is non-blocking,
/// so you can enqueue any number of tasks without waiting.
pub struct WPool {
    dispatch_handle: Mutex<Option<thread::JoinHandle<()>>>,
    max_workers: usize,
    min_workers: usize,
    pause_channel: Channel<()>,
    pause_lock: Mutex<()>,
    status: Arc<AtomicU8>,
    stop_once: Once,
    task_sender: Sender<Signal>,
    #[allow(dead_code)]
    waiting_queue: Arc<Mutex<VecDeque<Signal>>>,
    worker_count: Arc<AtomicUsize>,
}

impl WPool {
    /// `new` creates and starts a pool of worker threads.
    ///
    /// The `max_workers` parameter specifies the maximum number of workers that can
    /// execute tasks concurrently. When there are no incoming tasks, workers are
    /// gradually stopped until there are no remaining workers.
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
    /// If `min_workers` is greater than `max_workers` then we change `min_workers` to
    /// equal `max_workers`.
    pub fn new_with_min(max_workers: usize, min_workers: usize) -> Self {
        Self::new_base(max_workers, min_workers)
    }

    /// The number of `max_workers`.
    pub fn max_capacity(&self) -> usize {
        self.max_workers
    }

    /// The number of `min_workers`.
    pub fn min_capacity(&self) -> usize {
        self.min_workers
    }

    /// The number of active workers.
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
        let _pause_lock = safe_lock(&self.pause_lock);
        if self.is_stopped() || self.is_paused() {
            return;
        }

        for _ in 0..self.max_workers {
            let sender = self.pause_channel.clone_sender();
            let receiver = self.pause_channel.clone_receiver();
            self.submit_signal(Signal::Pause(sender, receiver));
        }

        // Blocks until all workers tell us they're paused.
        for _ in 0..self.max_workers {
            let _ = self.pause_channel.recv();
        }

        self.set_status(WPoolStatus::Paused);
    }

    /// Resumes/unpauses all paused workers.
    pub fn resume(&self) {
        // We want to hold the pause lock for the entirety of the resume operation.
        let _pause_lock = safe_lock(&self.pause_lock);
        if self.is_stopped() || !self.is_paused() {
            return;
        }
        for _ in 0..self.max_workers {
            let _ = self.pause_channel.send(());
        }
        self.set_status(WPoolStatus::Running);
    }

    /************************* Crate methods *****************************************/

    pub(crate) fn _waiting_queue_len(&self) -> usize {
        safe_lock(&self.waiting_queue).len()
    }

    /************************* Private methods ***************************************/

    // Private "quality-of-life" helper. Makes it so we don't have to update struct fields in multiple places.
    fn new_base(max_workers: usize, min_workers: usize) -> Self {
        let status = Arc::new(AtomicU8::new(WPoolStatus::Running.as_u8()));
        let worker_count = Arc::new(AtomicUsize::new(0));
        let waiting_queue = Arc::new(Mutex::new(VecDeque::new()));
        let worker_channel = bounded(0);
        let task_channel = unbounded();

        let dispatch_handle = Self::dispatch(
            max_workers,
            min_workers,
            Arc::clone(&status),
            Arc::clone(&worker_count),
            Arc::clone(&waiting_queue),
            task_channel.clone_receiver(),
            worker_channel.clone_sender(),
            worker_channel.clone_receiver(),
        );

        Self {
            dispatch_handle: Mutex::new(Some(dispatch_handle)),
            max_workers,
            min_workers,
            pause_channel: bounded(0),
            pause_lock: Mutex::new(()),
            status,
            stop_once: Once::new(),
            task_sender: task_channel.clone_sender(),
            waiting_queue,
            worker_count,
        }
    }

    /// `dispatch` starts our dispatcher thread. It is responsible for receiving
    /// tasks, dispatching tasks to workers, spawning workers, killing workers
    /// during pool shutdown, and more.
    fn dispatch(
        max_workers: usize,
        min_workers: usize,
        status: Arc<AtomicU8>,
        worker_count: Arc<AtomicUsize>,
        waiting_queue: Arc<Mutex<VecDeque<Signal>>>,
        task_receiver: Receiver<Signal>,
        worker_sender: Sender<Signal>,
        worker_receiver: Receiver<Signal>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut is_idle = false;
            let wait_group = WaitGroup::new();

            loop {
                // See `process_waiting_queue` method for more notes on this.
                if !safe_lock(&waiting_queue).is_empty() {
                    if !Self::process_waiting_queue(
                        Arc::clone(&waiting_queue),
                        task_receiver.clone(),
                        worker_sender.clone(),
                    ) {
                        break;
                    }
                    continue;
                }

                let signal = match task_receiver.recv_timeout(WORKER_IDLE_TIMEOUT) {
                    Ok(signal) => signal,
                    Err(RecvTimeoutError::Timeout) => {
                        if is_idle && worker_count.load(Ordering::SeqCst) > min_workers {
                            let _ = worker_sender.send(Signal::Terminate);
                            worker_count.fetch_sub(1, Ordering::SeqCst);
                        }
                        is_idle = true;
                        continue;
                    }
                    Err(_) => break,
                };

                match worker_sender.try_send(signal) {
                    Ok(_) => { /* Noop */ }
                    Err(TrySendError::Disconnected(_)) => {
                        println!(
                            "dispatch -> send signal to workers -> WORKER CHAN CLOSED, BREAKING MAIN LOOP."
                        );
                        break;
                    }
                    Err(TrySendError::Full(signal)) => {
                        if worker_count.load(Ordering::SeqCst) >= max_workers {
                            safe_lock(&waiting_queue).push_back(signal);
                        } else {
                            wait_group.add(1);
                            Self::spawn_worker(signal, wait_group.clone(), worker_receiver.clone());
                            worker_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                };

                is_idle = false;
            }

            if status.load(Ordering::SeqCst) == WPoolStatus::Stopped(true).as_u8() {
                Self::run_queued_tasks(Arc::clone(&waiting_queue), worker_sender.clone());
            }

            // Terminate workers as they become available.
            for _ in 0..worker_count.load(Ordering::SeqCst) {
                let _ = worker_sender.send(Signal::Terminate);
                worker_count.fetch_sub(1, Ordering::SeqCst);
            }

            wait_group.wait();
        })
    }

    /// `spawn_worker` spawns a new thread that acts as a worker. Unless the pool using `min_workers`,
    /// a worker will timeout after an entire cycle of being idle. The idle timeout cycle is ~4 seconds.
    fn spawn_worker(signal: Signal, wait_group: WaitGroup, worker_receiver: Receiver<Signal>) {
        thread::spawn(move || {
            let mut signal_opt = Some(signal);
            while signal_opt.is_some() {
                match signal_opt.take().expect("is_some()") {
                    Signal::NewTask(task) => task.run(),
                    Signal::Pause(sender, receiver) => {
                        let _ = sender.send(()); // Send 'paused ack'.
                        let _ = receiver.recv(); // Blocking.
                    }
                    Signal::Terminate => break,
                }
                signal_opt = match worker_receiver.recv() {
                    Ok(signal) => Some(signal),
                    Err(_) => break,
                }
            }
            wait_group.done();
            println!("worker -> exiting");
        });
    }

    /// Processes tasks within the waiting queue.
    /// As long as the waiting queue isn't empty, incoming signals (on task channel)
    /// are put into the waiting queue and signals to run are taken from the waiting
    /// queue. Once the waiting queue is empty, then go back to submitting incoming
    /// signals directly to available workers.
    fn process_waiting_queue(
        waiting_queue: Arc<Mutex<VecDeque<Signal>>>,
        task_receiver: Receiver<Signal>,
        worker_sender: Sender<Signal>,
    ) -> bool {
        match task_receiver.try_recv() {
            Ok(signal) => safe_lock(&waiting_queue).push_back(signal),
            Err(TryRecvError::Empty) => {
                if safe_lock(&waiting_queue).front().is_some()
                    && worker_sender
                        .send(safe_lock(&waiting_queue).front().expect("front").clone())
                        .is_ok()
                {
                    let _ = safe_lock(&waiting_queue).pop_front();
                }
            }
            Err(_) => return false, // Task channel closed.
        };
        true
    }

    /// Essentially drains the wait_queue.
    fn run_queued_tasks(
        waiting_queue: Arc<Mutex<VecDeque<Signal>>>,
        worker_sender: Sender<Signal>,
    ) {
        // Acquire lock for entirety of this process.
        let mut wq = safe_lock(&waiting_queue);
        while !wq.is_empty() {
            if wq.front().is_some() {
                let signal = wq.pop_front().expect("front");
                let _ = worker_sender.send(signal);
            }
        }
    }

    /// Submit a Signal to the "task channel".
    fn submit_signal(&self, signal: Signal) {
        if self.is_stopped() {
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

    fn _is_running(&self) -> bool {
        self.status() == WPoolStatus::Running
    }

    fn is_paused(&self) -> bool {
        self.status() == WPoolStatus::Paused
    }

    fn is_stopped(&self) -> bool {
        matches!(self.status(), WPoolStatus::Stopped(_))
    }

    fn _is_stopped_waiting(&self) -> bool {
        self.status() == WPoolStatus::Stopped(true)
    }

    fn shutdown(&self, wait: bool) {
        self.stop_once.call_once(|| {
            self.resume();
            // Acquire pause lock to wait for any pauses in progress to complete
            let _pause_lock = safe_lock(&self.pause_lock);
            self.set_status(WPoolStatus::Stopped(wait));
            drop(_pause_lock);
            self.task_sender.drop();
        });
        if let Some(handle) = safe_lock(&self.dispatch_handle).take() {
            let _ = handle.join();
        }
    }
}
