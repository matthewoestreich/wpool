use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};

use crate::{
    channel::Channel,
    job::Signal,
    monotonic_counter, safe_lock,
    worker::{Worker, WorkerStatus},
};

//
// Dispatcher is meant to route signals to workers, spawn and/or kil workers,
// listen for any status updates from workers, and holds the 'source-of-truth'
// list containing all active worker threads.
//
// To acheive these goals, it spawns 2 threads : a "worker status handler thread" and
// a "main dispatcher thread".
//
// The "worker status handler thread":
//   - listens for any status updates from worker threads and handles them accordingly
//
// The "main dispatcher thread":
//   - listens for incoming tasks and routes them to workers
//   - spawns new workers if needed
//   - terminates workers during pool shutdown
//   - terminates the "worker status handler thread" during pool shutdown
//
pub(crate) struct Dispatcher {
    pub(crate) waiting: AtomicBool,
    pub(crate) waiting_queue: Mutex<VecDeque<Signal>>,
    pub(crate) workers: Mutex<HashMap<usize, Worker>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    has_spawned: AtomicBool,
    worker_channel: Channel<Mutex<Option<Sender<Signal>>>, Receiver<Signal>>,
    task_channel: Channel<Mutex<Option<Sender<Signal>>>, Receiver<Signal>>,
    worker_status_channel: Channel<Mutex<Option<Sender<WorkerStatus>>>, Receiver<WorkerStatus>>,
    max_workers: usize,
}

impl Dispatcher {
    pub(crate) fn new(max_workers: usize) -> Self {
        let (task_tx, task_rx) = unbounded();
        let (status_tx, status_rx) = unbounded();
        let (worker_tx, worker_rx) = bounded(0);

        Self {
            has_spawned: false.into(),
            handle: None.into(),
            waiting: false.into(),
            max_workers,
            waiting_queue: VecDeque::new().into(),
            worker_channel: Channel::new(Some(worker_tx).into(), worker_rx),
            worker_status_channel: Channel::new(Some(status_tx).into(), status_rx),
            task_channel: Channel::new(Some(task_tx).into(), task_rx),
            workers: HashMap::new().into(),
        }
    }

    pub(crate) fn spawn(self: Arc<Self>) -> Arc<Self> {
        if self.has_spawned.swap(true, Ordering::SeqCst) {
            return self;
        }

        let get_next_id = monotonic_counter();
        // Copy of dispatcher for the "main dispatcher thread".
        let dispatcher = Arc::clone(&self);
        // Copy of dispatcher for the "worker status handler thread".
        let handler = Arc::clone(&self);

        // This is the "worker status handler thread".
        // Since the dispatchers holds a record of all spawned worker threads, we need to know when
        // a worker terminates itself so we can update said record. A worker will terminate itself
        // if it did not receive a signal within the timeout duration.
        let worker_status_handle = thread::spawn(move || {
            while let Ok(WorkerStatus::Terminating(id)) = handler.worker_status_channel.recv() {
                handler.workers_remove_and_join(&id);
            }
        });

        // This is the "main dispatcher thread".
        // It is responsible for receiving tasks, dispatching tasks to workers, spawning
        // workers, killing workers during pool shutdown, holding the 'source of truth'
        // list for all spawned worker threads, and more.
        *safe_lock(&self.handle) = Some(thread::spawn(move || {
            loop {
                // As long as the waiting queue isn't empty, incoming signals (on task channel)
                // are put into the waiting queue and signals to run are taken from the waiting
                // queue. Once the waiting queue is empty, then go back to submitting incoming
                // signals directly to available workers.
                if !dispatcher.is_waiting_queue_empty() {
                    if !dispatcher.process_waiting_queue() {
                        break;
                    }
                    continue;
                }

                let signal = match dispatcher.task_channel.recv() {
                    Ok(signal) => signal,
                    Err(_) => break,
                };

                // At max workers
                if dispatcher.workers_len() >= dispatcher.max_workers {
                    println!("dispatcher -> at max workers, putting signal in wait queue");
                    dispatcher.waiting_queue_push_back(signal);
                    continue;
                }

                println!("dispatcher -> not at max workers, spawning");

                if let Some(status_sender) = dispatcher.worker_status_channel.clone_sender() {
                    let id = get_next_id();
                    dispatcher.workers_insert(
                        id,
                        Worker::spawn(
                            id,
                            dispatcher.worker_channel.clone_receiver(),
                            status_sender,
                            signal,
                        ),
                    );
                }
            }

            // If the user has called `.stop_wait()`, wait for the waiting queue to also finish.
            if dispatcher.is_waiting() {
                dispatcher.run_queued_tasks();
            }

            println!("    dispatcher -> killing all workers");
            dispatcher.kill_all_workers();
            println!("    dispatcher -> closing worker status channel.");
            dispatcher.worker_status_channel.close();
            let _ = worker_status_handle.join();
            println!("    dispatcher -> exiting dispatch thread");
        }));

        self
    }

    pub(crate) fn submit(&self, signal: Signal) {
        let _ = self.task_channel.send(signal);
    }

    pub(crate) fn close_task_channel(&self) {
        self.task_channel.close();
    }

    #[allow(dead_code)]
    pub(crate) fn close_worker_channel(&self) {
        self.worker_channel.close();
    }

    #[allow(dead_code)]
    pub(crate) fn waiting_queue_len(&self) -> usize {
        safe_lock(&self.waiting_queue).len()
    }

    pub(crate) fn join(&self) {
        if let Some(handle) = safe_lock(&self.handle).take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::SeqCst)
    }

    pub(crate) fn set_is_waiting(&self, is_waiting: bool) {
        self.waiting.store(is_waiting, Ordering::SeqCst);
    }

    fn waiting_queue_push_back(&self, signal: Signal) {
        safe_lock(&self.waiting_queue).push_back(signal);
    }

    // NOTE: this does not remove anything from underlying queue!!!
    fn waiting_queue_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).front().cloned()
    }

    fn waiting_queue_pop_front(&self) -> Option<Signal> {
        safe_lock(&self.waiting_queue).pop_front()
    }

    fn is_waiting_queue_empty(&self) -> bool {
        safe_lock(&self.waiting_queue).is_empty()
    }

    fn workers_len(&self) -> usize {
        safe_lock(&self.workers).len()
    }

    fn _workers_is_empty(&self) -> bool {
        safe_lock(&self.workers).is_empty()
    }

    fn workers_insert(&self, id: usize, worker: Worker) -> Option<Worker> {
        safe_lock(&self.workers).insert(id, worker)
    }

    fn workers_remove(&self, element: &usize) -> Option<Worker> {
        safe_lock(&self.workers).remove(element)
    }

    fn workers_remove_and_join(&self, id: &usize) {
        if let Some(mut worker) = self.workers_remove(id) {
            worker.join();
        }
    }

    fn kill_all_workers(&self) {
        let mut workers = safe_lock(&self.workers);
        println!("[kill_workers] sending kill signal to all workers");
        // Send kill signal to workers as they become available
        for _ in workers.iter() {
            println!("    [kill_workers] BEFFORE -> ab to send kill signal");
            // Will block until a ready worker calls recv
            let send_kill_result = self.worker_channel.send(Signal::Terminate);
            println!("    [kill_workers] AFTER -> send kill signal result={send_kill_result:?}");
        }
        println!("[kill_workers] joining worker handles");
        for (_, mut worker) in workers.drain() {
            worker.join();
        }
        println!("[kill_workers] all workers should be dead");
    }

    /*
    fn process_waiting_queue(&self) -> bool {
        use crossbeam_channel::Select;

        let task_receiver = self.task_channel.clone_receiver();
        let worker_sender = if let Some(sender) = self.worker_channel.clone_sender() {
            sender
        } else {
            println!("proc_wait_que -> ERROR -> unable to acquire worker_sender from self! Exiting");
            return false;
        };

        let mut cb_select = Select::new();
        let task_ready = cb_select.recv(&task_receiver);
        let worker_ready = cb_select.send(&worker_sender);

        // Try select without blocking
        match cb_select.try_select() {
            Ok(operation) => {
                if operation.index() == task_ready {
                    println!("proc_wait_que -> task_ready | wait_que_len={}", self.waiting_queue_len());
                    // Got new task
                    if let Ok(signal) = operation.recv(&task_receiver) {
                        println!("    proc_wait_que -> task_ready | task -> wait_queue | wait_que_len={}", self.waiting_queue_len());
                        self.waiting_queue_push_back(signal);
                        return true;
                    }
                    println!("    proc_wait_que -> task_ready | could not get task, continuing | wait_que_len={}", self.waiting_queue_len());
                } else if operation.index() == worker_ready {
                    println!("    proc_wait_que -> worker_ready | wait_que_len={}", self.waiting_queue_len());
                    // Can send waiting signal to worker
                    if let Some(signal) = self.waiting_queue_pop_front() {
                        println!("    proc_wait_que -> worker_ready | wait_que -> worker_chan | wait_que_len={}", self.waiting_queue_len());
                        let _ = operation.send(&worker_sender, signal);
                        return true;
                    }
                    println!("    proc_wait_que -> worker_ready | nothing in wait queue, continuing | wait_que_len={}", self.waiting_queue_len());
                }
                println!("    proc_wait_que -> RETURNING FALSE | wait_que_len={}", self.waiting_queue_len());
                false
            }
            Err(_) => {
                // Nothing ready
                println!("proc_wait_que -> NEITHER TASK OR WORKER CHAN READY, RETURNING FALSE | wait_que_len={}", self.waiting_queue_len());
                false
            },
        }
    }
    */

    fn process_waiting_queue(&self) -> bool {
        match self.task_channel.try_recv() {
            Ok(signal) => {
                println!(
                    "process_wait_queue -> got signal! start | wait queue size = {}",
                    self.waiting_queue_len()
                );
                self.waiting_queue_push_back(signal);
                println!(
                    "    process_wait_queue -> got signal! end : (sent sgnal from task chan to wait que) | wait queue size = {}",
                    self.waiting_queue_len()
                );
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                // Something exists in wait queue
                if self.waiting_queue_front().is_some() {
                    if self
                        .worker_channel
                        .send(self.waiting_queue_front().expect("already checked"))
                        .is_ok()
                    {
                        let _ = self.waiting_queue_pop_front();
                        println!(
                            "    process_wait_queue -> task channel empty -> worker ready! start : popped_front on wait queue | wait queue size = {}",
                            self.waiting_queue_len()
                        );
                        return true;
                    }
                    println!(
                        "process_wait_queue -> worker channel is closed! | wait queue size = {}",
                        self.waiting_queue_len()
                    );
                }
                println!("    process_wait_queue -> nothing in wait queue...");
            }
            Err(_) => {
                // Task channel closed
                println!(
                    "process_wait_queue -> RETURNING FALSE -> task channel closed | wait queue size = {}",
                    self.waiting_queue_len()
                );
                return false;
            }
        }
        println!(
            "process_wait_queue -> at end RETURNING TRUE | wait queue size = {}",
            self.waiting_queue_len()
        );
        true
    }

    fn run_queued_tasks(&self) {
        let mut wq = safe_lock(&self.waiting_queue);
        while !wq.is_empty() {
            if wq.front().is_some() {
                let _ = self
                    .worker_channel
                    .send(wq.pop_front().expect("already checked front"));
            }
        }
    }
}
