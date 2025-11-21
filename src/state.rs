use std::{
    collections::HashMap,
    thread::{self, JoinHandle, ThreadId},
};

use crate::{
    AsWPoolStatus, PanicReport, WPoolStatus,
    channel::{Receiver, Sender, bounded},
};

/***************** MUTABLE SHARED STATE SOURCE OF TRUTH **********************************/
// We use lock-free "shared" mutable state via channels. We spawn a "state management"
// thread that owns all state data, so no need for Arc or locks. All request to read or
// to write state data must be sent via the "state management sender" half of the "state
// manager channel".
//
// If you need to add anything to shared mutable state, add it here! You may also need to
// create helper functions (bottom of file) as well.
pub(crate) struct SharedData {
    // The number of currently alive and active workers.
    pub(crate) worker_count: usize,
    // I really wanted to avoid storing non-primitives in shared state but just letting
    // workers die on their own is not a great practice and causes "leaky threads".
    // We cannot rely on the len of `worker_handles` in place of `worker_count` because
    // we only clean-up dead worker handles when we have time.
    pub(crate) worker_handles: HashMap<ThreadId, Option<JoinHandle<()>>>,
    // Stored as u8 to limit overhead. The `WPoolStatus` enum offers helpers to make
    // interacting with u8 as ergonomic as possible.
    pub(crate) pool_status: u8,
    // Why not just store the entire waiting_queue in state? Because the dispatcher thread
    // is the only thread that needs access to it (it owns the waiting queue) and storing
    // just the count means less overhead.
    pub(crate) waiting_queue_length: usize,
    pub(crate) panic_reports: Vec<PanicReport>,
}

/******** IF YOU ARE ADDING STATE YOU ALSO NEED TO INCLUDE A DEFAULT VALUE HERE! *********/
impl Default for SharedData {
    fn default() -> Self {
        Self {
            worker_count: 0,
            worker_handles: HashMap::new(),
            pool_status: WPoolStatus::Running.as_u8(),
            waiting_queue_length: 0,
            panic_reports: Vec::new(),
        }
    }
}

pub(crate) type CallbackFn = Box<dyn FnOnce(&mut SharedData) + Send>;

pub(crate) enum Message {
    Callback(CallbackFn),
    WokerTerminating(ThreadId),
    InsertWorker(ThreadId, Option<JoinHandle<()>>),
    TaskPanic(PanicReport),
}

/// Spawn a state manager thread. The state manager is the source of truth for all shared state.
/// It listens for state mutation requests and state retreival requests.
pub(crate) fn spawn_manager(
    receiver: Receiver<Message>,
    initial_state: Option<SharedData>,
) -> JoinHandle<()> {
    let mut state = initial_state.unwrap_or_default();
    thread::spawn(move || {
        while let Ok(query) = receiver.recv() {
            match query {
                Message::Callback(callback_fn) => callback_fn(&mut state),
                Message::TaskPanic(panic_info) => {
                    state.panic_reports.push(panic_info);
                }
                Message::WokerTerminating(id) => {
                    if let Some(mut handle_opt) = state.worker_handles.remove(&id)
                        && let Some(handle) = handle_opt.take()
                    {
                        // Do not decrement worker count here. That is something the caller needs
                        // to explicitly handle from the callsite.
                        let _ = handle.join();
                    }
                }
                Message::InsertWorker(thread_id, join_handle) => {
                    state.worker_handles.insert(thread_id, join_handle);
                }
            }
        }
    })
}

/// Provide a query function (callback function) that is passed a mutable refernce to current state.
pub(crate) fn query<R, F>(sender: &Sender<Message>, query_fn: F) -> R
where
    R: Send + std::fmt::Debug + 'static,
    F: FnOnce(&mut SharedData) -> R + Send + 'static,
{
    let chan = bounded(0);
    let reply = chan.clone_sender();
    let closure = move |state: &mut SharedData| {
        let _ = reply.send(query_fn(state));
    };
    let _ = sender.send(Message::Callback(Box::new(closure)));
    chan.recv().expect("state to exist")
}

#[derive(Clone)]
pub(crate) struct StateOps {
    sender: Sender<Message>,
}

impl StateOps {
    pub(crate) fn new(sender: Sender<Message>) -> Self {
        Self { sender }
    }

    pub(crate) fn drop_sender(&self) {
        self.sender.drop();
    }

    pub(crate) fn clone_sender(&self) -> Sender<Message> {
        self.sender.clone()
    }

    pub(crate) fn worker_count(&self) -> usize {
        query(&self.sender, |state| state.worker_count)
    }

    pub(crate) fn inc_worker_count(&self) {
        query(&self.sender, |state| state.worker_count += 1);
    }

    pub(crate) fn dec_worker_count(&self) {
        query(&self.sender, |state| state.worker_count -= 1);
    }

    pub(crate) fn pool_status(&self) -> WPoolStatus {
        query(&self.sender, |state| state.pool_status.as_enum())
    }

    pub(crate) fn set_pool_status(&self, status: WPoolStatus) {
        query(&self.sender, move |state| {
            state.pool_status = status.as_u8()
        });
    }

    pub(crate) fn waiting_queue_len(&self) -> usize {
        query(&self.sender, |state| state.waiting_queue_length)
    }

    pub(crate) fn set_waiting_queue_len(&self, len: usize) {
        query(&self.sender, move |state| state.waiting_queue_length = len);
    }

    pub(crate) fn panic_reports(&self) -> Vec<PanicReport> {
        query(&self.sender, |state| state.panic_reports.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn insert_worker_handle(&self, key: ThreadId, value: JoinHandle<()>) {
        query(&self.sender, move |state| {
            state.worker_handles.insert(key, Some(value))
        });
    }

    pub(crate) fn join_worker_handles(&self) {
        query(&self.sender, |state| {
            for (_, handle_opt) in state.worker_handles.iter_mut() {
                if let Some(handle) = handle_opt.take() {
                    let _ = handle.join();
                }
            }
        })
    }
}
