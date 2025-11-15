use std::{
    collections::HashMap,
    thread::{self, JoinHandle, ThreadId},
};

use crate::{
    AsWPoolStatus, WPoolStatus,
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
pub(crate) struct StateData {
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
}

/******** IF YOU ARE ADDING STATE YOU ALSO NEED TO INCLUDE A DEFAULT VALUE HERE! *********/
impl Default for StateData {
    fn default() -> Self {
        Self {
            worker_count: 0,
            worker_handles: HashMap::new(),
            pool_status: WPoolStatus::Running.as_u8(),
            waiting_queue_length: 0,
        }
    }
}

pub(crate) enum Query {
    Closure(Box<dyn FnOnce(&mut StateData) + Send>),
}

pub(crate) struct StateManager {
    #[allow(dead_code)]
    state: StateData,
}

impl StateManager {
    pub(crate) fn spawn(
        receiver: Receiver<Query>,
        initial_state: Option<StateData>,
    ) -> JoinHandle<()> {
        let mut state = initial_state.unwrap_or_default();
        thread::spawn(move || {
            while let Ok(action) = receiver.recv() {
                match action {
                    Query::Closure(f) => f(&mut state),
                }
            }
        })
    }
}

// For organizations sake, store state helper fns and state mutation helper fns as static methods.
pub(crate) struct StateMut {}

#[allow(dead_code)]
impl StateMut {
    pub(crate) fn query_state<R, F>(sender: &Sender<Query>, query_fn: F) -> R
    where
        R: Send + std::fmt::Debug + 'static,
        F: FnOnce(&mut StateData) -> R + Send + 'static,
    {
        let chan = bounded(0);
        let reply = chan.clone_sender();
        let closure = move |state: &mut StateData| {
            let _ = reply.send(query_fn(state));
        };
        sender.send(Query::Closure(Box::new(closure))).unwrap();
        chan.recv().expect("state to exist")
    }

    pub(crate) fn get_worker_count(state_sender: &Sender<Query>) -> usize {
        Self::query_state(state_sender, |state| state.worker_count)
    }

    pub(crate) fn increment_worker_count(state_sender: &Sender<Query>) {
        Self::query_state(state_sender, |state| state.worker_count += 1);
    }

    pub(crate) fn decrement_worker_count(state_sender: &Sender<Query>) {
        Self::query_state(state_sender, |state| state.worker_count -= 1);
    }

    pub(crate) fn get_pool_status(state_sender: &Sender<Query>) -> WPoolStatus {
        Self::query_state(state_sender, |state| state.pool_status.as_enum())
    }

    pub(crate) fn set_pool_status(state_sender: &Sender<Query>, status: WPoolStatus) {
        Self::query_state(state_sender, move |state| {
            state.pool_status = status.as_u8()
        });
    }

    pub(crate) fn get_waiting_queue_len(state_sender: &Sender<Query>) -> usize {
        Self::query_state(state_sender, |state| state.waiting_queue_length)
    }

    pub(crate) fn set_waiting_queue_len(state_sender: &Sender<Query>, len: usize) {
        Self::query_state(state_sender, move |state| state.waiting_queue_length = len);
    }

    pub(crate) fn increment_waiting_queue_len(state_sender: &Sender<Query>) {
        Self::query_state(state_sender, move |state| state.waiting_queue_length += 1);
    }

    pub(crate) fn decrement_waiting_queue_len(state_sender: &Sender<Query>) {
        Self::query_state(state_sender, move |state| state.waiting_queue_length -= 1);
    }

    pub(crate) fn insert_worker_handle(
        state_sender: &Sender<Query>,
        key: ThreadId,
        value: JoinHandle<()>,
    ) {
        Self::query_state(state_sender, move |state| {
            state.worker_handles.insert(key, Some(value))
        });
    }

    pub(crate) fn join_all_worker_handles(state_sender: &Sender<Query>) {
        Self::query_state(state_sender, |state| {
            for (_, h_opt) in state.worker_handles.iter_mut() {
                if let Some(h) = h_opt.take() {
                    let _ = h.join();
                }
            }
        })
    }
}
