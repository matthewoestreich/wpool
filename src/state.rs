use std::{
    collections::HashMap,
    thread::{self, JoinHandle, ThreadId},
};

use crate::{
    WPoolStatus,
    channel::{Receiver, Sender, bounded},
};

pub(crate) struct StateData {
    pub(crate) worker_count: usize,
    #[allow(dead_code)]
    pub(crate) worker_handles: HashMap<ThreadId, Option<JoinHandle<()>>>,
    pub(crate) pool_status: u8,
    pub(crate) waiting_queue_length: usize,
}

#[allow(clippy::derivable_impls)]
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

pub(crate) fn query_state<R, F>(sender: &Sender<Query>, f: F) -> R
where
    R: Send + std::fmt::Debug + 'static,
    F: FnOnce(&mut StateData) -> R + Send + 'static,
{
    let chan = bounded(0);
    let reply = chan.clone_sender();
    let closure = move |state: &mut StateData| {
        let res = f(state);
        let _ = reply.send(res);
    };
    sender.send(Query::Closure(Box::new(closure))).unwrap();
    chan.recv().expect("state to exist")
}
