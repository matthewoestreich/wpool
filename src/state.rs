use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU8, AtomicUsize},
    },
    thread::{self, JoinHandle, ThreadId},
};

use crate::{
    WPoolStatus,
    channel::{Receiver, Sender, bounded},
};

pub(crate) struct StateData {
    pub(crate) worker_count: AtomicUsize,
    #[allow(dead_code)]
    pub(crate) worker_handles: Mutex<HashMap<ThreadId, Option<JoinHandle<()>>>>,
    pub(crate) pool_status: AtomicU8,
    pub(crate) waiting_queue_length: AtomicUsize,
}

#[allow(clippy::derivable_impls)]
impl Default for StateData {
    fn default() -> Self {
        Self {
            worker_count: AtomicUsize::new(0),
            worker_handles: Mutex::new(HashMap::new()),
            pool_status: AtomicU8::new(WPoolStatus::Running.as_u8()),
            waiting_queue_length: AtomicUsize::new(0),
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

pub fn query_state<R, F>(sender: &Sender<Query>, f: F) -> R
where
    R: Send + std::fmt::Debug + 'static,
    F: FnOnce(&mut StateData) -> R + Send + 'static,
{
    println!("  query_state -> start");
    let chan = bounded(0);
    let reply = chan.clone_sender();
    let closure = move |state: &mut StateData| {
        let res = f(state);
        match reply.send(res) {
            Ok(_) => println!(">> query_state -> from closure, sent success"),
            Err(e) => println!(">> query_state -> from closure, error sending : {e:?}"),
        }
    };
    println!("    query_state -> sending query to manager");
    sender.send(Query::Closure(Box::new(closure))).unwrap();
    println!("    query_state -> DONE sending query to manager");
    let got = chan.recv().expect("state to exist");
    println!("    query_state -> end -> returning {got:?}");
    got
}
