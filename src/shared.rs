use std::{
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    thread,
};

use crate::{
    WPoolStatus,
    channel::{Receiver, Sender, unbounded},
};

pub(crate) enum Action<Value> {
    Increment,
    Decrement,
    Store(Value),
}

pub(crate) enum Query<Action> {
    Get,
    Set(Action),
}

pub(crate) enum State {
    WorkerCount(Query<Action<usize>>, Option<Sender<usize>>),
    WaitingQueueLength(Query<Action<usize>>, Option<Sender<usize>>),
    PoolStatus(Query<u8>, Option<Sender<u8>>),
}

pub(crate) struct Manager {
    #[allow(dead_code)]
    handle: Option<thread::JoinHandle<()>>,
}

impl Manager {
    pub(crate) fn spawn(receiver: Receiver<State>) -> Self {
        let status = AtomicU8::new(WPoolStatus::Running.as_u8());
        let worker_count = AtomicUsize::new(0);
        let waiting_queue_len = AtomicUsize::new(0);

        Self {
            handle: Some(thread::spawn(move || {
                while let Ok(query) = receiver.recv() {
                    match query {
                        State::WorkerCount(method, reply) => match method {
                            Query::Get => {
                                _ = reply
                                    .expect("reply chan")
                                    .send(worker_count.load(Ordering::SeqCst));
                            }
                            Query::Set(set_type) => match set_type {
                                Action::Increment => {
                                    _ = worker_count.fetch_add(1, Ordering::SeqCst)
                                }
                                Action::Decrement => {
                                    _ = worker_count.fetch_sub(1, Ordering::SeqCst)
                                }
                                Action::Store(value) => worker_count.store(value, Ordering::SeqCst),
                            },
                        },
                        State::WaitingQueueLength(method, reply) => match method {
                            Query::Get => {
                                _ = reply
                                    .expect("reply chan")
                                    .send(waiting_queue_len.load(Ordering::SeqCst))
                            }
                            Query::Set(set_type) => match set_type {
                                Action::Increment => {
                                    _ = waiting_queue_len.fetch_add(1, Ordering::SeqCst)
                                }
                                Action::Decrement => {
                                    _ = waiting_queue_len.fetch_sub(1, Ordering::SeqCst)
                                }
                                Action::Store(value) => {
                                    waiting_queue_len.store(value, Ordering::SeqCst)
                                }
                            },
                        },
                        State::PoolStatus(method, reply) => match method {
                            Query::Get => {
                                _ = reply
                                    .expect("reply chan")
                                    .send(status.load(Ordering::SeqCst))
                            }
                            Query::Set(value) => status.store(value, Ordering::SeqCst),
                        },
                    };
                }
            })),
        }
    }
}

pub(crate) fn get_wait_queue_len(state_mgmt: &Sender<State>) -> usize {
    let chan = unbounded();
    let _ = state_mgmt.send(State::WaitingQueueLength(
        Query::Get,
        Some(chan.clone_sender()),
    ));
    chan.recv().expect("WaitQueueLength answer")
}

pub(crate) fn get_pool_status(state_mgmt: &Sender<State>) -> WPoolStatus {
    let chan = unbounded();
    let _ = state_mgmt.send(State::PoolStatus(Query::Get, Some(chan.clone_sender())));
    let r = chan.recv();
    WPoolStatus::from_u8(r.expect("WPoolStatus answer"))
}

pub(crate) fn set_pool_status(state_mgmt: &Sender<State>, status: WPoolStatus) {
    let _ = state_mgmt.send(State::PoolStatus(Query::Set(status.as_u8()), None));
}

pub(crate) fn get_worker_count(state_mgmt: &Sender<State>) -> usize {
    let chan = unbounded();
    let _ = state_mgmt.send(State::WorkerCount(Query::Get, Some(chan.clone_sender())));
    chan.recv().expect("WorkerCount answer")
}

#[allow(dead_code)]
pub(crate) fn set_worker_count(state_mgmt: &Sender<State>, value: usize) {
    let _ = state_mgmt.send(State::WorkerCount(Query::Set(Action::Store(value)), None));
}

pub(crate) fn increment_worker_count(state_mgmt: &Sender<State>) {
    let _ = state_mgmt.send(State::WorkerCount(Query::Set(Action::Increment), None));
}

pub(crate) fn decrement_worker_count(state_mgmt: &Sender<State>) {
    let _ = state_mgmt.send(State::WorkerCount(Query::Set(Action::Decrement), None));
}
