use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle, ThreadId},
};

use crate::{AsWPoolStatus, PanicReport, WPoolStatus, safe_lock};

struct StateInnerGuarded {
    worker_handles: HashMap<ThreadId, Option<JoinHandle<()>>>,
    panic_reports: Vec<PanicReport>,
}

struct StateInner {
    worker_count: AtomicUsize,
    waiting_queue_len: AtomicUsize,
    pool_status: AtomicU8,
    shutdown_now: AtomicBool,
    guarded: Mutex<StateInnerGuarded>,
}

impl StateInner {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self {
            worker_count: AtomicUsize::new(worker_count),
            waiting_queue_len: AtomicUsize::new(0),
            pool_status: AtomicU8::new(WPoolStatus::Running.as_u8()),
            shutdown_now: AtomicBool::new(false),
            guarded: Mutex::new(StateInnerGuarded {
                worker_handles: HashMap::new(),
                panic_reports: Vec::new(),
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct State {
    inner: Arc<StateInner>,
}

impl State {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self {
            inner: Arc::new(StateInner::new(worker_count)),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.inner.worker_count.load(Ordering::SeqCst)
    }

    pub(crate) fn shutdown_now(&self) -> bool {
        self.inner.shutdown_now.load(Ordering::SeqCst)
    }

    pub(crate) fn set_shutdown_now(&self, v: bool) {
        self.inner.shutdown_now.store(v, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    pub(crate) fn inc_worker_count(&self) {
        self.inner.worker_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_worker_count(&self) {
        self.inner.worker_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn pool_status(&self) -> WPoolStatus {
        self.inner
            .pool_status
            .load(Ordering::SeqCst)
            .as_wpool_status()
    }

    pub(crate) fn set_pool_status(&self, status: WPoolStatus) {
        self.inner
            .pool_status
            .store(status.as_u8(), Ordering::SeqCst);
    }

    pub(crate) fn waiting_queue_len(&self) -> usize {
        self.inner.waiting_queue_len.load(Ordering::SeqCst)
    }

    pub(crate) fn inc_waiting_queue_len(&self) {
        self.inner.waiting_queue_len.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_waiting_queue_len(&self) {
        self.inner.waiting_queue_len.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn panic_reports(&self) -> Vec<PanicReport> {
        safe_lock(&self.inner.guarded).panic_reports.clone()
    }

    pub(crate) fn insert_panic_report(&self, report: PanicReport) {
        safe_lock(&self.inner.guarded).panic_reports.push(report);
    }

    pub(crate) fn handle_worker_terminating(&self, thread_id: ThreadId) {
        if let Some(mut handle_opt) = safe_lock(&self.inner.guarded)
            .worker_handles
            .remove(&thread_id)
            && let Some(handle) = handle_opt.take()
        {
            self.dec_worker_count();
            let _ = thread::spawn(move || {
                let _ = handle.join();
            });
        }
    }

    pub(crate) fn insert_worker_handle(&self, handle: JoinHandle<()>) {
        safe_lock(&self.inner.guarded)
            .worker_handles
            .insert(handle.thread().id(), Some(handle));
    }

    pub(crate) fn join_worker_handles(&self) {
        let mut lock = safe_lock(&self.inner.guarded);

        let handles: Vec<JoinHandle<()>> = lock
            .worker_handles
            .drain()
            .filter_map(|(_, maybe_handle)| maybe_handle)
            .collect();

        self.inner.worker_count.store(0, Ordering::SeqCst);
        drop(lock); // Don't join while holding lock.

        for handle in handles {
            let _ = handle.join();
        }
    }
}
