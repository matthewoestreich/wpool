use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle, ThreadId},
};

use crate::{AsWPoolStatus, PanicReport, WPoolStatus, safe_lock};

struct StateInner {
    worker_handles: HashMap<ThreadId, Option<JoinHandle<()>>>,
    pool_status: u8,
    panic_reports: Vec<PanicReport>,
}

impl Default for StateInner {
    fn default() -> Self {
        Self {
            worker_handles: HashMap::new(),
            pool_status: WPoolStatus::Running.as_u8(),
            panic_reports: Vec::new(),
        }
    }
}

pub(crate) struct State {
    worker_count: Arc<AtomicUsize>,
    waiting_queue_len: Arc<AtomicUsize>,
    inner: Arc<Mutex<StateInner>>,
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            worker_count: Arc::clone(&self.worker_count),
            waiting_queue_len: Arc::clone(&self.waiting_queue_len),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            worker_count: Arc::new(AtomicUsize::new(0)),
            waiting_queue_len: Arc::new(AtomicUsize::new(0)),
            inner: Arc::new(Mutex::new(StateInner::default())),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.worker_count.load(Ordering::SeqCst)
    }

    pub(crate) fn inc_worker_count(&self) {
        self.worker_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_worker_count(&self) {
        self.worker_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn pool_status(&self) -> WPoolStatus {
        safe_lock(&self.inner).pool_status.as_wpool_status()
    }

    pub(crate) fn set_pool_status(&self, status: WPoolStatus) {
        safe_lock(&self.inner).pool_status = status.as_u8();
    }

    pub(crate) fn waiting_queue_len(&self) -> usize {
        self.waiting_queue_len.load(Ordering::SeqCst)
    }

    pub(crate) fn set_waiting_queue_len(&self, len: usize) {
        self.waiting_queue_len.store(len, Ordering::SeqCst);
    }

    pub(crate) fn panic_reports(&self) -> Vec<PanicReport> {
        safe_lock(&self.inner).panic_reports.clone()
    }

    pub(crate) fn insert_panic_report(&self, report: PanicReport) {
        safe_lock(&self.inner).panic_reports.push(report);
    }

    pub(crate) fn handle_worker_terminating(&self, thread_id: ThreadId) {
        if let Some(mut handle_opt) = safe_lock(&self.inner).worker_handles.remove(&thread_id)
            && let Some(handle) = handle_opt.take()
        {
            // Call `.join` from a separate thread so the state manager does not block.
            // Do not decrement worker count here. That is something the caller needs
            // to explicitly handle from the callsite.
            let _ = thread::spawn(move || {
                let _ = handle.join();
            });
        }
    }

    #[allow(dead_code)]
    pub(crate) fn insert_worker_handle(&self, key: ThreadId, value: JoinHandle<()>) {
        safe_lock(&self.inner)
            .worker_handles
            .insert(key, Some(value));
    }

    pub(crate) fn join_worker_handles(&self) {
        let mut lock = safe_lock(&self.inner);

        let handles: Vec<JoinHandle<()>> = lock
            .worker_handles
            .drain()
            .filter_map(|(_, maybe_handle)| maybe_handle)
            .collect();

        drop(lock); // Don't join while holding lock.

        for handle in handles {
            let _ = handle.join();
        }
    }
}
