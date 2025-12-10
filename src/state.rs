use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle, ThreadId},
};

use crate::{AsWPoolStatus, PanicReport, WPoolStatus, safe_lock};

#[derive(Clone)]
pub(crate) struct State {
    worker_count: Arc<AtomicUsize>,
    waiting_queue_len: Arc<AtomicUsize>,
    pool_status: Arc<AtomicU8>,
    worker_handles: Arc<Mutex<HashMap<ThreadId, Option<JoinHandle<()>>>>>,
    panic_reports: Arc<Mutex<Vec<PanicReport>>>,
}

impl State {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self {
            worker_count: Arc::new(AtomicUsize::new(worker_count)),
            waiting_queue_len: Arc::new(AtomicUsize::new(0)),
            pool_status: Arc::new(AtomicU8::new(WPoolStatus::Running.as_u8())),
            worker_handles: Arc::new(Mutex::new(HashMap::new())),
            panic_reports: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.worker_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub(crate) fn inc_worker_count(&self) {
        self.worker_count.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    pub(crate) fn dec_worker_count(&self) {
        self.worker_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn pool_status(&self) -> WPoolStatus {
        self.pool_status.load(Ordering::SeqCst).as_wpool_status()
    }

    pub(crate) fn set_pool_status(&self, status: WPoolStatus) {
        self.pool_status.store(status.as_u8(), Ordering::SeqCst);
    }

    pub(crate) fn waiting_queue_len(&self) -> usize {
        self.waiting_queue_len.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub(crate) fn set_waiting_queue_len(&self, len: usize) {
        self.waiting_queue_len.store(len, Ordering::SeqCst);
    }

    pub(crate) fn inc_waiting_queue_len(&self) {
        self.waiting_queue_len.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_waiting_queue_len(&self) {
        self.waiting_queue_len.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn panic_reports(&self) -> Vec<PanicReport> {
        safe_lock(&self.panic_reports).clone()
    }

    pub(crate) fn insert_panic_report(&self, report: PanicReport) {
        safe_lock(&self.panic_reports).push(report);
    }

    pub(crate) fn handle_worker_terminating(&self, thread_id: ThreadId) {
        if let Some(mut handle_opt) = safe_lock(&self.worker_handles).remove(&thread_id)
            && let Some(handle) = handle_opt.take()
        {
            let _ = thread::spawn(move || {
                let _ = handle.join();
            });
        }
    }

    pub(crate) fn insert_worker_handle(&self, key: ThreadId, value: JoinHandle<()>) {
        safe_lock(&self.worker_handles).insert(key, Some(value));
    }

    pub(crate) fn join_worker_handles(&self) {
        let mut lock = safe_lock(&self.worker_handles);

        let handles: Vec<JoinHandle<()>> = lock
            .drain()
            .filter_map(|(_, maybe_handle)| maybe_handle)
            .collect();

        self.worker_count.store(0, Ordering::SeqCst);
        drop(lock); // Don't join while holding lock.

        for handle in handles {
            let _ = handle.join();
        }
    }
}
