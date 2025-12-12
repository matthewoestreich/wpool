use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle, ThreadId},
};

use crate::{AsWPoolStatus, PanicReport, WPoolStatus, safe_lock};

struct StateInner {
    worker_count: AtomicUsize,
    waiting_queue_len: AtomicUsize,
    pool_status: AtomicU8,
    worker_handles: Mutex<HashMap<ThreadId, Option<JoinHandle<()>>>>,
    panic_reports: Mutex<Vec<PanicReport>>,
    pending_timeout: Mutex<Option<ThreadId>>,
}

impl StateInner {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self {
            worker_count: AtomicUsize::new(worker_count),
            waiting_queue_len: AtomicUsize::new(0),
            pool_status: AtomicU8::new(WPoolStatus::Running.as_u8()),
            pending_timeout: None.into(),
            worker_handles: HashMap::new().into(),
            panic_reports: Vec::new().into(),
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

    pub(crate) fn pending_timeout(
        &self,
    ) -> Result<MutexGuard<'_, Option<ThreadId>>, PoisonError<MutexGuard<'_, Option<ThreadId>>>>
    {
        self.inner.pending_timeout.lock()
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.inner.worker_count.load(Ordering::SeqCst)
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
        safe_lock(&self.inner.panic_reports).clone()
    }

    pub(crate) fn insert_panic_report(&self, report: PanicReport) {
        safe_lock(&self.inner.panic_reports).push(report);
    }

    pub(crate) fn join_worker(&self, thread_id: ThreadId) {
        if let Some(mut handle_opt) = safe_lock(&self.inner.worker_handles).remove(&thread_id)
            && let Some(handle) = handle_opt.take()
        {
            self.dec_worker_count();
            let _ = thread::spawn(move || {
                let _ = handle.join();
            });
        }
    }

    pub(crate) fn insert_worker_handle(&self, handle: JoinHandle<()>) {
        safe_lock(&self.inner.worker_handles).insert(handle.thread().id(), Some(handle));
    }

    pub(crate) fn join_worker_handles(&self) {
        let mut lock = safe_lock(&self.inner.worker_handles);

        let handles: Vec<JoinHandle<()>> = lock
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
