use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle, ThreadId},
};

use crate::{AsWPoolStatus, PanicReport, WPoolStatus, safe_lock};

pub(crate) struct SharedDataInner {
    pub(crate) worker_count: usize,
    pub(crate) worker_handles: HashMap<ThreadId, Option<JoinHandle<()>>>,
    pub(crate) pool_status: u8,
    pub(crate) waiting_queue_length: usize,
    pub(crate) panic_reports: Vec<PanicReport>,
}

impl Default for SharedDataInner {
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

pub(crate) struct SharedData {
    inner: Arc<Mutex<SharedDataInner>>,
}

impl Clone for SharedData {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl SharedData {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedDataInner::default())),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        safe_lock(&self.inner).worker_count
    }

    pub(crate) fn inc_worker_count(&self) {
        safe_lock(&self.inner).worker_count += 1;
    }

    pub(crate) fn dec_worker_count(&self) {
        safe_lock(&self.inner).worker_count -= 1;
    }

    pub(crate) fn pool_status(&self) -> WPoolStatus {
        safe_lock(&self.inner).pool_status.as_wpool_status()
    }

    pub(crate) fn set_pool_status(&self, status: WPoolStatus) {
        safe_lock(&self.inner).pool_status = status.as_u8();
    }

    pub(crate) fn waiting_queue_len(&self) -> usize {
        safe_lock(&self.inner).waiting_queue_length
    }

    pub(crate) fn set_waiting_queue_len(&self, len: usize) {
        safe_lock(&self.inner).waiting_queue_length = len;
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
