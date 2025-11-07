use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::safe_lock;

#[derive(Clone)]
pub(crate) struct WaitGroup {
    inner: Arc<WaitGroupInner>,
}

struct WaitGroupInner {
    count: AtomicUsize,
    cvar: Condvar,
    lock: Mutex<()>,
}

impl WaitGroup {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WaitGroupInner {
                count: AtomicUsize::new(0),
                cvar: Condvar::new(),
                lock: Mutex::new(()),
            }),
        }
    }

    pub(crate) fn add(&self, delta: usize) {
        let _guard = safe_lock(&self.inner.lock);
        self.inner.count.fetch_add(delta, Ordering::SeqCst);
    }

    pub(crate) fn done(&self) {
        let _guard = safe_lock(&self.inner.lock);
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.cvar.notify_all();
        }
    }

    pub(crate) fn wait(&self) {
        let mut guard = safe_lock(&self.inner.lock);
        while self.inner.count.load(Ordering::SeqCst) > 0 {
            guard = self.inner.cvar.wait(guard).unwrap();
        }
    }
}
