use std::sync::{Arc, Mutex};

use crate::safe_lock;

#[derive(Clone)]
pub(crate) enum Signal {
    NewTask(Task),
    Pause(Arc<crate::thread::Pauser>),
    Terminate,
}

impl std::fmt::Debug for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Signal::NewTask(_) => write!(f, "Signal::NewTask(Task)"),
            Signal::Pause(_) => write!(f, "Signal::Pause(Pauser)"),
            Signal::Terminate => write!(f, "Signal::Terminate"),
        }
    }
}

pub(crate) type TaskFn = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct Task {
    inner: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Task {
    pub fn new(f: TaskFn) -> Self {
        let f = Mutex::new(Some(f));
        let inner = Arc::new(move || {
            if let Some(f) = safe_lock(&f).take() {
                f();
            }
        });
        Task { inner }
    }

    pub fn run(&self) {
        (self.inner)();
    }
}

impl Clone for Task {
    fn clone(&self) -> Self {
        Task {
            inner: Arc::clone(&self.inner),
        }
    }
}
