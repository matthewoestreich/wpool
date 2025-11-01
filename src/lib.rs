mod channel;
mod dispatcher;
mod pauser;
mod worker;
mod wpool;

pub use wpool::WPool;

use std::sync::Arc;

pub(crate) type Task = Box<dyn FnOnce() + Send + 'static>;

pub(crate) enum Signal {
    NewTask(Task),
    Pause(Arc<crate::pauser::Pauser>),
    Terminate,
}

// Allows us to easily lock a Mutex while handling possible poison.
pub(crate) fn lock_safe<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
