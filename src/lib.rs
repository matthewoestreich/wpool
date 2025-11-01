mod channel;
mod dispatcher;
mod pauser;
mod worker;
mod wpool;

pub use wpool::WPool;

pub(crate) type Task = Box<dyn FnOnce() + Send + 'static>;

pub(crate) enum Signal {
    NewTask(Task),
    Pause(std::sync::Arc<crate::pauser::Pauser>),
}

// Allows us to easily lock a Mutex while handling possible poison.
pub(crate) fn safe_lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
