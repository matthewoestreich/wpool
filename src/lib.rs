mod channel;
mod dispatcher;
mod pauser;
mod signal;
mod task;
mod worker;
mod wpool;

pub use wpool::WPool;

// Allows us to easily lock a Mutex while handling possible poison.
pub(crate) fn lock_safe<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
