mod channel;
mod dispatcher;
mod job;
mod thread;
mod worker;
mod wpool;

pub use wpool::WPool;

// Allows us to easily lock a Mutex while handling possible poison.
pub(crate) fn safe_lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

// Returns numbers in sequential order.
// ```rust
// let next = monotonic_counter();
// let next_number = next();
// ```
pub(crate) fn monotonic_counter() -> Box<dyn Fn() -> usize + Send + 'static> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    Box::new(move || next.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
}
