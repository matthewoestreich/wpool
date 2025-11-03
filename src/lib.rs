mod dispatcher;
mod pauser;
mod worker;
mod wpool;

pub use wpool::WPool;

pub(crate) type Task = Box<dyn FnOnce() + Send + 'static>;

pub(crate) enum Signal {
    NewTask(Task),
    Pause(std::sync::Arc<crate::pauser::Pauser>),
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

#[derive(Default)]
pub(crate) struct Channel<S, R> {
    pub(crate) sender: S,
    pub(crate) receiver: R,
}

impl<S, R> Channel<S, R> {
    pub(crate) fn new(sender: S, receiver: R) -> Self {
        Self { sender, receiver }
    }
}

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
