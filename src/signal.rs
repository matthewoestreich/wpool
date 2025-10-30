use crate::{pauser::Pauser, task::TaskFn};
use std::sync::Arc;

pub(crate) enum Signal {
    Task(TaskFn),
    Pause(Arc<Pauser>),
    Terminate,
}
