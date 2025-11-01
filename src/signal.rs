use crate::{pauser::Pauser, task::Task};
use std::sync::Arc;

pub(crate) enum Signal {
    NewTask(Task),
    Pause(Arc<Pauser>),
    Terminate,
}
