use crate::{pauser::Pauser, task::Task};
use std::sync::Arc;

pub(crate) enum Signal {
    Job(Task),
    Pause(Arc<Pauser>),
    Terminate,
}
