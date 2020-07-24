//! Module which contains storage which is shared by the different endpoint handlers.

use crate::pipeline::actor::PipelineActor;
use actix::prelude::Addr;
use std::collections::HashMap;
use std::sync::RwLock;

/// Structure which is shared between the different endpoint handlers. Contains a HashMap with maps pipeline_hash to an actor address
pub struct Config {
    pub addresses: RwLock<HashMap<String, Addr<PipelineActor>>>,
}
