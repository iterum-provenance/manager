use crate::pipeline::actor::PipelineActor;
// use crate::pipelines::message_queue::actor::MessageQueueActor;
// use crate::pipelines::pipeline_manager::PipelineManager;
use actix::prelude::Addr;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct Config {
    // pub manager: Addr<PipelineManager>,
    // pub mq_actor: Addr<MessageQueueActor>,
    pub addresses: RwLock<HashMap<String, Addr<PipelineActor>>>,
}
