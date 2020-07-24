//! Message send to the actor to retrieve the status of a PipelineExecution
use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;

pub struct PipelineStatusMessage {}

impl Message for PipelineStatusMessage {
    type Result = String;
}

impl Handler<PipelineStatusMessage> for PipelineActor {
    type Result = String;

    /// Send the result back as a struct serialized as a json string. This can also probably be passed around using the actual struct,
    /// but for this, some traits need to be implemented which i'm unaware of.
    fn handle(&mut self, _msg: PipelineStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        serde_json::to_string_pretty(&self.create_pipeline_execution()).unwrap()
    }
}
