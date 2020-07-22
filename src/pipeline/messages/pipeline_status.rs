use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;

pub struct PipelineStatusMessage {}

impl Message for PipelineStatusMessage {
    type Result = String;
}

impl Handler<PipelineStatusMessage> for PipelineActor {
    type Result = String;

    fn handle(&mut self, _msg: PipelineStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        serde_json::to_string_pretty(&self.create_pipeline_execution()).unwrap()
    }
}
