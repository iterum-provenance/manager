//! Message send to the actor when it can terminate
use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;

pub struct StopMessage {}

impl Message for StopMessage {
    type Result = bool;
}

impl Handler<StopMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        info!("Receiving stop message.");
        ctx.stop();
        true
    }
}
