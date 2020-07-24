//! Message send from endpoint handler to a pipeline actor to check whether the step can stop
use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;

/// Message contains the step name on which the status has to be checked
pub struct JobStatusMessage {
    pub step_name: String,
}

impl Message for JobStatusMessage {
    type Result = bool;
}

impl Handler<JobStatusMessage> for PipelineActor {
    type Result = bool;

    /// Return a boolean value, true indicating the step is done, and the sidecar from who this message comes can stop itself
    fn handle(&mut self, msg: JobStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let status = self.job_statuses.get(&msg.step_name).unwrap();
        status.upstream_done(&self.job_statuses) && status.mq_empty()
    }
}
