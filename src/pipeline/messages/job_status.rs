use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;

pub struct JobStatusMessage {
    pub step_name: String,
}

impl Message for JobStatusMessage {
    type Result = bool;
}

impl Handler<JobStatusMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: JobStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let status = self.job_statuses.get(&msg.step_name).unwrap();
        // debug!("Status retrieved for: \t{}", msg.step_name);
        // debug!("Node upstream       : \t{:?}", status.node_upstream);
        // debug!(
        //     "Messages inp channel: \t{:?}",
        //     status.mq_input_channel_count
        // );
        // debug!("Job nodes           : \t{:?}", status.instances_in_job);
        // debug!("Job nodes done      : \t{:?}", status.instances_done);
        // debug!(
        //     "Upstream done       : \t{:?}",
        //     status.upstream_done(&self.job_statuses)
        // );
        // debug!("MQ empty done       : \t{:?}", status.mq_empty());
        // debug!("----");
        status.upstream_done(&self.job_statuses) && status.mq_empty()
    }
}
