//! Message send to the pipeline actor which contains the statuses retrieved from the Kubernetes API and the RabbitMQ api.
//! The pipeline actor then uses this info to update its own status map

use crate::pipeline::actor::PipelineActor;
use actix::prelude::*;
use std::collections::HashMap;

/// Message contains the amount of instances done for each step, and the amount of messages left on the input queue for each step
pub struct KubeJobStatusMessage {
    pub instances_done_counts: HashMap<String, usize>,
    pub mq_input_channel_counts: HashMap<String, Option<usize>>,
}

impl Message for KubeJobStatusMessage {
    type Result = bool;
}

impl Handler<KubeJobStatusMessage> for PipelineActor {
    type Result = bool;

    /// Handle the receiving of a KubeJobStatusMessage
    fn handle(&mut self, msg: KubeJobStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let new_statuses = self.job_statuses.clone();

        // Check for each step whether the status of the pipeline changed
        for (name, _job) in new_statuses.iter() {
            let job_ref = self.job_statuses.get_mut(name).unwrap();
            let changed = job_ref.instances_done != *msg.instances_done_counts.get(name).unwrap_or(&0);

            job_ref.mq_input_channel_count = *msg.mq_input_channel_counts.get(name).unwrap_or(&None);
            job_ref.instances_done = *msg.instances_done_counts.get(name).unwrap_or(&0);

            // If there is an update, print this update
            if changed {
                debug!("Status changed for  : \t{}", name);
                debug!("Node upstream       : \t{:?}", job_ref.node_upstream);
                debug!("Messages inp channel: \t{:?}", job_ref.mq_input_channel_count);
                debug!("Job nodes           : \t{:?}", job_ref.instances_in_job);
                debug!("Job nodes done      : \t{:?}", job_ref.instances_done);
                debug!("----");
            }
        }
        let success: Vec<bool> = self
            .job_statuses
            .iter()
            .map(|(_, status)| status.is_done())
            .filter(|val| !val)
            .collect();

        success.is_empty()
    }
}
