use crate::pipelines::lifecycle::actor::PipelineActor;
use actix::prelude::*;
use std::collections::HashMap;

pub struct KubeJobStatusMessage {
    pub status: bool,
    pub instances_done_counts: HashMap<String, usize>,
    pub mq_input_channel_counts: HashMap<String, Option<usize>>,
}

impl Message for KubeJobStatusMessage {
    type Result = bool;
}

impl Handler<KubeJobStatusMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: KubeJobStatusMessage, ctx: &mut Context<Self>) -> Self::Result {
        // debug!("Current pipeline status:");
        // for (job_name, status) in self.statuses.iter() {
        //     debug!("Job name:{}\tStatus: {}", job_name, status);
        // }

        // self.instances_done_counts = msg.instances_done_counts;
        // self.mq_channel_counts = msg.mq_input_channel_counts;
        let new_statuses = self.job_statuses.clone();

        for (name, job) in new_statuses.iter() {
            let job_ref = self.job_statuses.get_mut(name).unwrap();
            let changed = job_ref.instances_done != *msg.instances_done_counts.get(name).unwrap();

            job_ref.mq_input_channel_count = *msg.mq_input_channel_counts.get(name).unwrap();
            job_ref.instances_done = *msg.instances_done_counts.get(name).unwrap();

            if changed {
                debug!("Status changed for  : \t{}", name);
                debug!("Node upstream       : \t{:?}", job_ref.node_upstream);
                debug!(
                    "Messages inp channel: \t{:?}",
                    job_ref.mq_input_channel_count
                );
                debug!("Job nodes           : \t{:?}", job_ref.instances_in_job);
                debug!("Job nodes done      : \t{:?}", job_ref.instances_done);
                debug!("----");
            }
        }

        // new_statuses.iter().for_each(|(name, job)| {});

        // let mut change = false;
        // self.statuses.iter().for_each(|(job_name, job_status)| {
        //     let status = match msg.kube_statuses.clone().entry(job_name.to_string()) {
        //         Entry::Occupied(value) => *value.get(),
        //         _ => false,
        //     };
        //     if status != *job_status {
        //         info!("There is a change in status!");
        //         info!("{} is now {}", job_name, status);
        //         change = true;
        //     }
        // });
        // // info!("Change in job statuses:");
        // self.statuses = msg.kube_statuses;
        // if change {
        //     info!("Current pipeline status:");
        //     for (job_name, status) in self.statuses.iter() {
        //         info!("Status: {}\t job: {}", status, job_name);
        //     }
        //     info!("Current queue counts:");
        //     for (job_name, count) in msg.mq_counts.iter() {
        //         info!("Count: {}\t queue: {}", count, job_name);
        //     }
        // }

        // Kill message to self
        let success: Vec<bool> = self
            .job_statuses
            .iter()
            .map(|(_, status)| status.is_done())
            .filter(|val| !val)
            .collect();
        if success.is_empty() {
            ctx.stop();
        }

        msg.status
    }
}
