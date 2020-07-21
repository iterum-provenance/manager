// use crate::pipeline::actor::PipelineActor;
// use actix::prelude::Addr;
// use iterum_rust::pipeline::PipelineRun;
use iterum_rust::pipeline::StepStatus;
use std::collections::HashMap;

// pub struct PipelineInfo {
//     pub actor: Option<Addr<PipelineActor>>,
// }

#[derive(Debug, Clone)]
pub struct JobStatus {
    pub node_upstream: Option<String>,
    pub instances_in_job: usize,
    pub instances_done: usize,
    pub mq_input_channel_count: Option<usize>,
}

impl From<JobStatus> for StepStatus {
    fn from(status: JobStatus) -> StepStatus {
        if status.is_done() {
            StepStatus::Succeeded
        } else {
            StepStatus::Running
        }
    }
}

impl JobStatus {
    pub fn mq_empty(&self) -> bool {
        self.mq_input_channel_count == Some(0) || self.mq_input_channel_count == None
    }

    pub fn upstream_done(&self, status_map: &HashMap<String, JobStatus>) -> bool {
        let default = "".to_owned();
        let upstream_name = self.node_upstream.as_ref().unwrap_or(&default);
        match status_map.get(upstream_name) {
            Some(node) => node.is_done(),
            None => {
                warn!("Could not find upstream. Returning false");
                false
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.instances_in_job == self.instances_done && self.mq_empty()
    }
}
