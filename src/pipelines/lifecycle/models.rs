// pub struct PipelineActor {
//     pub mq_actor: Addr<MessageQueueActor>,
//     pub pipeline_job: PipelineJob,
//     pub statuses: HashMap<String, bool>,
//     pub first_node_upstream_map: HashMap<String, String>,
//     pub instances_per_job: HashMap<String, usize>,
//     pub instances_done_counts: HashMap<String, usize>,
//     pub mq_input_channel_counts: HashMap<String, usize>,
//     pub lineage_map: HashMap<String, FragmentLineage>,
// }
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct JobStatus {
    pub node_upstream: Option<String>,
    pub instances_in_job: usize,
    pub instances_done: usize,
    pub mq_input_channel_count: Option<usize>,
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
