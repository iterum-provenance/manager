use iterum_rust::pipeline::PipelineRun;

use crate::pipeline::models::JobStatus;
use std::collections::HashMap;

pub fn create_job_statuses(
    pipeline_job: PipelineRun,
    node_upstream_map: HashMap<String, String>,
) -> HashMap<String, JobStatus> {
    let combiner_name = format!("{}-combiner", pipeline_job.pipeline_run_hash);
    let fragmenter_name = format!("{}-fragmenter", pipeline_job.pipeline_run_hash);

    let mut statuses: HashMap<String, JobStatus> = HashMap::new();

    let status_combiner = JobStatus {
        node_upstream: Some(node_upstream_map.get(&combiner_name).unwrap().to_string()),
        instances_in_job: 1,
        instances_done: 0,
        mq_input_channel_count: None,
    };
    statuses.insert(combiner_name, status_combiner);
    let status_fragmenter = JobStatus {
        node_upstream: None,
        instances_in_job: 1,
        instances_done: 0,
        mq_input_channel_count: None,
    };
    statuses.insert(fragmenter_name, status_fragmenter);

    for step in &pipeline_job.steps {
        let step_name = format!("{}-{}", pipeline_job.pipeline_run_hash, step.name);
        let status = JobStatus {
            node_upstream: Some(node_upstream_map.get(&step_name).unwrap().to_string()),
            instances_in_job: step.instance_count,
            instances_done: 0,
            mq_input_channel_count: None,
        };
        statuses.insert(step_name, status);
    }
    statuses
}
