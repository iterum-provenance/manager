use crate::error::ManagerError;
use crate::pipelines::defaults::{
    empty_config, empty_hash, none_config_files_all, none_usize, one_instance,
};
use crate::pipelines::lifecycle::models::JobStatus;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use kube::{
    api::Api,
    api::{DeleteParams, ListParams, Meta, PostParams},
    Client,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransformationStep {
    pub name: String,
    #[serde(default = "one_instance")]
    pub instance_count: usize,
    #[serde(default = "none_usize")]
    pub prefetch_count: Option<usize>,
    pub image: String,
    pub input_channel: String,
    pub output_channel: String,
    #[serde(default = "empty_config")]
    pub config: Config,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fragmenter {
    pub image: String,
    pub output_channel: String,
    #[serde(default = "empty_config")]
    pub config: Config,
    #[serde(
        default = "none_config_files_all",
        skip_serializing_if = "Option::is_none"
    )]
    pub config_files_all: Option<HashMap<String, Vec<String>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Combiner {
    pub input_channel: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "HashMap::new", skip_serializing_if = "HashMap::is_empty")]
    pub config_files: HashMap<String, String>,
    #[serde(default = "HashMap::new", flatten)]
    pub config: HashMap<String, Value>,
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct FragmenterConfig {
//     #[serde(default = "HashMap::new")]
//     pub config_files_all: HashMap<String, Vec<String>>,
//     #[serde(flatten)]
//     pub config: HashMap<String, Value>,
// }

impl Config {
    pub fn is_empty(&self) -> bool {
        self.config.is_empty() && self.config_files.is_empty()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PipelineJob {
    #[serde(default = "empty_hash")]
    pub pipeline_run_hash: String,
    pub name: String,
    pub input_dataset: String,
    pub input_dataset_commit_hash: String,
    pub fragmenter: Fragmenter,
    pub steps: Vec<TransformationStep>,
    pub combiner: Combiner,
    #[serde(default = "empty_config", skip_serializing_if = "Config::is_empty")]
    pub config: Config,
}

use super::job_templates;
impl PipelineJob {
    pub async fn submit(pipeline_job: PipelineJob) {
        info!("Submitting the pipeline to Kubernetes API.");
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client, "default");
        let pp = PostParams::default();

        let mut jobs: Vec<Job> = vec![];
        let fragmenter = job_templates::fragmenter(&pipeline_job);
        jobs.push(fragmenter);
        for step in &pipeline_job.steps {
            let job = job_templates::job(&pipeline_job, &step);
            jobs.push(job);
        }
        let combiner = job_templates::combiner(&pipeline_job);
        jobs.push(combiner);

        for job in jobs {
            match jobs_client.create(&pp, &job).await {
                Ok(o) => {
                    let name = Meta::name(&o);
                    assert_eq!(Meta::name(&job), name);
                    info!("Created job {}", name);
                }
                Err(kube::Error::Api(ae)) => {
                    panic!("Kubernetes error: {}", ae);
                    // assert_eq!(ae.code, 409)
                }
                    , // if you skipped delete, for instance
                Err(e) => panic!("Error {}", e), // any other case is probably bad
            }
        }
    }

    pub async fn delete_all() -> Result<(), ManagerError> {
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
        let pods_client: Api<Pod> = Api::namespaced(client.clone(), "default");
        let lp = ListParams::default();
        let jobs = jobs_client.list(&lp).await.unwrap();
        info!("Deleting all jobs:");
        // let mut names: Vec<String> = vec![];
        for job in jobs {
            // job.
            let name = job.metadata.unwrap().name.unwrap();
            let dp = DeleteParams::default();

            jobs_client.delete(&name, &dp).await?;

            let lp = ListParams::default().labels(&format!("job-name={}", name));
            pods_client.delete_collection(&lp).await?;

            info!("Job: {:?}", name);
            // names.push(name)
        }
        Ok(())
    }

    pub fn create_job_statuses(
        self,
        node_upstream_map: HashMap<String, String>,
    ) -> HashMap<String, JobStatus> {
        let combiner_name = format!("{}-combiner", self.pipeline_run_hash);
        let fragmenter_name = format!("{}-fragmenter", self.pipeline_run_hash);

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

        for step in &self.steps {
            let step_name = format!("{}-{}", self.pipeline_run_hash, step.name);
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
}
