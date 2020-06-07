use crate::error::ManagerError;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde::{Deserialize, Serialize};

use kube::{
    api::Api,
    api::{DeleteParams, ListParams, Meta, PostParams},
    Client,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransformationStep {
    pub name: String,
    pub instance_count: usize,
    pub image: String,
    pub input_channel: String,
    pub output_channel: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PipelineJob {
    #[serde(default = "empty_hash")]
    pub pipeline_run_hash: String,
    pub name: String,
    pub input_dataset: String,
    pub input_dataset_commit_hash: String,
    pub fragmenter_image: String,
    pub fragmenter_output_channel: String,
    pub fragmenter_config_files: Vec<String>,
    pub steps: Vec<TransformationStep>,
    pub combiner_input_channel: String,
}

fn empty_hash() -> String {
    String::from("")
}

use super::job_templates;
impl PipelineJob {
    pub async fn submit(pipeline_job: PipelineJob) {
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
                Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
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
}
