//! Contains KubeAPI model, which is currently empty, but it may contain additional credentials for example in the future. This api implements
//! the various functions to submit, delete and check on the status of Kubernetes Jobs required by the manager.

use crate::error::ManagerError;
use iterum_rust::pipeline::PipelineRun;
use k8s_openapi::api::{batch::v1::Job, core::v1::Pod};
use kube::{
    api::Api,
    api::{DeleteParams, ListParams, Meta, PostParams},
    Client,
};
use std::collections::HashMap;

use super::job_templates;

pub struct KubeAPI {}

impl KubeAPI {
    /// Submit a pipeline to the kubernetes API. First converts the pipeline job into a list of Jobs which can be submitted to the Kubernetes API,
    /// and then submits them.
    pub async fn submit(pipeline_job: PipelineRun) {
        info!("Submitting the pipeline to Kubernetes API.");
        // Create client to communicate with Kube API.
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client, "default");
        let pp = PostParams::default();

        // Create a list of jobs from a pipeline job.
        let mut jobs: Vec<Job> = vec![];
        let fragmenter = job_templates::fragmenter(&pipeline_job);
        jobs.push(fragmenter);
        for step in &pipeline_job.steps {
            let job = job_templates::transformation_step(&pipeline_job, &step);
            jobs.push(job);
        }
        let combiner = job_templates::combiner(&pipeline_job);
        jobs.push(combiner);

        // Submit each job to the kubernetes API
        for job in jobs {
            match jobs_client.create(&pp, &job).await {
                Ok(o) => {
                    let name = Meta::name(&o);
                    assert_eq!(Meta::name(&job), name);
                    info!("Created job {}", name);
                }
                Err(kube::Error::Api(ae)) => {
                    panic!("Kubernetes error: {}", ae);
                }
                Err(e) => panic!("Error {}", e),
            }
        }
    }

    /// Helper function to delete a job. For the sake of DRY.
    async fn delete_job(jobs_client: &Api<Job>, pods_client: &Api<Pod>, job: Job) -> Result<(), ManagerError> {
        let name = job.metadata.unwrap().name.unwrap();
        let dp = DeleteParams::default();

        jobs_client.delete(&name, &dp).await?;

        let lp = ListParams::default().labels(&format!("job-name={}", name));
        pods_client.delete_collection(&lp).await?;

        info!("Removed job: {:?}", name);
        Ok(())
    }

    /// Delete the jobs of a pipeline run
    pub async fn delete_pipeline(pipeline_run_hash: &str) -> Result<(), ManagerError> {
        // Create clients to communicate with Kube API.
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
        let pods_client: Api<Pod> = Api::namespaced(client.clone(), "default");

        // Retrieve all jobs for the pipeline run
        let lp = ListParams::default().labels(&format!("pipeline_run_hash={}", pipeline_run_hash));
        let jobs = jobs_client.list(&lp).await.unwrap();

        // Delete each job
        info!("Deleting jobs for pipeline {}", pipeline_run_hash);
        for job in jobs {
            KubeAPI::delete_job(&jobs_client, &pods_client, job).await?;
        }
        Ok(())
    }

    /// Delete all jobs present on the cluster
    pub async fn delete_all() -> Result<(), ManagerError> {
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
        let pods_client: Api<Pod> = Api::namespaced(client.clone(), "default");
        let lp = ListParams::default();
        let jobs = jobs_client.list(&lp).await.unwrap();

        info!("Deleting all jobs.");
        for job in jobs {
            KubeAPI::delete_job(&jobs_client, &pods_client, job).await?;
        }
        Ok(())
    }

    /// Retrieve status of the jobs of a pipeline run
    pub async fn get_status(pipeline_job: &PipelineRun) -> HashMap<String, usize> {
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
        let lp = ListParams::default().labels(&format!("pipeline_run_hash={}", pipeline_job.pipeline_run_hash));
        let jobs = jobs_client
            .list(&lp)
            .await
            .expect("Manager was not able to reach Kubernetes API..");

        let mut instances_done_counts: HashMap<String, usize> = HashMap::new();

        for job in jobs {
            let metadata = &job.metadata.as_ref().unwrap();
            let name = metadata.name.clone().unwrap();

            let status = job.status.clone().unwrap();
            let instances_done = match status.succeeded {
                Some(val) => val as usize,
                None => 0,
            };
            instances_done_counts.insert(name.clone(), instances_done);
        }

        instances_done_counts
    }
}
