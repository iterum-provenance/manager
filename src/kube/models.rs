use crate::error::ManagerError;
use iterum_rust::pipeline::PipelineRun;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use std::collections::HashMap;

use kube::{
    api::Api,
    api::{DeleteParams, ListParams, Meta, PostParams},
    Client,
};

use super::job_templates;

pub struct KubeAPI {}

impl KubeAPI {
    pub async fn submit(pipeline_job: PipelineRun) {
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
                } // if you skipped delete, for instance
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

    pub async fn get_status(pipeline_job: &PipelineRun) -> HashMap<String, usize> {
        let client = Client::try_default().await.expect("create client");
        let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
        let lp = ListParams::default().labels(&format!(
            "pipeline_run_hash={}",
            pipeline_job.pipeline_run_hash
        ));
        let jobs = jobs_client
            .list(&lp)
            .await
            .expect("Manager was not able to reach Kubernetes API..");

        let mut instances_done_counts: HashMap<String, usize> = HashMap::new();

        for job in jobs {
            let metadata = &job.metadata.as_ref().unwrap();
            let name = metadata.name.clone().unwrap();
            // let labels = metadata.labels.clone().unwrap();

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
