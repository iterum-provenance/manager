use crate::pipelines::pipeline::PipelineJob;
use actix::prelude::*;
// use actix::Addr;
use crate::pipelines::provenance::models::FragmentLineage;

use k8s_openapi::api::batch::v1::Job;
use kube::{api::Api, api::ListParams, Client};
use std::collections::HashMap;
use std::time::Duration;

pub struct KubeJobStatusMessage {
    pub status: bool,
    pub kube_statuses: HashMap<String, bool>,
}

impl Message for KubeJobStatusMessage {
    type Result = bool;
}

pub struct PipelineActor {
    pub pipeline_job: PipelineJob,
    pub statuses: HashMap<String, bool>,
    pub first_node_upstream_map: HashMap<String, String>,
    pub lineage_map: HashMap<String, FragmentLineage>,
}

impl PipelineActor {}

impl Actor for PipelineActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Pipeline actor is alive");

        ctx.run_interval(Duration::from_millis(5000), |act, context| {
            Arbiter::spawn(get_jobs_status(act.pipeline_job.clone(), context.address()));
        });

        Arbiter::spawn(PipelineJob::submit(self.pipeline_job.clone()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline actor is stopped");

        Arbiter::spawn(send_lineage_data(
            self.pipeline_job.pipeline_run_hash.to_string(),
            self.lineage_map.clone(),
        ));
    }
}

async fn send_lineage_data(
    _pipeline_run_hash: String,
    provenance_data: HashMap<String, FragmentLineage>,
) {
    debug!("Sending provenance info to the daemon");
    debug!("Sending this: {:?}", provenance_data);
    warn!("NOT ACTUALLY SENDING INFO YET, UNIMPLEMENTED.");
}

impl Handler<KubeJobStatusMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: KubeJobStatusMessage, ctx: &mut Context<Self>) -> Self::Result {
        debug!("Current pipeline status:");
        for (job_name, status) in self.statuses.iter() {
            debug!("Job name:{}\tStatus: {}", job_name, status);
        }

        self.statuses = msg.kube_statuses;
        let success: Vec<bool> = self
            .statuses
            .iter()
            .map(|(_, &val)| val)
            .filter(|val| !val)
            .collect();
        if success.is_empty() {
            ctx.stop();
        }

        msg.status
    }
}

async fn get_jobs_status(pipeline_job: PipelineJob, actor_addr: Addr<PipelineActor>) {
    info!(
        "Checking status for pipeline with hash {}",
        pipeline_job.pipeline_run_hash
    );

    let mut statuses: HashMap<String, bool> = HashMap::new();

    let client = Client::try_default().await.expect("create client");
    let jobs_client: Api<Job> = Api::namespaced(client.clone(), "default");
    // let pods_client: Api<Pod> = Api::namespaced(client.clone(), "default");

    let lp = ListParams::default().labels(&format!(
        "pipeline_run_hash={}",
        pipeline_job.pipeline_run_hash
    ));
    let jobs = jobs_client.list(&lp).await.unwrap();
    for job in jobs {
        let metadata = &job.metadata.as_ref().unwrap();
        let name = metadata.name.clone().unwrap();
        let status = job.status.clone().unwrap();
        let success = match status.succeeded {
            Some(val) => val >= 1, // Should actually depend on the pipeline definition
            None => false,
        };
        info!("Status of {} is {:?}", name, status);
        statuses.insert(name, success);
    }
    actor_addr
        .send(KubeJobStatusMessage {
            status: true,
            kube_statuses: statuses,
        })
        .await
        .unwrap();
}

pub struct JobStatusMessage {
    pub step_name: String,
}

impl Message for JobStatusMessage {
    type Result = bool;
}

impl Handler<JobStatusMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: JobStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!("Retrieving {} from upstream list", msg.step_name);
        info!("Upstream list: {:?}", self.first_node_upstream_map);

        match self.first_node_upstream_map.get(&msg.step_name) {
            Some(step_upstream) => match self.statuses.get(step_upstream) {
                Some(&status) => status,
                None => false,
            },
            None => false,
        }
    }
}

pub struct FragmentLineageMessage {
    pub fragment_id: String,
    pub fragment_lineage: FragmentLineage,
}

impl Message for FragmentLineageMessage {
    type Result = bool;
}

impl Handler<FragmentLineageMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: FragmentLineageMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!("Received lineage for fragment: {}", msg.fragment_id);

        self.lineage_map
            .insert(msg.fragment_id, msg.fragment_lineage)
            .is_none()
    }
}

pub struct StopMessage {}

impl Message for StopMessage {
    type Result = bool;
}

impl Handler<StopMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        info!("Receiving stop message.");
        ctx.stop();
        true
    }
}
