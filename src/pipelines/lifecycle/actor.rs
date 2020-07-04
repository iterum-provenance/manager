use crate::pipelines::pipeline::PipelineJob;
use actix::prelude::*;
// use actix::Addr;
use super::messages::KubeJobStatusMessage;
use crate::pipelines::message_queue::actor::MessageQueueActor;
use crate::pipelines::message_queue::messages::GetAllQueueCountsMessage;
use crate::pipelines::provenance::models::FragmentLineage;

use crate::pipelines::lifecycle::models::JobStatus;
use crate::pipelines::provenance::actor::LineageActor;
use k8s_openapi::api::batch::v1::Job;
use kube::{api::Api, api::ListParams, Client};
use std::collections::HashMap;
use std::time::Duration;

pub struct PipelineActor {
    pub mq_actor: Addr<MessageQueueActor>,
    pub pipeline_job: PipelineJob,
    // pub statuses: HashMap<String, bool>,
    // pub first_node_upstream_map: HashMap<String, String>,
    // pub instances_per_job: HashMap<String, usize>,
    // pub instances_done_counts: HashMap<String, usize>,
    // pub lineage_map: HashMap<String, FragmentLineage>,
    pub mq_channel_counts: HashMap<String, usize>,
    pub job_statuses: HashMap<String, JobStatus>,
    pub lineage_actor: Addr<LineageActor>,
}

impl PipelineActor {}

impl Actor for PipelineActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Pipeline actor is alive");

        info!("Starting periodic check of status");
        ctx.run_interval(Duration::from_millis(10000), |act, context| {
            Arbiter::spawn(get_jobs_status(
                act.mq_actor.clone(),
                act.pipeline_job.clone(),
                context.address(),
                act.lineage_actor.clone(),
                // act.instances_per_job.clone(),
            ));
        });
        info!("Spawn submission of pipeline");
        Arbiter::spawn(PipelineJob::submit(self.pipeline_job.clone()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline actor is stopped");
    }
}

async fn get_jobs_status(
    mq_actor: Addr<MessageQueueActor>,
    pipeline_job: PipelineJob,
    actor_addr: Addr<PipelineActor>,
    lineage_addr: Addr<LineageActor>,
    // instance_counts: HashMap<String, usize>,
) {
    let mut instances_done_counts: HashMap<String, usize> = HashMap::new();
    let mut mq_input_channel_counts: HashMap<String, Option<usize>> = HashMap::new();

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
    let message_queue_counts = mq_actor
        .send(GetAllQueueCountsMessage {})
        .await
        .unwrap()
        .unwrap();

    // info!("Message queue counts: {:?}", message_queue_counts);
    for job in jobs {
        let metadata = &job.metadata.as_ref().unwrap();
        let name = metadata.name.clone().unwrap();
        let labels = metadata.labels.clone().unwrap();
        let input_channel = labels.get("input_channel").unwrap();

        let status = job.status.clone().unwrap();
        // let instance_count = instance_counts.get(&name).unwrap();
        let queue_count = match message_queue_counts.get(input_channel) {
            Some(count) => Some(*count as usize),
            None => None,
        };
        let instances_done = match status.succeeded {
            Some(val) => val as usize,
            None => 0,
        };
        mq_input_channel_counts.insert(name.clone(), queue_count);
        instances_done_counts.insert(name.clone(), instances_done);
    }
    let success = actor_addr
        .send(KubeJobStatusMessage {
            status: true,
            instances_done_counts,
            mq_input_channel_counts,
        })
        .await
        .unwrap();

    if success {
        // self.lineage_actor.send()
        // use crate::pipelines::provenance::actor::StopMessage;
        // self.lineage_actor.send(StopMessage {}).wait();
        info!("Pipeline done. Killing actors.");
        actor_addr.send(StopMessage {}).await.unwrap();
        lineage_addr
            .send(crate::pipelines::provenance::actor::StopMessage {})
            .await
            .unwrap();
        // ctx.stop();
    };
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
