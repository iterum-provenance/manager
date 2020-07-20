use actix::prelude::*;
use iterum_rust::pipeline::PipelineRun;
// use actix::Addr;
use super::messages::KubeJobStatusMessage;
use crate::kube::KubeAPI;
use crate::mq::RabbitMqAPI;
// use crate::pipelines::message_queue::actor::MessageQueueActor;
// use crate::pipelines::message_queue::messages::GetAllQueueCountsMessage;
use iterum_rust::provenance::FragmentLineage;

use crate::pipeline::models::JobStatus;
use crate::provenance_tracker::actor::LineageActor;
use futures::join;
use std::collections::HashMap;
use std::time::Duration;

pub struct PipelineActor {
    pub pipeline_job: PipelineRun,
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
            Arbiter::spawn(get_jobs_status(act.pipeline_job.clone(), context.address()));
        });
        info!("Spawn submission of pipeline");
        Arbiter::spawn(KubeAPI::submit(self.pipeline_job.clone()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline actor is stopped");
        Arbiter::spawn(send_stop_message_to_lineage_tracker(
            self.lineage_actor.clone(),
        ));
    }
}

async fn send_stop_message_to_lineage_tracker(address: Addr<LineageActor>) {
    address
        .send(crate::provenance_tracker::actor::StopMessage {})
        .await
        .unwrap();
}

async fn get_jobs_status(pipeline_job: PipelineRun, actor_addr: Addr<PipelineActor>) {
    let kube_status = KubeAPI::get_status(&pipeline_job);
    let mq_status = RabbitMqAPI::get_message_queue_counts(&pipeline_job);

    // Await both futures
    let (instances_done_counts, mq_input_channel_counts) = join!(kube_status, mq_status);

    let success = actor_addr
        .send(KubeJobStatusMessage {
            status: true,
            instances_done_counts,
            mq_input_channel_counts,
        })
        .await
        .unwrap();

    if success {
        info!("Pipeline done. Killing actors.");
        actor_addr.send(StopMessage {}).await.unwrap();
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
