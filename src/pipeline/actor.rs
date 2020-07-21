use super::messages::KubeJobStatusMessage;
use crate::kube::KubeAPI;
use crate::mq::RabbitMqAPI;
use crate::pipeline::models::JobStatus;
use crate::provenance_tracker::actor::LineageActor;
use actix::prelude::*;
use futures::join;
use iterum_rust::pipeline::PipelineRun;
use iterum_rust::pipeline::{PipelineExecution, StepStatus};
use std::collections::HashMap;
use std::time::Duration;

pub struct PipelineActor {
    pub pipeline_job: PipelineRun,
    pub mq_channel_counts: HashMap<String, usize>,
    pub job_statuses: HashMap<String, JobStatus>,
    pub lineage_actor: Option<Addr<LineageActor>>,
}

impl PipelineActor {
    pub fn new(pipeline_run: PipelineRun) -> PipelineActor {
        let job_statuses = crate::kube::misc::create_job_statuses(
            pipeline_run.clone(),
            pipeline_run.create_first_node_upstream_map(),
        );

        PipelineActor {
            pipeline_job: pipeline_run,
            lineage_actor: None,
            mq_channel_counts: HashMap::new(),
            job_statuses,
        }
    }

    pub fn create_pipeline_execution(&self) -> PipelineExecution {
        let mut statuses: HashMap<String, StepStatus> = HashMap::new();

        for (key, value) in &self.job_statuses {
            statuses.insert(key.to_string(), value.clone().into());
        }

        PipelineExecution {
            pipeline_run: self.pipeline_job.clone(),
            status: statuses,
            results: None,
        }
    }
}

impl Actor for PipelineActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Pipeline actor is alive");

        info!("Starting periodic check of status");
        ctx.run_interval(Duration::from_millis(10000), |act, context| {
            Arbiter::spawn(get_jobs_status(act.pipeline_job.clone(), context.address()));
        });

        info!("Start provenance tracking actor");
        let lineage_actor = LineageActor {
            pipeline_run_hash: self.pipeline_job.clone().pipeline_run_hash,
            channel: None,
        };
        let lineage_addr = lineage_actor.start();
        self.lineage_actor = Some(lineage_addr);

        info!("Creating pipeline execution on daemon");
        let pe = self.create_pipeline_execution();
        Arbiter::spawn(crate::daemon::api::store_pipeline_execution(pe));

        info!("Spawn submission of pipeline");
        Arbiter::spawn(KubeAPI::submit(self.pipeline_job.clone()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline actor is stopped");

        info!("Updating pipeline execution on daemon");
        let pe = self.create_pipeline_execution();
        Arbiter::spawn(crate::daemon::api::store_pipeline_execution(pe));

        Arbiter::spawn(send_stop_message_to_lineage_tracker(
            self.lineage_actor.clone().unwrap(),
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
