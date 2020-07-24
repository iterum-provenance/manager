//! Contains the actor which is spawned for each of the pipelines active on the cluster.
use super::messages::{KubeJobStatusMessage, StopMessage};
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

/// Some state which the actor requires. The pipelinejob and lineageactor are mostly constant, but the mq_channel_counts and the job_statuses update
/// throughout the lifetime of the pipeline.
pub struct PipelineActor {
    pub pipeline_job: PipelineRun,
    pub mq_channel_counts: HashMap<String, usize>,
    pub job_statuses: HashMap<String, JobStatus>,
    pub lineage_actor: Option<Addr<LineageActor>>,
}

impl PipelineActor {
    /// Builder pattern to construct a new actor
    pub fn new(pipeline_run: PipelineRun) -> PipelineActor {
        let job_statuses =
            crate::kube::misc::create_job_statuses(pipeline_run.clone(), pipeline_run.create_first_node_upstream_map());

        PipelineActor {
            pipeline_job: pipeline_run,
            lineage_actor: None,
            mq_channel_counts: HashMap::new(),
            job_statuses,
        }
    }

    /// Creates a PipelineExecution from the current state known to the actor
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

    /// Function which is run when the actor is started
    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Pipeline actor is alive");

        // Spawn a job which checks the status of the messages on the queue, and the status of the Kubernetes jobs every 10s.
        // A KubeJobMessage is send back to this actor after each status check, so that this actor can update its state.
        info!("Starting periodic check of status");
        ctx.run_interval(Duration::from_millis(10000), |act, context| {
            Arbiter::spawn(get_jobs_status(act.pipeline_job.clone(), context.address()));
        });

        // Also start a provenance tracking actor, which is responsible for consuming lineage information of the lineage queue
        info!("Start provenance tracking actor");
        let lineage_actor = LineageActor {
            pipeline_run: self.pipeline_job.clone(),
            channel: None,
        };
        let lineage_addr = lineage_actor.start();
        self.lineage_actor = Some(lineage_addr);

        // Send a PipelineExecution struct to the daemon, so the daemon is aware that a pipeline has started
        info!("Creating pipeline execution on daemon");
        let pe = self.create_pipeline_execution();
        Arbiter::spawn(crate::daemon::api::store_pipeline_execution(pe));

        // Submit the jobs to the Kubernetes API
        info!("Spawn submission of pipeline");
        Arbiter::spawn(KubeAPI::submit(self.pipeline_job.clone()));
    }

    /// Function which is run when the actor is stopped
    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline actor is stopped");

        // Update PipelineExecution on the daemon
        info!("Updating pipeline execution on daemon");
        let pe = self.create_pipeline_execution();
        Arbiter::spawn(crate::daemon::api::store_pipeline_execution(pe));

        // Also kill the provenance tracker actor
        Arbiter::spawn(send_stop_message_to_lineage_tracker(
            self.lineage_actor.clone().unwrap(),
        ));
    }
}

// Function which is spawned when a stopmessage has to be send to the lineage actor
async fn send_stop_message_to_lineage_tracker(address: Addr<LineageActor>) {
    address
        .send(crate::provenance_tracker::actor::StopMessage {})
        .await
        .unwrap();
}

// Function which run periodically (currently every 10s) to retrieve the status from the kubernetes API and the RabbitMQ API.
async fn get_jobs_status(pipeline_job: PipelineRun, actor_addr: Addr<PipelineActor>) {
    // Retrieve status from Kube API
    let kube_status = KubeAPI::get_status(&pipeline_job);

    // Retrieve status from RabbitMQ API
    let mq_status = RabbitMqAPI::get_message_queue_counts(&pipeline_job);

    // Await both futures
    let (instances_done_counts, mq_input_channel_counts) = join!(kube_status, mq_status);

    // Update the state on the pipeline actor by sending a message
    let success = actor_addr
        .send(KubeJobStatusMessage {
            instances_done_counts,
            mq_input_channel_counts,
        })
        .await
        .unwrap();

    // Send a stopmessage to the pipeline actor whenever the pipeline has completed.
    if success {
        info!("Pipeline done. Killing actor.");
        actor_addr.send(StopMessage {}).await.unwrap();
    };
}
