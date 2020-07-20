use crate::config;
use crate::error::ManagerError;
use crate::kube::KubeAPI;
// use crate::pipelines::messages::JobStatusMessage;
// use crate::pipelines::pipeline_manager::RequestAddress;
use actix_web::{delete, get, post, web, HttpResponse};
use iterum_rust::pipeline::PipelineRun;
use iterum_rust::utils;
use serde_json::json;
use std::collections::HashMap;

use crate::pipeline::actor::PipelineActor;
// use crate::pipelines::pipeline_manager::NewPipelineMessage;
use crate::provenance_tracker::actor::LineageActor;

use actix::prelude::*;

#[post("/submit_pipeline_actor")]
async fn submit_pipeline_actor(
    config: web::Data<config::Config>,
    pipeline: web::Json<PipelineRun>,
) -> Result<HttpResponse, ManagerError> {
    info!("Submitting a pipeline");

    let mut pipeline = pipeline.into_inner();
    info!("Pipeline: {:?}", pipeline);

    pipeline.pipeline_run_hash = utils::create_random_hash().to_lowercase();

    // Check whether pipeline is valid
    if pipeline.is_valid() {
        let pipeline_run = pipeline.clone();
        let job_statuses = crate::kube::misc::create_job_statuses(
            pipeline_run,
            pipeline.create_first_node_upstream_map(),
        );

        // let lineage_actor = ;
        let pipeline_hash = pipeline.clone().pipeline_run_hash;
        let lineage_actor = LineageActor {
            pipeline_run_hash: pipeline_hash,
            channel: None,
        };
        // Arbiter::spawn(future: F)()
        // let lineage_addr = SyncArbiter::start(1, move || );

        let lineage_addr = lineage_actor.start();
        let actor = PipelineActor {
            // mq_actor: config.mq_actor.clone(),
            pipeline_job: pipeline.clone(),
            lineage_actor: lineage_addr,
            mq_channel_counts: HashMap::new(),
            job_statuses,
        };
        let address = actor.start();
        let mut write_lock = config.addresses.write().unwrap();
        write_lock.insert(pipeline.pipeline_run_hash.to_string(), address);
        // let result = config.manager.send(NewPipelineMessage {
        //     pipeline_run_hash: pipeline.pipeline_run_hash.to_string(),
        //     address,
        // });
        // result.await.unwrap();
        Ok(HttpResponse::Ok().json(pipeline))
    } else {
        Ok(HttpResponse::Conflict().json(json!({"message":"Pipeline is not valid."})))
    }
}

#[delete("/delete_pipelines")]
async fn delete_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    info!("Deleting pipelines");
    KubeAPI::delete_all().await?;

    // Also kill all actors.
    let mut write_lock = config.addresses.write().unwrap();
    use crate::pipeline::actor::StopMessage;

    for kv in write_lock.iter().enumerate() {
        let (name, address) = kv.1;
        address
            .send(StopMessage {})
            .await
            .map_err(|_| warn!("Actor does not exist."));
    }
    write_lock.clear();

    Ok(HttpResponse::Ok().finish())
}

#[get("/list_queues")]
async fn list_queues() -> Result<HttpResponse, ManagerError> {
    let queue_info = crate::mq::RabbitMqAPI::get_all_queues().await;
    Ok(HttpResponse::Ok().json(queue_info))
}

#[get("/delete_queues")]
async fn delete_all_queues() -> Result<HttpResponse, ManagerError> {
    info!("Deleting all queues");
    let queue_info = crate::mq::RabbitMqAPI::delete_all_queues().await;
    Ok(HttpResponse::Ok().finish())
}
