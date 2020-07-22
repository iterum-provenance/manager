use crate::config;
use crate::error::ManagerError;
use crate::kube::KubeAPI;
use iterum_rust::pipeline::PipelineExecution;
// use crate::pipelines::messages::JobStatusMessage;
// use crate::pipelines::pipeline_manager::RequestAddress;
use actix_web::{delete, get, post, web, HttpResponse};
use iterum_rust::pipeline::PipelineRun;
use iterum_rust::utils;
use serde_json::json;
use std::collections::HashMap;

use crate::pipeline::actor::PipelineActor;
use crate::pipeline::messages::PipelineStatusMessage;
// use crate::provenance_tracker::actor::LineageActor;
use crate::daemon;
use actix::prelude::*;
// use crate::pipelines::message_queue::actor::MessageQueueActor;
// use crate::pipelines::pipeline_manager::PipelineManager;
use actix::prelude::Addr;
use std::sync::RwLock;

#[get("/pipelines")]
async fn get_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    let mut stopped_pipelines: Vec<String> = Vec::new();
    let mut active_pipelines: Vec<String> = Vec::new();
    {
        let read_lock = config.addresses.read().unwrap();

        for (key, address) in read_lock.iter() {
            let result = match address.send(PipelineStatusMessage {}).await {
                Ok(_) => true,
                Err(err) => match err {
                    MailboxError::Closed => false,
                    MailboxError::Timeout => true,
                },
            };
            if result {
                active_pipelines.push(key.clone())
            } else {
                stopped_pipelines.push(key.clone())
            }
        }
    }
    {
        let mut write_lock = config.addresses.write().unwrap();
        for stopped_pipeline in stopped_pipelines.iter() {
            write_lock.remove(stopped_pipeline);
        }
    }
    Ok(HttpResponse::Ok().json(active_pipelines))
}

async fn get_pipeline_execution_from_actors_or_daemon(
    pipeline_addresses: &RwLock<HashMap<String, Addr<PipelineActor>>>,
    pipeline_hash: &str,
) -> Option<PipelineExecution> {
    debug!("Retrieving pipeline hash for {}", pipeline_hash);
    let read_lock = pipeline_addresses.read().unwrap();
    let pipeline_execution = match read_lock.get(pipeline_hash) {
        Some(address) => {
            debug!("Retrieving from actor.");
            match address.send(PipelineStatusMessage {}).await {
                Ok(result) => {
                    debug!("Retrieved from actor.");
                    let parsed: PipelineExecution = serde_json::from_str(&result).unwrap();
                    Some(parsed)
                }
                Err(_) => None,
            }
        }
        None => None,
    };

    match pipeline_execution {
        Some(value) => Some(value),
        None => {
            debug!("Retrieving from daemon.");
            daemon::api::get_pipeline_execution(&pipeline_hash).await
        }
    }
}

#[get("/pipelines/{pipeline_hash}")]
async fn get_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();
    info!(
        "Retrieving pipeline hash without dataset for {}",
        pipeline_hash
    );

    let pipeline_execution =
        get_pipeline_execution_from_actors_or_daemon(&config.addresses, &pipeline_hash).await;
    match pipeline_execution {
        Some(value) => Ok(HttpResponse::Ok().json(value.pipeline_run)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

#[delete("/pipelines/{pipeline_hash}")]
async fn delete_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();
    info!(
        "Deleting pipeline hash without dataset for {}",
        pipeline_hash
    );

    // Also the actor.
    {
        let mut write_lock = config.addresses.write().unwrap();
        use crate::pipeline::actor::StopMessage;

        if let Some(address) = write_lock.get(&pipeline_hash) {
            address
                .send(StopMessage {})
                .await
                .map_err(|_| warn!("Actor does not exist."))
                .unwrap();
        }

        write_lock.remove(&pipeline_hash);
    }
    KubeAPI::delete_pipeline(&pipeline_hash).await?;
    daemon::api::delete_pipeline_execution(&pipeline_hash).await;

    Ok(HttpResponse::Ok().finish())
}

#[get("/pipelines/{pipeline_hash}/status")]
async fn get_pipeline_status(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();

    let pipeline_execution =
        get_pipeline_execution_from_actors_or_daemon(&config.addresses, &pipeline_hash).await;

    match pipeline_execution {
        Some(value) => Ok(HttpResponse::Ok().json(value.status)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

#[post("/pipelines")]
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
        let actor = PipelineActor::new(pipeline.clone());
        let address = actor.start();
        {
            let mut write_lock = config.addresses.write().unwrap();
            write_lock.insert(pipeline.pipeline_run_hash.to_string(), address);
        }
        Ok(HttpResponse::Ok().json(pipeline))
    } else {
        Ok(HttpResponse::Conflict().json(json!({"message":"Pipeline is not valid."})))
    }
}

#[delete("/pipelines")]
async fn delete_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    info!("Deleting pipeline");
    KubeAPI::delete_all().await?;

    // Also kill all actors.
    let mut write_lock = config.addresses.write().unwrap();
    use crate::pipeline::actor::StopMessage;

    for kv in write_lock.iter().enumerate() {
        let (_, address) = kv.1;
        address
            .send(StopMessage {})
            .await
            .map_err(|_| warn!("Actor does not exist."))
            .unwrap();
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
    crate::mq::RabbitMqAPI::delete_all_queues().await;
    Ok(HttpResponse::Ok().finish())
}
