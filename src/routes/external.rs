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

use actix::prelude::*;

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

#[get("/pipelines/{}")]
async fn get_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();

    let read_lock = config.addresses.read().unwrap();
    let pipeline_execution_str = match read_lock.get(&pipeline_hash) {
        Some(address) => match address.send(PipelineStatusMessage {}).await {
            Ok(result) => Some(result),
            Err(err) => None,
        },
        None => None,
    };

    if result {
        active_pipelines.push(key.clone())
    } else {
        stopped_pipelines.push(key.clone())
    }

    //     for (key, address) in read_lock.iter() {
    //         let result = match address.send(PipelineStatusMessage {}).await {
    //             Ok(_) => true,
    //             Err(err) => match err {
    //                 MailboxError::Closed => false,
    //                 MailboxError::Timeout => true,
    //             },
    //         };
    //         if result {
    //             active_pipelines.push(key.clone())
    //         } else {
    //             stopped_pipelines.push(key.clone())
    //         }
    //     }
    // }
    // {
    //     let mut write_lock = config.addresses.write().unwrap();
    //     for stopped_pipeline in stopped_pipelines.iter() {
    //         write_lock.remove(stopped_pipeline);
    //     }
    Ok(HttpResponse::Ok().json(active_pipelines))
}

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

#[delete("/delete_pipelines")]
async fn delete_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    info!("Deleting pipelines");
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
