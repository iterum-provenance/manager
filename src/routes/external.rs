//! The external routes are used by the CLI to retrieve information about pipelines such that it can be presented to the user
use crate::config;
use crate::daemon;
use crate::error::ManagerError;
use crate::kube::KubeAPI;
use crate::pipeline::actor::PipelineActor;
use crate::pipeline::messages::PipelineStatusMessage;
use actix::prelude::Addr;
use actix::prelude::*;
use actix_web::{delete, get, post, web, HttpResponse};
use iterum_rust::pipeline::PipelineExecution;
use iterum_rust::pipeline::PipelineRun;
use iterum_rust::utils;
use serde_json::json;
use std::collections::HashMap;
use std::sync::RwLock;

/// Retrieve pipelines known to the manager
#[get("/pipelines")]
async fn get_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    let mut stopped_pipelines: Vec<String> = Vec::new();
    let mut active_pipelines: Vec<String> = Vec::new();
    {
        let read_lock = config.addresses.read().unwrap();

        // Iterate over the pipelines in the hashmap
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
    // Only return the still active pipelines
    Ok(HttpResponse::Ok().json(active_pipelines))
}

/// Helper function to retrieve a specific pipeline execution.
/// First tries to retrieve it from the manager, and if not present, retrieves it from the daemon.
async fn get_pipeline_execution_from_actors_or_daemon(
    pipeline_addresses: &RwLock<HashMap<String, Addr<PipelineActor>>>,
    pipeline_hash: &str,
) -> Option<PipelineExecution> {
    info!("Retrieving pipeline hash for {}", pipeline_hash);

    // Try to retrieve from actors present in the manager
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

    // If this failed, try to retrieve from the daemon
    match pipeline_execution {
        Some(value) => Some(value),
        None => {
            debug!("Retrieving from daemon.");
            daemon::api::get_pipeline_execution(&pipeline_hash).await
        }
    }
}

/// Retrieve specific pipeline
#[get("/pipelines/{pipeline_hash}")]
async fn get_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();
    info!("Retrieving pipeline hash without dataset for {}", pipeline_hash);

    let pipeline_execution = get_pipeline_execution_from_actors_or_daemon(&config.addresses, &pipeline_hash).await;
    match pipeline_execution {
        Some(value) => Ok(HttpResponse::Ok().json(value.pipeline_run)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

/// Helper to delete a pipeline, for the sake of DRY
async fn delete_pipeline_helper(config: web::Data<config::Config>, pipeline_hash: &str) -> Result<(), ManagerError> {
    // Scope the write lock, so that it is released after usage.
    {
        let mut write_lock = config.addresses.write().unwrap();
        use crate::pipeline::messages::StopMessage;

        if let Some(address) = write_lock.get(pipeline_hash) {
            match address.send(StopMessage {}).await {
                Ok(_) => info!("Send stopmessage to actor."),
                Err(err) => info!("Actor does not exist: {}", err),
            };
        }

        write_lock.remove(pipeline_hash);
    }

    // Delete the pipeline in Kubernetes
    KubeAPI::delete_pipeline(&pipeline_hash).await?;

    Ok(())
}

/// Deletes a specific pipeline
#[delete("/pipelines/{pipeline_hash}")]
async fn delete_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();
    info!("Deleting pipeline hash without dataset for {}", pipeline_hash);
    delete_pipeline_helper(config, &pipeline_hash).await?;

    Ok(HttpResponse::Ok().finish())
}

/// Purge a specific pipeline (Which also deletes the pipeline in the daemon)
#[delete("/pipelines/{pipeline_hash}/purge")]
async fn purge_pipeline(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();
    info!("Purging pipeline hash without dataset for {}", pipeline_hash);

    delete_pipeline_helper(config, &pipeline_hash).await?;
    daemon::api::delete_pipeline_execution(&pipeline_hash).await;

    Ok(HttpResponse::Ok().finish())
}

/// Retrieve the status from a specific pipeline
#[get("/pipelines/{pipeline_hash}/status")]
async fn get_pipeline_status(
    config: web::Data<config::Config>,
    pipeline_hash: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_hash = pipeline_hash.into_inner();

    let pipeline_execution = get_pipeline_execution_from_actors_or_daemon(&config.addresses, &pipeline_hash).await;

    match pipeline_execution {
        Some(value) => Ok(HttpResponse::Ok().json(value.status)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

/// Submit a new pipeline
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

        // Start a new pipeline actor
        let address = actor.start();

        // Add the pipeline to the hashmap shared between endpoints
        {
            let mut write_lock = config.addresses.write().unwrap();
            write_lock.insert(pipeline.pipeline_run_hash.to_string(), address);
        }
        Ok(HttpResponse::Ok().json(pipeline))
    } else {
        Ok(HttpResponse::Conflict().json(json!({"message":"Pipeline is not valid."})))
    }
}

/// Delete all pipelines known by Kubernetes
#[delete("/pipelines")]
async fn delete_pipelines(config: web::Data<config::Config>) -> Result<HttpResponse, ManagerError> {
    info!("Deleting pipeline");
    KubeAPI::delete_all().await?;

    // Also kill all actors.
    let mut write_lock = config.addresses.write().unwrap();
    use crate::pipeline::messages::StopMessage;

    for kv in write_lock.iter().enumerate() {
        let (_, address) = kv.1;
        match address.send(StopMessage {}).await {
            Ok(_) => info!("Send stopmessage to actor."),
            Err(err) => info!("Actor does not exist: {}", err),
        };
    }
    write_lock.clear();

    Ok(HttpResponse::Ok().finish())
}

/// List all queues, with the corresponding amount of messages in each queue
#[get("/list_queues")]
async fn list_queues() -> Result<HttpResponse, ManagerError> {
    let queue_info = crate::mq::RabbitMqAPI::get_all_queues().await;
    Ok(HttpResponse::Ok().json(queue_info))
}

/// Delete all queues
#[get("/delete_queues")]
async fn delete_all_queues() -> Result<HttpResponse, ManagerError> {
    info!("Deleting all queues");
    crate::mq::RabbitMqAPI::delete_all_queues().await;
    Ok(HttpResponse::Ok().finish())
}
