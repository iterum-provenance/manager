use super::provenance;
use crate::config;
use crate::error::ManagerError;
use crate::pipelines::lifecycle::actor::JobStatusMessage;
use crate::pipelines::pipeline::PipelineJob;
use crate::pipelines::pipeline_manager::RequestAddress;
use actix_web::{delete, get, post, web, HttpResponse};
use amiquip::{Connection, QueueDeclareOptions};
use k8s_openapi::api::batch::v1::Job;
use kube::{api::Api, api::ListParams, Client};
use serde_json::json;
use std::collections::HashMap;
use std::env;

use crate::pipelines::lifecycle::actor::PipelineActor;
use crate::pipelines::pipeline_manager::NewPipelineMessage;

use actix::prelude::*;

async fn get_pod_names() -> Result<Vec<String>, ManagerError> {
    use k8s_openapi::api::core::v1::Pod;

    let client = Client::try_default().await.expect("create client");
    let pods: Api<Pod> = Api::namespaced(client, "default");

    let lp = ListParams::default(); //.labels("app=blog");
    let pods = pods.list(&lp).await.unwrap();

    info!("There are pods.");
    let mut names: Vec<String> = vec![];
    for pod in pods {
        let name = pod.metadata.unwrap().name.unwrap().clone();
        info!("Pod: {:?}", name);
        names.push(name)
    }
    Ok(names)
}

async fn get_job_names() -> Result<Vec<String>, ManagerError> {
    let client = Client::try_default().await.expect("create client");
    let jobs: Api<Job> = Api::namespaced(client, "default");

    let lp = ListParams::default(); //.labels("app=blog");
    let jobs = jobs.list(&lp).await.unwrap();

    info!("There are pods.");
    let mut names: Vec<String> = vec![];
    for job in jobs {
        // job.
        let name = job.metadata.unwrap().name.unwrap();
        info!("Pod: {:?}", name);
        names.push(name)
    }
    Ok(names)
}

#[post("/submit_pipeline")]
async fn submit_pipeline(pipeline: web::Json<PipelineJob>) -> Result<HttpResponse, ManagerError> {
    info!("Submitting a pipeline");

    let pipeline = pipeline.into_inner();

    pipeline.submit().await?;

    Ok(HttpResponse::Ok().json(pipeline))
}

#[post("/submit_pipeline_actor")]
async fn submit_pipeline_actor(
    config: web::Data<config::Config>,
    pipeline: web::Json<PipelineJob>,
) -> Result<HttpResponse, ManagerError> {
    info!("Submitting a pipeline");

    let pipeline = pipeline.into_inner();

    // Check whether pipeline is valid
    let mut outputs = HashMap::new();
    let mut first_node_upstream_map: HashMap<String, String> = HashMap::new();
    for step in &pipeline.steps {
        outputs.insert(
            step.output_channel.to_string(),
            format!("{}-{}", pipeline.pipeline_hash, step.name.to_string()),
        );
    }
    outputs.insert(
        pipeline.fragmenter_output_channel.to_string(),
        format!("{}-fragmenter", pipeline.pipeline_hash),
    );
    let mut invalid = false;
    for step in &pipeline.steps {
        match outputs.get(&step.input_channel) {
            Some(parent) => {
                first_node_upstream_map.insert(
                    format!("{}-{}", pipeline.pipeline_hash, step.name.to_string()),
                    parent.to_string(),
                );
            }
            None => {
                invalid = true;
            }
        };
    }
    match outputs.get(&pipeline.combiner_input_channel) {
        Some(parent) => {
            first_node_upstream_map.insert(
                format!("{}-combiner", pipeline.pipeline_hash),
                parent.to_string(),
            );
        }
        None => {
            invalid = true;
        }
    };
    if !invalid {
        pipeline.submit().await?;
        let actor = PipelineActor {
            pipeline_job: pipeline.clone(),
            statuses: HashMap::new(),
            first_node_upstream_map,
            lineage_map: HashMap::new(),
        };
        let address = actor.start();
        let result = config.manager.send(NewPipelineMessage {
            pipeline_hash: pipeline.pipeline_hash.to_string(),
            address,
        });
        result.await.unwrap();
        Ok(HttpResponse::Ok().json(pipeline))
    } else {
        Ok(HttpResponse::Conflict().json(json!({"message":"Pipeline is not valid."})))
    }
}

#[get("/get_pods")]
async fn get_pods() -> Result<HttpResponse, ManagerError> {
    info!("Getting pods");
    let pod_names = get_pod_names().await?;

    Ok(HttpResponse::Ok().json(pod_names))
}

#[get("/pipeline/{pipeline}/status")]
async fn get_pipeline_status() -> Result<HttpResponse, ManagerError> {
    info!("Getting status of pipeline");

    let queues = vec!["step1_input", "step1_output", "step2_output"];

    let mut connection =
        Connection::insecure_open(&env::var("MQ_BROKER_URL_LOCAL").unwrap()).unwrap();
    let channel = connection.open_channel(None).unwrap();

    let mut message_counts = HashMap::new();

    for queue_name in queues {
        let queue = channel
            .queue_declare(queue_name, QueueDeclareOptions::default())
            .unwrap();
        message_counts.insert(queue_name, queue.declared_message_count().unwrap());
    }

    Ok(HttpResponse::Ok().json(message_counts))
}

#[get("/pipeline/{pipeline_hash}/{step_name}/upstream_finished")]
async fn is_step_done(
    config: web::Data<config::Config>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_hash, step_name) = path.into_inner();
    info!(
        "Getting status of step named {} from pipeline with hash {}",
        step_name, pipeline_hash
    );

    let address = match config
        .manager
        .send(RequestAddress { pipeline_hash })
        .await
        .unwrap()
    {
        Some(address) => address,
        None => return Ok(HttpResponse::NotFound().finish()),
    };

    info!("Address retrieved.. {:?}", 1);
    let status = address.send(JobStatusMessage { step_name }).await;
    info!("Status: {:?}", status);
    match status {
        Ok(status) => {
            let status = status.unwrap_or(false);
            Ok(HttpResponse::Ok().json(json!({ "finished": status })))
        }
        Err(_) => Ok(HttpResponse::Conflict().finish()),
    }
}

#[get("/get_jobs")]
async fn get_jobs() -> Result<HttpResponse, ManagerError> {
    info!("Getting jobs");
    let job_names = get_job_names().await?;

    Ok(HttpResponse::Ok().json(job_names))
}

#[get("/get_pipelines")]
async fn get_pipelines() -> Result<HttpResponse, ManagerError> {
    info!("Getting pipelines");
    let pod_names = get_pod_names().await?;

    Ok(HttpResponse::Ok().json(pod_names))
}

#[delete("/delete_pipelines")]
async fn delete_pipelines() -> Result<HttpResponse, ManagerError> {
    info!("Deleting pipelines");
    PipelineJob::delete_all().await?;

    Ok(HttpResponse::Ok().finish())
}

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(get_pods);
    cfg.service(get_jobs);
    cfg.service(get_pipelines);
    cfg.service(submit_pipeline);
    cfg.service(submit_pipeline_actor);
    cfg.service(delete_pipelines);
    cfg.service(get_pipeline_status);
    cfg.service(is_step_done);

    provenance::routes::init_routes(cfg);
}
