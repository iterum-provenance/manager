//! The internal routes are used by the pipeline artifact.
use crate::config;
use crate::error::ManagerError;
use crate::pipeline::messages::JobStatusMessage;
use actix_web::{get, web, HttpResponse};
use serde_json::json;

/// Endpoint used by the sidecars in the pipeline jobs to check whether the pods in that step can terminate.
#[get("/pipeline/{pipeline_run_hash}/{step_name}/upstream_finished")]
async fn is_upstream_finished(
    config: web::Data<config::Config>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_run_hash, step_name) = path.into_inner();
    info!(
        "Getting status of step named {} from pipeline with hash {}",
        step_name, pipeline_run_hash
    );
    let read_lock = config.addresses.read().unwrap();
    let address = match read_lock.get(&pipeline_run_hash) {
        Some(address) => address,
        None => return Ok(HttpResponse::NotFound().finish()),
    };

    let status = address.send(JobStatusMessage { step_name }).await;
    info!("Status: {:?}", status);
    match status {
        Ok(status) => Ok(HttpResponse::Ok().json(json!({ "finished": status }))),
        Err(_) => Ok(HttpResponse::Conflict().finish()),
    }
}
