use super::models::FragmentLineage;
use crate::config;
use crate::error::ManagerError;
use crate::pipelines::lifecycle::actor::FragmentLineageMessage;
use crate::pipelines::pipeline_manager::RequestAddress;
use actix_web::{post, web, HttpResponse};

#[post("/pipeline/{pipeline_hash}/lineage/{fragment_id}")]
async fn submit_fragment_lineage(
    config: web::Data<config::Config>,
    fragment_lineage: web::Json<FragmentLineage>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_hash, fragment_id) = path.into_inner();
    info!(
        "Submitting lineage of fragment: {}:{}",
        pipeline_hash, fragment_id
    );

    let fragment_lineage = fragment_lineage.into_inner();

    let address = match config
        .manager
        .send(RequestAddress { pipeline_hash })
        .await
        .unwrap()
    {
        Some(address) => address,
        None => return Ok(HttpResponse::NotFound().finish()),
    };

    let success = address
        .send(FragmentLineageMessage {
            fragment_id,
            fragment_lineage,
        })
        .await;

    match success {
        Ok(success) => Ok(HttpResponse::Ok().json(success)),
        Err(_) => Ok(HttpResponse::Conflict().finish()),
    }
}

#[post("/dummy/pipeline/{pipeline_hash}/lineage/{fragment_id}")]
async fn submit_fragment_lineage_dummy(
    _config: web::Data<config::Config>,
    fragment_lineage: web::Json<FragmentLineage>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_hash, fragment_id) = path.into_inner();
    info!(
        "Submitting lineage of fragment: {}:{}",
        pipeline_hash, fragment_id
    );

    let fragment_lineage = fragment_lineage.into_inner();

    info!("{:?}", fragment_lineage);
    Ok(HttpResponse::Ok().json(true))
}

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(submit_fragment_lineage);
    cfg.service(submit_fragment_lineage_dummy);
}
