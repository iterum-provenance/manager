use super::models::FragmentLineage;
use crate::config;
use crate::error::ManagerError;
use crate::pipelines::messages::FragmentLineageMessage;
// use crate::pipelines::pipeline_manager::RequestAddress;
use actix_web::{post, web, HttpResponse};

// #[post("/pipeline/{pipeline_run_hash}/lineage/{fragment_id}")]
// async fn submit_fragment_lineage(
//     config: web::Data<config::Config>,
//     fragment_lineage: web::Json<FragmentLineage>,
//     path: web::Path<(String, String)>,
// ) -> Result<HttpResponse, ManagerError> {
//     let (pipeline_run_hash, fragment_id) = path.into_inner();

//     let fragment_lineage = fragment_lineage.into_inner();

//     let read_lock = config.addresses.read().unwrap();
//     let address = match read_lock.get(&pipeline_run_hash) {
//         Some(address) => address,
//         None => return Ok(HttpResponse::NotFound().finish()),
//     };

//     let success = address
//         .send(FragmentLineageMessage {
//             fragment_id,
//             fragment_lineage,
//         })
//         .await;

//     match success {
//         Ok(success) => {
//             if success {
//                 // info!("Successfully send lineage data ");
//             } else {
//                 warn!("The fragment ID was already present.");
//             }
//             Ok(HttpResponse::Ok().json(success))
//         }
//         Err(_) => {
//             error!("Could not send lineage data to actor. Maybe the actor is not active?");
//             Ok(HttpResponse::Conflict().finish())
//         }
//     }
// }

// pub fn init_routes(cfg: &mut web::ServiceConfig) {
//     cfg.service(submit_fragment_lineage);
// }
