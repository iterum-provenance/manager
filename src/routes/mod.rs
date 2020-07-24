//! Module which contains the endpoints for the communication with the manager. It is split into two submodules: internal and external routes.
//! The internal routes are used by the pipeline artifact, and the external routes are used by the CLI.

mod external;
mod internal;

use actix_web::web;

/// Initializes the different routes, such that Actix exposes the endpoints
pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(internal::is_upstream_finished);
    cfg.service(external::submit_pipeline_actor);
    cfg.service(external::delete_pipelines);
    cfg.service(external::list_queues);
    cfg.service(external::delete_all_queues);
    cfg.service(external::get_pipelines);
    cfg.service(external::get_pipeline);
    cfg.service(external::delete_pipeline);
    cfg.service(external::purge_pipeline);
    cfg.service(external::get_pipeline_status);
}
