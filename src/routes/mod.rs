mod external;
mod internal;

use actix_web::web;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(internal::is_upstream_finished);
    cfg.service(external::submit_pipeline_actor);
    cfg.service(external::delete_pipelines);
    cfg.service(external::list_queues);
    cfg.service(external::delete_all_queues);
    cfg.service(external::get_pipelines);
    cfg.service(external::get_pipeline);
    cfg.service(external::delete_pipeline);
    cfg.service(external::get_pipeline_status);
}
