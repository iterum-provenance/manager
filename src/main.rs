//! The manager repository, which combines the *provenance tracker*, and the *pipeline manager* of **Iterum**.
//! In general, the *provenance tracker* resides in the `provenance_tracker` submodule, and the *pipeline manager* resides in the rest of the submodules.
//! To support the running of multiple pipelines simultaneously, the manager uses the actor system. It spawns a new actor for each of the pipelines. This actor, in turn, spawns an actor for the provenance tracker.
//! After a pipeline has completed (or failed), this actor and its children actors are killed.

#[macro_use]
extern crate log;
use actix_web::{web, App, HttpResponse, HttpServer};
use dotenv::dotenv;
use listenfd::ListenFd;
use std::{collections::HashMap, env, sync::RwLock};

mod config;
mod daemon;
mod error;
mod kube;
mod mq;
mod pipeline;
mod provenance_tracker;
mod routes;

/// Main starts up the manager. It first initializes a shared hashmap, mapping pipeline hashes to Actor Addresses.
#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    // Set up environment variables, and logger
    dotenv().ok();
    env_logger::init();
    let mut listenfd = ListenFd::from_env();

    // Initialize shared config between actix workers
    let config = web::Data::new(config::Config {
        addresses: RwLock::new(HashMap::new()),
    });

    // Set up actix server, initializing endpoints
    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(config.clone())
            .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                let message = format!("Error when handling JSON: {:?}", err);
                error!("{}", message);
                actix_web::error::InternalError::from_response(err, HttpResponse::Conflict().body(message)).into()
            }))
            .configure(routes::init_routes)
    });

    // Start the actix server
    server = match listenfd.take_tcp_listener(0)? {
        Some(listener) => server.listen(listener)?,
        None => {
            let host = env::var("HOST").expect("Host not set");
            let port = env::var("PORT").expect("Port not set");
            server.bind(format!("{}:{}", host, port))?
        }
    };

    info!("Starting Iterum Cluster Manager");
    server.run().await
}
