#[macro_use]
extern crate log;
use actix_web::{web, HttpResponse};
use actix_web::{App, HttpServer};
use dotenv::dotenv;

use listenfd::ListenFd;
use std::env;

mod config;
mod daemon;
mod error;
mod kube;
mod mq;
mod pipeline;
mod provenance_tracker;
mod routes;

use std::collections::HashMap;
use std::sync::RwLock;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    env_logger::init();

    let mut listenfd = ListenFd::from_env();
    let config = web::Data::new(config::Config {
        addresses: RwLock::new(HashMap::new()),
    });

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(config.clone())
            .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                let message = format!("Error when handling JSON: {:?}", err);
                error!("{}", message);
                actix_web::error::InternalError::from_response(
                    err,
                    HttpResponse::Conflict().body(message),
                )
                .into()
            }))
            .configure(routes::init_routes)
    });

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
