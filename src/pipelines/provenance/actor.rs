// use crate::pipelines::pipeline::PipelineJob;
use actix::prelude::*;
// use actix::Addr;
// use crate::pipelines::provenance::models::FragmentLineage;
use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, Result};
use std::env;

use super::models::FragmentLineage;
// use k8s_openapi::api::batch::v1::Job;
// use kube::{api::Api, api::ListParams, Client};
// use crate::pipelines::message_queue::messages::RabbitMqStatusMessage;
// use std::collections::HashMap;
// use std::env;
// use std::time::Duration;

// This actor consumes lineage messages from the queue and redirects it to the daemon (storage backend)
pub struct LineageActor {
    pub pipeline_run_hash: String,
    pub listener: Option<Addr<MQListenActor>>,
}

impl LineageActor {}

impl Actor for LineageActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Lineage actor started for {}", self.pipeline_run_hash);
        let pipeline_run_hash = self.pipeline_run_hash.clone();

        let mq_listener = SyncArbiter::start(1, move || MQListenActor {
            pipeline_run_hash: pipeline_run_hash.clone(),
        });
        self.listener = Some(mq_listener);

        // mq_listener.
        // Arbiter::spawn(handle_lineage_messages(self.pipeline_run_hash.clone()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Lineage actor is stopped");
        self.listener.as_ref().unwrap().do_send(StopMessage {});
    }
}

pub struct MQListenActor {
    pub pipeline_run_hash: String,
}

impl MQListenActor {}

impl Actor for MQListenActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, ctx: &mut SyncContext<Self>) {
        info!("MQ Listen actor started for {}", self.pipeline_run_hash);

        handle_lineage_messages(self.pipeline_run_hash.clone());
        // Arbiter::spawn(handle_lineage_messages(self.pipeline_run_hash.clone()));
    }

    fn stopped(&mut self, _ctx: &mut SyncContext<Self>) {
        info!("MQ Listen actor is stopped");
    }
}

fn handle_lineage_messages(pipeline_run_hash: String) {
    // info!("Getting status of pipeline");

    let queue_name = format!("{}-lineage", pipeline_run_hash);
    info!("Lineage actor will listen to {}", queue_name);

    let mut connection = Connection::insecure_open(&env::var("MQ_BROKER_URL").unwrap()).unwrap();
    let channel = connection.open_channel(None).unwrap();

    let queue = channel
        .queue_declare(queue_name, QueueDeclareOptions::default())
        .unwrap();

    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    info!("Lineage actor started listening to the lineage queue");
    for (_i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let lineage_info: FragmentLineage = serde_json::from_slice(&delivery.body).unwrap();
                info!("sending lineage data to daemon [{:?}]", lineage_info);
                info!("Not actually sending yet..");
                consumer.ack(delivery).unwrap();
            }
            other => {
                info!("Consumer has stopped: {:?}", other);
                break;
            }
        }
    }

    connection.close().unwrap();
}

pub struct StopMessage {}

impl Message for StopMessage {
    type Result = bool;
}

impl Handler<StopMessage> for LineageActor {
    type Result = bool;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        info!("Receiving stop message.");
        ctx.stop();
        true
    }
}

impl Handler<StopMessage> for MQListenActor {
    type Result = bool;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut SyncContext<Self>) -> Self::Result {
        info!("Receiving stop message.");
        ctx.stop();
        true
    }
}
