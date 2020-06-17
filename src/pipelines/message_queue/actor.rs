// use crate::pipelines::pipeline::PipelineJob;
use actix::prelude::*;
// use actix::Addr;
// use crate::pipelines::provenance::models::FragmentLineage;

// use k8s_openapi::api::batch::v1::Job;
// use kube::{api::Api, api::ListParams, Client};
use crate::pipelines::message_queue::messages::RabbitMqStatusMessage;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

pub struct MessageQueueActor {
    pub queue_counts: HashMap<String, u64>,
}

impl MessageQueueActor {}

impl Actor for MessageQueueActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Message queue actor is alive");

        ctx.run_interval(Duration::from_millis(10000), |_act, context| {
            Arbiter::spawn(get_message_queue_counts(context.address()));
        });
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Message queue actor is stopped");
    }
}

async fn get_message_queue_counts(actor_addr: Addr<MessageQueueActor>) {
    // info!("Retrieving message queue counts:");

    let url = format!("{}/queues", env::var("MQ_BROKER_URL_MANAGEMENT").unwrap());

    let username = env::var("MQ_BROKER_USERNAME").unwrap();
    let password = env::var("MQ_BROKER_PASSWORD").unwrap();
    // Get queue counts..
    use super::utils::get_queues;
    let counts = get_queues(url, username, password).await;

    // info!("Counts: {:?}", counts);

    actor_addr
        .send(RabbitMqStatusMessage {
            queue_counts: counts,
        })
        .await
        .unwrap();
}

pub struct StopMessage {}

impl Message for StopMessage {
    type Result = bool;
}

impl Handler<StopMessage> for MessageQueueActor {
    type Result = bool;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        info!("Receiving stop message.");
        ctx.stop();
        true
    }
}
