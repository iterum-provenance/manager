//! Contains the actor responsible for tracking provenance which is spawned by the pipeline actor
use crate::daemon;
use actix::prelude::*;
use futures_util::stream::StreamExt;
use iterum_rust::pipeline::PipelineRun;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use std::env;

/// This actor consumes lineage messages from the queue and redirects it to the daemon (storage backend)
pub struct LineageActor {
    pub pipeline_run: PipelineRun,
    pub channel: Option<Channel>,
}

impl LineageActor {}

impl Actor for LineageActor {
    type Context = Context<Self>;

    /// Function which is run when this actor is started
    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Lineage actor started for {}", self.pipeline_run.pipeline_run_hash);

        // Some set up for listening to the message queue
        let queue_name = format!("{}-lineage", self.pipeline_run.pipeline_run_hash);
        info!("Lineage actor will listen to {}", queue_name);
        let broker_url = env::var("MQ_BROKER_URL").unwrap();

        // Spawn an additional process which does the actual listening
        Arbiter::spawn(setup_mq_listener(
            self.pipeline_run.clone(),
            queue_name,
            broker_url,
            ctx.address(),
        ));
    }

    /// Function which is run when this actor is stopped
    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Lineage actor is stopped");

        // Stop the listening to the message queue
        self.channel
            .as_ref()
            .unwrap()
            .basic_cancel("lineage_tracker", BasicCancelOptions::default())
            .wait()
            .unwrap();
    }
}

/// Function to set up the listening to a message queue. It also starts listening, redirecting each piece of lineage information to the daemon.
async fn setup_mq_listener(
    pipeline_run: PipelineRun,
    queue_name: String,
    broker_url: String,
    address: Addr<LineageActor>,
) {
    // Set up the connection
    let conn = Connection::connect(&broker_url, ConnectionProperties::default().with_default_executor(8))
        .await
        .unwrap();
    info!("CONNECTED");
    let channel = conn.create_channel().await.unwrap();

    // Update the channel on the actor state
    address
        .send(SetChannelMessage {
            channel: channel.clone(),
        })
        .await
        .unwrap();

    // Set up consumer
    let mut consumer = channel
        .basic_consume(
            &queue_name,
            "lineage_tracker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Spawn a process listening to the queue
    Arbiter::spawn(async move {
        info!("Started listening to queue {}", queue_name);

        // Iterate over the stream
        while let Some(delivery) = consumer.next().await {
            let (channel, delivery) = match delivery {
                Ok(delivery) => delivery,
                Err(e) => {
                    info!("Channel closed: {}", e);
                    continue;
                }
            };
            match serde_json::from_slice(&delivery.data) {
                Ok(result) => {
                    // Redirect info to the daemon
                    daemon::api::store_fragment_lineage(&pipeline_run, result).await;
                }
                Err(_) => {
                    warn!("Error deserializing fragment lineage.");
                    continue;
                }
            };

            // Acknowledge the received message
            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await
                .expect("ack");
        }
    });
}

/// Message send to this actor to set the channel from which the consumer should consume message
pub struct SetChannelMessage {
    channel: Channel,
}

impl Message for SetChannelMessage {
    type Result = bool;
}

impl Handler<SetChannelMessage> for LineageActor {
    type Result = bool;

    fn handle(&mut self, msg: SetChannelMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!("Receiving channel message.");

        self.channel = Some(msg.channel);
        true
    }
}

/// Message send to this actor when it has to stop
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
