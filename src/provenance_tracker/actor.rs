use actix::prelude::*;
use futures_util::stream::StreamExt;
use iterum_rust::provenance::FragmentLineage;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use std::env;

// This actor consumes lineage messages from the queue and redirects it to the daemon (storage backend)
pub struct LineageActor {
    pub pipeline_run_hash: String,
    pub channel: Option<Channel>,
}

impl LineageActor {}

impl Actor for LineageActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Lineage actor started for {}", self.pipeline_run_hash);

        let queue_name = format!("{}-lineage", self.pipeline_run_hash);
        info!("Lineage actor will listen to {}", queue_name);
        let broker_url = env::var("MQ_BROKER_URL").unwrap();
        // let executor = ThreadPool::new().unwrap();
        Arbiter::spawn(setup_mq_listener(queue_name, broker_url, ctx.address()));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Lineage actor is stopped");

        // Arbiter::spawn
        // let channel = self.channel.clone();
        self.channel
            .as_ref()
            .unwrap()
            .basic_cancel("lineage_tracker", BasicCancelOptions::default());
    }
}

async fn setup_mq_listener(queue_name: String, broker_url: String, address: Addr<LineageActor>) {
    let conn = Connection::connect(
        &broker_url,
        ConnectionProperties::default().with_default_executor(8),
    )
    .await
    .unwrap();
    info!("CONNECTED");
    let channel_a = conn.create_channel().await.unwrap();
    let channel_b = conn.create_channel().await.unwrap();
    address
        .send(SetChannelMessage {
            channel: channel_b.clone(),
        })
        .await
        .unwrap();
    let queue = channel_a
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    info!("Declared queue {:?}", queue);
    let mut consumer = channel_b
        .basic_consume(
            &queue_name,
            "lineage_tracker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    Arbiter::spawn(async move {
        info!("Started listening to queue {}", queue_name);
        while let Some(delivery) = consumer.next().await {
            let (channel, delivery) = delivery.expect("error in consumer");
            // info!("Received [{:?}]", delivery.data);
            let lineage_info: FragmentLineage = serde_json::from_slice(&delivery.data).unwrap();
            info!("sending lineage data to daemon [{:?}]", lineage_info);
            info!("Not actually sending yet..");
            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await
                .expect("ack");
        }
    });
}

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
        // ctx.stop();
        true
    }
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
