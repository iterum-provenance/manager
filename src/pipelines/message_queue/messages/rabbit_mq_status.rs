use crate::pipelines::message_queue::actor::MessageQueueActor;
use actix::prelude::*;
use actix::Message;
use std::collections::HashMap;

pub struct RabbitMqStatusMessage {
    pub queue_counts: HashMap<String, u64>,
}

impl Message for RabbitMqStatusMessage {
    type Result = bool;
}

impl Handler<RabbitMqStatusMessage> for MessageQueueActor {
    type Result = bool;

    fn handle(&mut self, msg: RabbitMqStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // Simply copy the new state..
        self.queue_counts = msg.queue_counts;

        true
    }
}
