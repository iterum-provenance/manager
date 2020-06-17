use crate::pipelines::message_queue::actor::MessageQueueActor;
use actix::prelude::*;
use actix::Message;
use std::collections::HashMap;

pub struct GetAllQueueCountsMessage {}

impl Message for GetAllQueueCountsMessage {
    type Result = Option<HashMap<String, u64>>;
}

impl Handler<GetAllQueueCountsMessage> for MessageQueueActor {
    type Result = Option<HashMap<String, u64>>;

    fn handle(&mut self, _msg: GetAllQueueCountsMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Some(self.queue_counts.clone())
    }
}
