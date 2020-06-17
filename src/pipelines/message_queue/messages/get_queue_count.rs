use crate::pipelines::message_queue::actor::MessageQueueActor;
use actix::prelude::*;
use actix::Message;

pub struct GetQueueCountMessage {
    pub queue_name: String,
}

impl Message for GetQueueCountMessage {
    type Result = Option<u64>;
}

impl Handler<GetQueueCountMessage> for MessageQueueActor {
    type Result = Option<u64>;

    fn handle(&mut self, msg: GetQueueCountMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match self.queue_counts.get(&msg.queue_name) {
            Some(queue_count) => Some(*queue_count),
            None => None,
        }
    }
}
