use crate::pipelines::lifecycle::actor::PipelineActor;
use crate::pipelines::lifecycle::actor::StopMessage;
use actix::prelude::*;
use actix::Addr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub struct NewPipelineMessage {
    pub pipeline_run_hash: String,
    pub address: Addr<PipelineActor>,
}

impl Message for NewPipelineMessage {
    type Result = bool;
}

pub struct PipelineManager {
    pub addresses: HashMap<String, Addr<PipelineActor>>,
}

impl Actor for PipelineManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipelinemanager actor is alive");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipelinemanager actor is stopped");
    }
}

impl Handler<NewPipelineMessage> for PipelineManager {
    type Result = bool;

    fn handle(&mut self, msg: NewPipelineMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!(
            "Added actor address of pipeline with hash {}",
            msg.pipeline_run_hash
        );
        // let entry = self.addresses.entry(msg.pipeline_run_hash.to_string());
        match self.addresses.entry(msg.pipeline_run_hash) {
            Entry::Occupied(mut old) => {
                error!("Actor address already exists in map. This should not happen.");
                error!("Replacing the actor address and killing the one already present.");
                Arbiter::spawn(send_stop_message(old.get().clone()));
                old.insert(msg.address);
                false
            }
            Entry::Vacant(v) => {
                info!("Actor addres does not exist in map yet. Adding it.");
                v.insert(msg.address);
                true
            }
        }
    }
}

async fn send_stop_message(address: Addr<PipelineActor>) {
    address.send(StopMessage {}).await.unwrap();
}

pub struct RequestAddress {
    pub pipeline_run_hash: String,
}

impl Message for RequestAddress {
    type Result = Option<Addr<PipelineActor>>;
}

impl Handler<RequestAddress> for PipelineManager {
    type Result = Option<Addr<PipelineActor>>;

    fn handle(&mut self, msg: RequestAddress, _ctx: &mut Context<Self>) -> Self::Result {
        info!(
            "Manager returns address of actor of pipeline with hash {}",
            msg.pipeline_run_hash
        );

        match self.addresses.get(&msg.pipeline_run_hash) {
            Some(address) => Some(address.clone()),
            None => {
                info!("Pipeline manager could not retrieve address.");
                None
            }
        }
    }
}

pub struct RemoveActorFromMapMessage {
    pub pipeline_run_hash: String,
}

impl Message for RemoveActorFromMapMessage {
    type Result = bool;
}

impl Handler<RemoveActorFromMapMessage> for PipelineManager {
    type Result = bool;

    fn handle(&mut self, msg: RemoveActorFromMapMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!(
            "Manager removes actor from pipeline hash {} from the hashmap.",
            msg.pipeline_run_hash
        );

        self.addresses.remove(&msg.pipeline_run_hash).is_some()
    }
}
