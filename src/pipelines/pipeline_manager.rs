use crate::pipelines::lifecycle::actor::PipelineActor;
use actix::prelude::*;
use actix::Addr;

use std::collections::HashMap;

pub struct NewPipelineMessage {
    pub pipeline_hash: String,
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
            msg.pipeline_hash
        );
        if !self.addresses.contains_key(&msg.pipeline_hash) {
            info!("Actor addres does not exist in map yet. Adding it.");
            self.addresses.insert(msg.pipeline_hash, msg.address);
            true
        } else {
            info!("Actor address already exists in map.");
            false
        }
    }
}

pub struct RequestAddress {
    pub pipeline_hash: String,
}

impl Message for RequestAddress {
    type Result = Option<Addr<PipelineActor>>;
}

impl Handler<RequestAddress> for PipelineManager {
    // type Error
    // type Future = Ready<Result<HttpResponse, Error>>;
    type Result = Option<Addr<PipelineActor>>;

    fn handle(&mut self, msg: RequestAddress, _ctx: &mut Context<Self>) -> Self::Result {
        info!(
            "Manager returns address of actor of pipeline with hash {}",
            msg.pipeline_hash
        );

        match self.addresses.get(&msg.pipeline_hash) {
            Some(address) => Some(address.clone()),
            None => {
                info!("Pipeline manager could not retrieve address.");
                None
            }
        }
    }
}
