use crate::pipelines::lifecycle::actor::PipelineActor;
use crate::pipelines::provenance::models::FragmentLineage;
use actix::prelude::*;

pub struct FragmentLineageMessage {
    pub fragment_id: String,
    pub fragment_lineage: FragmentLineage,
}

impl Message for FragmentLineageMessage {
    type Result = bool;
}

impl Handler<FragmentLineageMessage> for PipelineActor {
    type Result = bool;

    fn handle(&mut self, msg: FragmentLineageMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // info!("Received lineage for fragment: {}", msg.fragment_id);

        self.lineage_map
            .insert(msg.fragment_id, msg.fragment_lineage)
            .is_none()
    }
}
