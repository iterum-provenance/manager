use crate::pipelines::pipeline_manager::PipelineManager;
use actix::prelude::Addr;

pub struct Config {
    pub manager: Addr<PipelineManager>,
}
