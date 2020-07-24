//! Contains the different messages passed between the different actors
pub mod job_status;
pub mod kube_job_status;
pub mod pipeline_status;
pub mod stop_message;

pub use job_status::JobStatusMessage;
pub use kube_job_status::KubeJobStatusMessage;
pub use pipeline_status::PipelineStatusMessage;
pub use stop_message::StopMessage;
