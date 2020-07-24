//! Contains an api which is used to communicate with the Kubernetes API within the same cluster.
//! It is used to submit and delete the Kubernetes Jobs of the pipelines, and check on their status.

pub mod job_templates;
pub mod misc;
pub mod models;
pub use models::KubeAPI;
