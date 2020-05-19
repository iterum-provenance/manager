use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use serde_json::json;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum ManagerError {
    KubeApi(kube::error::Error),
    Serialization(serde_json::error::Error),
}

impl Error for ManagerError {}

impl fmt::Display for ManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ManagerError::KubeApi(err) => write!(f, "Kubernetes error: {}", err),
            ManagerError::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl From<serde_json::error::Error> for ManagerError {
    fn from(error: serde_json::error::Error) -> ManagerError {
        ManagerError::Serialization(error)
    }
}

impl From<kube::error::Error> for ManagerError {
    fn from(error: kube::error::Error) -> ManagerError {
        ManagerError::KubeApi(error)
    }
}

impl ResponseError for ManagerError {
    fn error_response(&self) -> HttpResponse {
        let status_code = StatusCode::INTERNAL_SERVER_ERROR;

        let message = format!("{}", self);
        HttpResponse::build(status_code).json(json!({ "message": message }))
    }
}
