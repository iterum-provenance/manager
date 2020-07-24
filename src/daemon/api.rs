//! Contains the functions which can be called by other modules to communicate with the daemon
use hyper::{Body, Client, Method, Request};
use iterum_rust::{
    pipeline::{PipelineExecution, PipelineRun},
    provenance::FragmentLineage,
};

/// Helper function to construct the URL to communicate with the daemon
fn daemon_url() -> String {
    "http://daemon-service:3000".to_string()
}

/// Sends the pipeline execution to the daemon to be stored there in the storage backend.
pub async fn store_pipeline_execution(pipeline_execution: PipelineExecution) {
    let client = Client::new();

    let url = format!(
        "{}/{}/pipelines",
        daemon_url(),
        &pipeline_execution.pipeline_run.input_dataset,
    );

    let pe_string = serde_json::to_string_pretty(&pipeline_execution).unwrap();

    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("content-type", "application/json")
        .body(Body::from(pe_string.clone()))
        .unwrap();
    client.request(req).await.expect("Was unable to reach the daemon.");
}

/// Retrieves a pipeline execution from the daemon.
pub async fn get_pipeline_execution(pipeline_run_hash: &str) -> Option<PipelineExecution> {
    let client = Client::new();

    let url = format!("{}/pipelines/{}", daemon_url(), &pipeline_run_hash);

    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::from(""))
        .unwrap();

    let resp = client.request(req).await.expect("Was unable to reach the daemon.");

    if resp.status().is_success() {
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();
        let parsed: PipelineExecution = serde_json::from_str(string).unwrap();
        Some(parsed)
    } else {
        None
    }
}

/// Sends a delete request of a pipeline execution to the daemon
pub async fn delete_pipeline_execution(pipeline_run_hash: &str) -> bool {
    let client = Client::new();

    let url = format!("{}/pipelines/{}", daemon_url(), &pipeline_run_hash);

    let req = Request::builder()
        .method(Method::DELETE)
        .uri(url)
        .body(Body::from(""))
        .unwrap();

    let resp = client.request(req).await.expect("Was unable to reach the daemon.");

    resp.status().is_success()
}

/// Send a FragmentLineage to the daemon
pub async fn store_fragment_lineage(pipeline_run: &PipelineRun, fragment_lineage: FragmentLineage) {
    let client = Client::new();
    let url = format!(
        "{}/{}/pipelines/{}/lineage",
        daemon_url(),
        &pipeline_run.input_dataset,
        &pipeline_run.pipeline_run_hash
    );

    let fl_string = serde_json::to_string(&fragment_lineage).unwrap();

    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("content-type", "application/json")
        .body(Body::from(fl_string.clone()))
        .unwrap();

    client.request(req).await.expect("Was unable to reach the daemon.");
}
