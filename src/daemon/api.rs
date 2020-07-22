use base64::encode;
use hyper::header::AUTHORIZATION;
use hyper::Client;
use hyper::StatusCode;
use hyper::{Body, Method, Request};
use iterum_rust::pipeline::PipelineExecution;
use iterum_rust::pipeline::PipelineRun;
use iterum_rust::provenance::FragmentLineage;

fn daemon_url() -> String {
    "http://daemon-service:3000".to_string()
}

pub async fn store_pipeline_execution(pipeline_execution: PipelineExecution) {
    let client = Client::new();

    let url = format!(
        "{}/{}/runs",
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
    client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");
}

pub async fn get_pipeline_execution(pipeline_run_hash: &str) -> Option<PipelineExecution> {
    let client = Client::new();

    let url = format!("{}/runs/{}", daemon_url(), &pipeline_run_hash);

    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::from(""))
        .unwrap();

    let resp = client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");

    if resp.status().is_success() {
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();
        let parsed: PipelineExecution = serde_json::from_str(string).unwrap();
        Some(parsed)
    } else {
        None
    }
}

pub async fn delete_pipeline_execution(pipeline_run_hash: &str) -> bool {
    let client = Client::new();

    let url = format!("{}/runs/{}", daemon_url(), &pipeline_run_hash);

    let req = Request::builder()
        .method(Method::DELETE)
        .uri(url)
        .body(Body::from(""))
        .unwrap();

    let resp = client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");

    resp.status().is_success()
}

pub async fn store_fragment_lineage(pipeline_run: &PipelineRun, fragment_lineage: FragmentLineage) {
    let client = Client::new();
    let url = format!(
        "{}/{}/runs/{}/lineage",
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

    client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");
}
