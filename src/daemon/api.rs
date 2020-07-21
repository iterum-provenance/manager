use base64::encode;
use hyper::header::AUTHORIZATION;
use hyper::Client;
use hyper::StatusCode;
use hyper::{Body, Method, Request};
use iterum_rust::pipeline::PipelineExecution;
use serde_json::value::Value;
use std::collections::HashMap;

fn daemon_url(dataset_name: &str, pipeline_run_hash: &str) -> String {
    let url = format!(
        "http://daemon-service:3000/{}/runs/{}",
        dataset_name, pipeline_run_hash
    );
    url
}

pub async fn store_pipeline_execution(pipeline_execution: PipelineExecution) {
    let client = Client::new();

    let url = daemon_url(
        &pipeline_execution.pipeline_run.input_dataset,
        &pipeline_execution.pipeline_run.pipeline_run_hash,
    );

    let pe_string = serde_json::to_string_pretty(&pipeline_execution).unwrap();

    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .body(Body::from(pe_string))
        .unwrap();

    client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");
}

pub async fn get_pipeline_execution() -> PipelineExecution {
    let client = Client::new();

    let url = daemon_url(
        &pipeline_execution.pipeline_run.input_dataset,
        &pipeline_execution.pipeline_run.pipeline_run_hash,
    );

    // let pe_string = serde_json::to_string_pretty(&pipeline_execution).unwrap();

    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::from(""))
        .unwrap();

    let resp = client
        .request(req)
        .await
        .expect("Was unable to reach the daemon.");

    let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
    let string = std::str::from_utf8(&bytes).unwrap();
    let parsed: PipelineExecution = serde_json::from_str(string).unwrap();

    parsed
}
