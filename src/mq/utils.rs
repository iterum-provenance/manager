use base64::encode;
use hyper::header::AUTHORIZATION;
use hyper::Client;
use hyper::{Body, Method, Request};
use serde_json::value::Value;
use std::collections::HashMap;

pub async fn get_queues(
    url: String,
    username: String,
    password: String,
) -> HashMap<String, Option<usize>> {
    let client = Client::new();

    let credentials_encoded = encode(format!("{}:{}", username, password));

    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .header(AUTHORIZATION, format!("Basic {}", credentials_encoded))
        .body(Body::from(""))
        .unwrap();

    let resp = client
        .request(req)
        .await
        .expect("Was unable to reach RabbitMQ.");

    let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
    let string = std::str::from_utf8(&bytes).unwrap();
    let parsed: Value = serde_json::from_str(string).unwrap();
    let mut map: HashMap<String, Option<usize>> = HashMap::new();

    let arr = parsed.as_array().unwrap();

    for item in arr {
        let name = item.get("name").unwrap().to_string();
        let name = name[1..name.len() - 1].to_string();

        let count = match item.get("messages") {
            Some(value) => value.as_u64().unwrap(),
            None => 0,
        };
        map.insert(name, Some(count as usize));
    }
    map
}
