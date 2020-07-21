use iterum_rust::pipeline::PipelineRun;
use k8s_openapi::api::batch::v1::Job;
use std::collections::HashMap;
use serde_json::{json, Value};
use std::env;
use std::collections::hash_map::Entry;

pub fn fragmenter(pipeline_job: &PipelineRun) -> Job {
    let hash = format!("{}-fragmenter", &pipeline_job.pipeline_run_hash);
    let outputbucket = format!("{}-fragmenter-output", &pipeline_job.pipeline_run_hash);
    let output_channel = format!(
        "{}-{}",
        &pipeline_job.pipeline_run_hash, &pipeline_job.fragmenter.output_channel
    );


    let mut config_files_all: HashMap<String, Vec<String>> = HashMap::new();

    for (key, value) in &pipeline_job.fragmenter.config.config_files {
        match config_files_all.entry(key.clone()) {
            Entry::Occupied(mut list) => {
                let entry = list.get_mut();
                entry.push(value.clone());
            }
            Entry::Vacant(v) => {
                v.insert(vec![value.clone()]);
            }
        }
    }
    for step in &pipeline_job.steps {
        for (key, value) in &step.config.config_files {
            match config_files_all.entry(key.clone()) {
                Entry::Occupied(mut list) => {
                    let entry = list.get_mut();
                    entry.push(value.clone());
                }
                Entry::Vacant(v) => {
                    v.insert(vec![value.clone()]);
                }
            }
        }
    }
    let local_config = pipeline_job.fragmenter.config.clone();
    let mut global_config = pipeline_job.config.clone();
    global_config.config.extend(local_config.config);
    global_config.config_files.extend(local_config.config_files);

    // fragmenter_config.config_files_all = config_files_all;
    let mut env_config: HashMap<String, Value> = HashMap::new();
    env_config.insert("config".to_owned(), serde_json::to_value(&global_config).unwrap());
    env_config.insert("config_files_all".to_owned(), serde_json::to_value(config_files_all).unwrap());

    let iterum_config = serde_json::to_string(&env_config).unwrap();
    let local_config2 = serde_json::to_string(&global_config).unwrap();

    let job: Job = serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": hash, "labels": {"pipeline_run_hash": pipeline_job.pipeline_run_hash, "input_channel": "", "output_channel": output_channel} },
        "spec": {
            "parallelism": 1,
            "template": {
                "metadata": {
                    "name": hash
                },
                "spec": {
                    "volumes": [
                        {"name": "data-volume", "emptyDir": {}}
                    ],
                    "containers": [{
                        "name": "sidecar",
                        "image": env::var("FRAGMENTER_SIDECAR_IMAGE").unwrap(),
                        "resources" : {
                            "limits": {
                                "cpu": "1000m",
                                "memory": "1024Mi",
                            }
                        },
                        "env": [
                            {"name": "DATA_VOLUME_PATH", "value": "/data-volume"},
                            {"name": "ITERUM_NAME", "value": &hash},
                            {"name": "PIPELINE_HASH", "value": &pipeline_job.pipeline_run_hash},
                            
                            {"name": "DAEMON_URL", "value": env::var("DAEMON_URL").unwrap()},
                            {"name": "DAEMON_DATASET", "value": &pipeline_job.input_dataset},
                            {"name": "DAEMON_COMMIT_HASH", "value": &pipeline_job.input_dataset_commit_hash},
                            
                            {"name": "MANAGER_URL", "value": env::var("MANAGER_URL").unwrap()},
                            
                            {"name": "MINIO_URL", "value": env::var("MINIO_URL").unwrap()},
                            {"name": "MINIO_ACCESS_KEY", "value": env::var("MINIO_ACCESS_KEY").unwrap()},
                            {"name": "MINIO_SECRET_KEY", "value": env::var("MINIO_SECRET_KEY").unwrap()},
                            {"name": "MINIO_USE_SSL", "value": env::var("MINIO_USE_SSL").unwrap()},
                            {"name": "MINIO_OUTPUT_BUCKET", "value": &outputbucket},
                            
                            {"name": "MQ_BROKER_URL", "value": env::var("MQ_BROKER_URL").unwrap()},
                            {"name": "MQ_OUTPUT_QUEUE", "value": &output_channel},
                            {"name": "MQ_INPUT_QUEUE", "value": "INVALID"},
                            
                            {"name": "FRAGMENTER_INPUT", "value": "tts.sock"},
                            {"name": "FRAGMENTER_OUTPUT", "value": "fts.sock"},
                            {"name": "ITERUM_CONFIG", "value": &iterum_config},
                            {"name": "ITERUM_CONFIG_PATH", "value": "config"},
                        ],
                        "volumeMounts": [{
                            "name": "data-volume",
                            "mountPath": "/data-volume"
                        }]
                    },
                    {
                        "name": "fragmenter",
                        "image": &pipeline_job.fragmenter.image,
                        "env": [
                            {"name": "DATA_VOLUME_PATH", "value": "/data-volume"},
                            {"name": "FRAGMENTER_INPUT", "value": "tts.sock"},
                            {"name": "FRAGMENTER_OUTPUT", "value": "fts.sock"},
                            {"name": "ITERUM_CONFIG", "value": &local_config2},
                            {"name": "ITERUM_CONFIG_PATH", "value": "config"},
                        ],
                        "volumeMounts": [{
                            "name": "data-volume",
                            "mountPath": "/data-volume"
                        }]
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    })).unwrap();
    job
}
