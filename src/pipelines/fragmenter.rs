use crate::error::ManagerError;
use crate::pipelines::pipeline::PipelineJob;
use k8s_openapi::api::batch::v1::Job;
use serde_json::json;
use std::env;

pub fn create_fragmenter_template(pipeline_job: &PipelineJob) -> Result<Job, ManagerError> {
    let hash = format!("{}-fragmenter", &pipeline_job.pipeline_run_hash);
    let outputbucket = format!("{}-fragmenter-output", &pipeline_job.pipeline_run_hash);
    let output_queue = format!(
        "{}-{}",
        &pipeline_job.pipeline_run_hash, &pipeline_job.fragmenter_output_channel
    );

    let job: Job = serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": hash, "labels": {"pipeline_run_hash": pipeline_job.pipeline_run_hash} },
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
                            {"name": "MQ_OUTPUT_QUEUE", "value": &output_queue},
                            {"name": "MQ_INPUT_QUEUE", "value": "INVALID"},

                            {"name": "FRAGMENTER_INPUT", "value": "tts.sock"},
                            {"name": "FRAGMENTER_OUTPUT", "value": "fts.sock"},
                        ],
                        "volumeMounts": [{
                            "name": "data-volume",
                            "mountPath": "/data-volume"
                        }]
                    },
                    {
                        "name": "fragmenter",
                        "image": &pipeline_job.fragmenter_image,
                        "env": [
                            {"name": "DATA_VOLUME_PATH", "value": "/data-volume"},
                            {"name": "FRAGMENTER_INPUT", "value": "tts.sock"},
                            {"name": "FRAGMENTER_OUTPUT", "value": "fts.sock"},
                        ],
                        "volumeMounts": [{
                            "name": "data-volume",
                            "mountPath": "/data-volume"
                        }]
                    }],
                    "restartPolicy": "OnFailure"
                }
            }
        }
    }))?;
    Ok(job)
}
