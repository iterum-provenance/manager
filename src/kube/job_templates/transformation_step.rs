//! Contains template to use for the creation of jobs a transformation step

use iterum_rust::pipeline::PipelineRun;
use iterum_rust::pipeline::TransformationStep;
use k8s_openapi::api::batch::v1::Job;
use serde_json::json;
use std::env;

/// Functions which creates a specific transformation step Kubernetes job from a pipeline run.
pub fn transformation_step(pipeline_job: &PipelineRun, step: &TransformationStep) -> Job {
    // Create some variables to be passed to the job
    let name = format!("{}-{}", pipeline_job.pipeline_run_hash, step.name);
    let outputbucket = format!("{}-output", &name);
    let input_channel = format!("{}-{}", &pipeline_job.pipeline_run_hash, &step.input_channel);
    let output_channel = format!("{}-{}", &pipeline_job.pipeline_run_hash, &step.output_channel);

    // Construct a config to be passed to the job
    let local_config = step.config.clone();
    let mut global_config = pipeline_job.config.clone();
    global_config.config.extend(local_config.config);
    global_config.config_files.extend(local_config.config_files);

    // Combine the extracted configs in the correct manner
    let iterum_config = serde_json::to_string(&global_config).unwrap();

    // Also create a prefetch count value
    let mq_prefetch_count_string = match step.prefetch_count {
        Some(count) => count.to_string(),
        None => String::from(""),
    };

    // Create the actual job
    let job: Job = serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": name, "labels": {
            "pipeline_run_hash": pipeline_job.pipeline_run_hash,
            "input_channel": input_channel,
            "output_channel": output_channel
        } },
        "spec": {
            "parallelism": step.instance_count,
            "completions": step.instance_count,
            "template": {
                "metadata": {
                    "name": name,

                },
                "spec": {
                    "volumes": [
                        {"name": "data-volume", "emptyDir": {}}
                    ],
                    "containers": [{
                        "name": "sidecar",
                        "image": env::var("SIDECAR_IMAGE").unwrap(),
                        "resources" : {
                            "limits": {
                                "cpu": "100m",
                            }
                        },
                        "env": [
                            {"name": "DATA_VOLUME_PATH", "value": "/data-volume"},
                            {"name": "ITERUM_NAME", "value": &name},
                            {"name": "PIPELINE_HASH", "value": &pipeline_job.pipeline_run_hash},

                            {"name": "MINIO_URL", "value": env::var("MINIO_URL").unwrap()},
                            {"name": "MINIO_ACCESS_KEY", "value": env::var("MINIO_ACCESS_KEY").unwrap()},
                            {"name": "MINIO_SECRET_KEY", "value": env::var("MINIO_SECRET_KEY").unwrap()},
                            {"name": "MINIO_USE_SSL", "value": env::var("MINIO_USE_SSL").unwrap()},
                            {"name": "MINIO_OUTPUT_BUCKET", "value": &outputbucket},

                            {"name": "MANAGER_URL", "value": env::var("MANAGER_URL").unwrap()},

                            {"name": "MQ_BROKER_URL", "value": env::var("MQ_BROKER_URL").unwrap()},
                            {"name": "MQ_OUTPUT_QUEUE", "value": &output_channel},
                            {"name": "MQ_INPUT_QUEUE", "value": &input_channel},
                            {"name": "MQ_PREFETCH_COUNT", "value": &mq_prefetch_count_string},

                            {"name": "TRANSFORMATION_STEP_INPUT", "value": "tts.sock"},
                            {"name": "TRANSFORMATION_STEP_OUTPUT", "value": "fts.sock"},
                            {"name": "ITERUM_CONFIG", "value": &iterum_config},
                            {"name": "ITERUM_CONFIG_PATH", "value": "config"},
                        ],
                        "volumeMounts": [{
                            "name": "data-volume",
                            "mountPath": "/data-volume"
                        }]
                    },
                    {
                        "name": "transformation-step",
                        "image": step.image,
                        "env": [
                            {"name": "DATA_VOLUME_PATH", "value": "/data-volume"},
                            {"name": "TRANSFORMATION_STEP_INPUT", "value": "tts.sock"},
                            {"name": "TRANSFORMATION_STEP_OUTPUT", "value": "fts.sock"},
                            {"name": "ITERUM_CONFIG", "value": &iterum_config},
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
    }))
    .unwrap();
    job
}
