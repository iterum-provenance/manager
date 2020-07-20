use super::utils::get_queues;
use amiquip::{Connection, QueueDeclareOptions, QueueDeleteOptions};
use iterum_rust::pipeline::PipelineRun;
use std::collections::HashMap;

use std::env;

pub struct RabbitMqAPI {}

impl RabbitMqAPI {
    pub async fn get_message_queue_counts(
        pipeline_job: &PipelineRun,
    ) -> HashMap<String, Option<usize>> {
        // Get queue counts..
        RabbitMqAPI::get_all_queues().await
    }

    pub async fn get_all_queues() -> HashMap<String, Option<usize>> {
        let url = format!("{}/queues", env::var("MQ_BROKER_URL_MANAGEMENT").unwrap());
        let username = env::var("MQ_BROKER_USERNAME").unwrap();
        let password = env::var("MQ_BROKER_PASSWORD").unwrap();

        // Get queue counts..
        get_queues(url, username, password).await
    }

    pub async fn delete_all_queues() {
        info!("Deleting all queues");
        let queue_info = RabbitMqAPI::get_all_queues().await;

        let mut connection =
            Connection::insecure_open(&env::var("MQ_BROKER_URL").unwrap()).unwrap();
        let channel = connection.open_channel(None).unwrap();

        for queue_name in queue_info.keys() {
            let queue = channel
                .queue_declare(queue_name, QueueDeclareOptions::default())
                .unwrap();
            let delete_options = QueueDeleteOptions::default();
            queue.delete(delete_options).unwrap();
            info!("Queue {} has been deleted.", queue_name);
        }
    }
}
