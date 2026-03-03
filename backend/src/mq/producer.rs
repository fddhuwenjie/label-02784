use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use tracing::info;

use crate::config::RabbitMqConfig;
use crate::error::AppError;
use crate::model::script::TaskMessage;

/// RabbitMQ producer for sending completion messages to the downstream queue.
#[derive(Clone)]
pub struct MqProducer {
    channel: Channel,
    publish_queue: String,
}

impl MqProducer {
    /// Create a new producer, establishing its own channel on the given connection URI.
    pub async fn connect(config: &RabbitMqConfig) -> Result<Self, AppError> {
        let conn = Connection::connect(&config.uri, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel
            .queue_declare(
                &config.publish_queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        info!(queue = %config.publish_queue, "Producer connected and queue declared");

        Ok(Self {
            channel,
            publish_queue: config.publish_queue.clone(),
        })
    }

    /// Publish a task completion message to the downstream queue.
    pub async fn publish_completion(&self, task_id: i64) -> Result<(), AppError> {
        let message = TaskMessage { task_id };
        let payload = serde_json::to_vec(&message)
            .map_err(|e| AppError::Config(format!("Failed to serialize message: {e}")))?;

        self.channel
            .basic_publish(
                "",
                &self.publish_queue,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // persistent
            )
            .await?
            .await?;

        info!(task_id, queue = %self.publish_queue, "Published completion message");
        Ok(())
    }

    /// Passive queue check for readiness probe.
    pub async fn check_publish_queue(&self) -> Result<(), AppError> {
        self.channel
            .queue_declare(
                &self.publish_queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }
}
