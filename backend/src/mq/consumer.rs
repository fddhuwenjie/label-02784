use std::sync::Arc;
use std::time::Duration;

use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer,
};
use tokio::sync::{watch, Semaphore};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::config::RabbitMqConfig;
use crate::error::AppError;
use crate::model::script::TaskMessage;
use crate::task::processor::TaskProcessor;

/// RabbitMQ consumer that dispatches tasks with concurrency control.
pub struct MqConsumer {
    config: RabbitMqConfig,
    task_semaphore: Arc<Semaphore>,
    shutdown_rx: watch::Receiver<bool>,
    processor: Arc<TaskProcessor>,
}

impl MqConsumer {
    pub fn new(
        config: RabbitMqConfig,
        task_semaphore: Arc<Semaphore>,
        shutdown_rx: watch::Receiver<bool>,
        processor: Arc<TaskProcessor>,
    ) -> Self {
        Self {
            config,
            task_semaphore,
            shutdown_rx,
            processor,
        }
    }

    /// Connect to RabbitMQ and start consuming messages.
    /// Blocks until cancellation is signalled and all in-flight tasks complete.
    pub async fn run(&self) -> Result<(), AppError> {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut reconnect_attempt: u32 = 0;

        loop {
            if *shutdown_rx.borrow() {
                info!("Shutdown signal received before connect, stopping consumer");
                break;
            }

            match self.connect_and_consume().await {
                Ok(ConsumeLoopExit::Shutdown) => break,
                Ok(ConsumeLoopExit::Reconnect(reason)) => {
                    // A live session ended unexpectedly; restart from initial backoff.
                    reconnect_attempt = 1;
                    let backoff = self.reconnect_backoff(reconnect_attempt);
                    warn!(
                        attempt = reconnect_attempt,
                        reason,
                        backoff_ms = backoff.as_millis() as u64,
                        "Consumer session ended, reconnecting"
                    );
                    if !self.wait_before_reconnect(&mut shutdown_rx, backoff).await {
                        break;
                    }
                }
                Err(e) => {
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    let backoff = self.reconnect_backoff(reconnect_attempt);
                    error!(
                        error = %e,
                        attempt = reconnect_attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        "Failed to initialize consumer session, reconnecting"
                    );
                    if !self.wait_before_reconnect(&mut shutdown_rx, backoff).await {
                        break;
                    }
                }
            }
        }

        info!("Consumer shut down gracefully");
        Ok(())
    }

    async fn connect_and_consume(&self) -> Result<ConsumeLoopExit, AppError> {
        let _conn = Connection::connect(&self.config.uri, ConnectionProperties::default()).await?;
        info!("Connected to RabbitMQ");

        let channel = _conn.create_channel().await?;
        channel
            .basic_qos(self.config.prefetch_count, BasicQosOptions::default())
            .await?;

        channel
            .queue_declare(
                &self.config.consume_queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let consumer = channel
            .basic_consume(
                &self.config.consume_queue,
                "media-prompt-consumer",
                BasicConsumeOptions {
                    no_ack: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        info!(
            queue = %self.config.consume_queue,
            "Started consuming messages"
        );

        Ok(self.consume_loop(consumer, channel).await)
    }

    fn reconnect_backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let factor = 1u64 << shift;
        let delay_ms = self
            .config
            .reconnect_initial_delay_ms
            .saturating_mul(factor)
            .min(self.config.reconnect_max_delay_ms);
        Duration::from_millis(delay_ms)
    }

    async fn wait_before_reconnect(
        &self,
        shutdown_rx: &mut watch::Receiver<bool>,
        delay: Duration,
    ) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(delay) => true,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info!("Shutdown signal received during reconnect wait");
                    false
                } else {
                    true
                }
            }
        }
    }

    async fn consume_loop(&self, mut consumer: Consumer, channel: Channel) -> ConsumeLoopExit {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut join_set = tokio::task::JoinSet::new();

        let exit = loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        info!("Shutdown signal received, stopping consumer");
                        break ConsumeLoopExit::Shutdown;
                    }
                }
                delivery = consumer.next() => {
                    match delivery {
                        Some(Ok(delivery)) => {
                            let payload = delivery.data.clone();
                            let tag = delivery.delivery_tag;

                            let task_msg: TaskMessage = match serde_json::from_slice(&payload) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    error!(error = %e, "Failed to deserialize message, NACKing");
                                    let _ = channel.basic_nack(
                                        tag,
                                        BasicNackOptions { requeue: false, ..Default::default() },
                                    ).await;
                                    continue;
                                }
                            };

                            info!(task_id = task_msg.task_id, "Received task message");

                            let permit = self.task_semaphore.clone().acquire_owned().await;
                            let permit = match permit {
                                Ok(p) => p,
                                Err(_) => {
                                    if *shutdown_rx.borrow() {
                                        info!("Task semaphore closed during shutdown");
                                        break ConsumeLoopExit::Shutdown;
                                    } else {
                                        warn!("Task semaphore closed unexpectedly, reconnect required");
                                        break ConsumeLoopExit::Reconnect("task semaphore closed");
                                    }
                                }
                            };

                            let processor = self.processor.clone();
                            let ch = channel.clone();

                            join_set.spawn(async move {
                                let _permit = permit;
                                let task_id = task_msg.task_id;

                                match processor.process(task_msg).await {
                                    Ok(()) => {
                                        info!(task_id, "Task completed successfully, ACKing");
                                        if let Err(e) = ch.basic_ack(tag, BasicAckOptions::default()).await {
                                            error!(task_id, error = %e, "Failed to ACK");
                                        }
                                    }
                                    Err(e) => {
                                        error!(task_id, error = %e, "Task failed, NACKing (no requeue)");
                                        if let Err(e) = ch.basic_nack(
                                            tag,
                                            BasicNackOptions { requeue: false, ..Default::default() },
                                        ).await {
                                            error!(task_id, error = %e, "Failed to NACK");
                                        }
                                    }
                                }
                            });
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "Consumer delivery error");
                            break ConsumeLoopExit::Reconnect("delivery error");
                        }
                        None => {
                            warn!("Consumer stream ended");
                            break ConsumeLoopExit::Reconnect("consumer stream closed");
                        }
                    }
                }
            }
        };

        // Wait for all in-flight tasks to complete
        info!(
            in_flight = join_set.len(),
            "Waiting for in-flight tasks to complete"
        );
        while join_set.join_next().await.is_some() {}
        info!("All in-flight tasks completed");
        exit
    }
}

enum ConsumeLoopExit {
    Shutdown,
    Reconnect(&'static str),
}
