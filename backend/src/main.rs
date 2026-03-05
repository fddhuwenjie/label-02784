mod config;
mod db;
mod error;
mod llm;
mod model;
mod mq;
mod server;
mod task;

use std::sync::Arc;

use tokio::sync::{watch, Semaphore};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::AppConfig;
use crate::db::mongo::MongoRepo;
use crate::db::mysql::MySqlRepo;
use crate::llm::client::LlmClient;
use crate::mq::consumer::MqConsumer;
use crate::mq::producer::MqProducer;
use crate::server::state::{HealthState, ServiceMetrics};
use crate::server::HttpServer;
use crate::task::processor::TaskProcessor;

#[tokio::main]
async fn main() {
    // Initialize structured logging
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    if let Err(e) = run().await {
        error!(error = %e, "Service terminated with error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config_path =
        std::env::var("APP_CONFIG_PATH").unwrap_or_else(|_| "config/config.yaml".to_string());
    let config = AppConfig::load(&config_path).await?;

    info!("Configuration loaded from {config_path}");

    // Initialize infrastructure clients
    let mongo = MongoRepo::connect(&config.mongodb).await?;
    let mysql = MySqlRepo::connect(&config.mysql).await?;
    let producer = MqProducer::connect(&config.rabbitmq).await?;

    // Initialize concurrency controls
    let task_semaphore = Arc::new(Semaphore::new(config.concurrency.max_tasks));
    let llm_semaphore = Arc::new(Semaphore::new(config.concurrency.max_llm_requests));

    // Initialize LLM client
    let mock_llm_enabled = config.llm.provider.eq_ignore_ascii_case("simple_text_json")
        && config
            .llm
            .base_url
            .trim_end_matches('/')
            .ends_with("/mock-llm");
    let llm_client = LlmClient::new(config.llm.clone(), llm_semaphore)?;

    // Build task processor
    let processor = Arc::new(TaskProcessor::new(
        mongo.clone(),
        mysql.clone(),
        llm_client,
        producer.clone(),
    ));

    let metrics = Arc::new(ServiceMetrics::default());
    let health_state = Arc::new(HealthState {
        rabbitmq: config.rabbitmq.clone(),
        mongo,
        mysql,
        producer,
        metrics,
        mock_llm_enabled,
    });

    // Shutdown signal channel for graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn HTTP server (health checks, metrics, mock LLM)
    let http_server = HttpServer::new(
        config.server.health_port,
        health_state,
        shutdown_rx.clone(),
    );
    let health_handle = tokio::spawn(async move {
        http_server.run().await;
    });

    // Spawn signal handler for graceful shutdown
    let signal_handle = tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutdown signal received, initiating graceful shutdown");
        let _ = shutdown_tx.send(true);
    });

    // Start the consumer (blocks until cancellation + drain)
    let consumer = MqConsumer::new(
        config.rabbitmq.clone(),
        task_semaphore,
        shutdown_rx,
        processor,
    );

    info!("Media Prompt Service starting");

    let result = consumer.run().await;

    // Cleanup
    health_handle.abort();
    signal_handle.abort();

    match result {
        Ok(()) => {
            info!("Service shut down gracefully");
            Ok(())
        }
        Err(e) => {
            error!(error = %e, "Service encountered an error");
            Err(Box::new(e))
        }
    }
}

/// Wait for SIGTERM or Ctrl+C.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to register SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => { info!("Received Ctrl+C"); }
            _ = sigterm.recv() => { info!("Received SIGTERM"); }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C");
    }
}
