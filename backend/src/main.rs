mod config;
mod db;
mod error;
mod llm;
mod model;
mod mq;
mod task;

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Semaphore};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::{AppConfig, RabbitMqConfig};
use crate::db::mongo::MongoRepo;
use crate::db::mysql::MySqlRepo;
use crate::error::AppError;
use crate::llm::client::LlmClient;
use crate::mq::consumer::MqConsumer;
use crate::mq::producer::MqProducer;
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

    // Spawn health check server
    let health_port = config.server.health_port;
    let health_shutdown_rx = shutdown_rx.clone();
    let health_state = health_state.clone();
    let health_handle = tokio::spawn(async move {
        run_health_server(health_port, health_state, health_shutdown_rx).await;
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

#[derive(Clone)]
struct HealthState {
    rabbitmq: RabbitMqConfig,
    mongo: MongoRepo,
    mysql: MySqlRepo,
    producer: MqProducer,
    metrics: Arc<ServiceMetrics>,
    mock_llm_enabled: bool,
}

#[derive(Default)]
struct ServiceMetrics {
    http_requests_total: AtomicU64,
    healthz_requests_total: AtomicU64,
    readyz_requests_total: AtomicU64,
    metrics_requests_total: AtomicU64,
    mock_llm_requests_total: AtomicU64,
    health_checks_total: AtomicU64,
    health_checks_failed_total: AtomicU64,
    health_check_duration_ms_sum: AtomicU64,
    health_check_last_duration_ms: AtomicU64,
    health_check_last_ok: AtomicU64,
}

impl ServiceMetrics {
    fn render_prometheus(&self) -> String {
        format!(
            "# HELP media_prompt_http_requests_total Total HTTP requests.\n\
             # TYPE media_prompt_http_requests_total counter\n\
             media_prompt_http_requests_total {}\n\
             # HELP media_prompt_healthz_requests_total Total /healthz requests.\n\
             # TYPE media_prompt_healthz_requests_total counter\n\
             media_prompt_healthz_requests_total {}\n\
             # HELP media_prompt_readyz_requests_total Total /readyz requests.\n\
             # TYPE media_prompt_readyz_requests_total counter\n\
             media_prompt_readyz_requests_total {}\n\
             # HELP media_prompt_metrics_requests_total Total /metrics requests.\n\
             # TYPE media_prompt_metrics_requests_total counter\n\
             media_prompt_metrics_requests_total {}\n\
             # HELP media_prompt_mock_llm_requests_total Total /mock-llm requests.\n\
             # TYPE media_prompt_mock_llm_requests_total counter\n\
             media_prompt_mock_llm_requests_total {}\n\
             # HELP media_prompt_health_checks_total Total dependency health checks.\n\
             # TYPE media_prompt_health_checks_total counter\n\
             media_prompt_health_checks_total {}\n\
             # HELP media_prompt_health_checks_failed_total Failed dependency health checks.\n\
             # TYPE media_prompt_health_checks_failed_total counter\n\
             media_prompt_health_checks_failed_total {}\n\
             # HELP media_prompt_health_check_duration_ms_sum Sum of dependency check duration in ms.\n\
             # TYPE media_prompt_health_check_duration_ms_sum counter\n\
             media_prompt_health_check_duration_ms_sum {}\n\
             # HELP media_prompt_health_check_last_duration_ms Last dependency check duration in ms.\n\
             # TYPE media_prompt_health_check_last_duration_ms gauge\n\
             media_prompt_health_check_last_duration_ms {}\n\
             # HELP media_prompt_health_check_last_ok Last dependency check status (1=ok,0=degraded).\n\
             # TYPE media_prompt_health_check_last_ok gauge\n\
             media_prompt_health_check_last_ok {}\n",
            self.http_requests_total.load(Ordering::Relaxed),
            self.healthz_requests_total.load(Ordering::Relaxed),
            self.readyz_requests_total.load(Ordering::Relaxed),
            self.metrics_requests_total.load(Ordering::Relaxed),
            self.mock_llm_requests_total.load(Ordering::Relaxed),
            self.health_checks_total.load(Ordering::Relaxed),
            self.health_checks_failed_total.load(Ordering::Relaxed),
            self.health_check_duration_ms_sum.load(Ordering::Relaxed),
            self.health_check_last_duration_ms.load(Ordering::Relaxed),
            self.health_check_last_ok.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    timestamp_ms: u128,
    dependencies: DependencySet,
}

#[derive(Debug, Serialize)]
struct DependencySet {
    rabbitmq: DependencyStatus,
    consume_queue: DependencyStatus,
    downstream_queue: DependencyStatus,
    mongodb: DependencyStatus,
    mysql: DependencyStatus,
}

#[derive(Debug, Serialize)]
struct DependencyStatus {
    ok: bool,
    detail: Option<String>,
}

impl DependencyStatus {
    fn healthy() -> Self {
        Self {
            ok: true,
            detail: None,
        }
    }

    fn unhealthy(detail: String) -> Self {
        Self {
            ok: false,
            detail: Some(detail),
        }
    }
}

impl HealthState {
    async fn check(&self) -> HealthResponse {
        let rabbitmq_check =
            dependency_status("rabbitmq", check_rabbitmq_connection(&self.rabbitmq));
        let consume_queue_check =
            dependency_status("consume_queue", check_consume_queue(&self.rabbitmq));
        let downstream_queue_check =
            dependency_status("downstream_queue", self.producer.check_publish_queue());
        let mongodb_check = dependency_status("mongodb", self.mongo.ping());
        let mysql_check = dependency_status("mysql", self.mysql.ping());

        let (rabbitmq, consume_queue, downstream_queue, mongodb, mysql) = tokio::join!(
            rabbitmq_check,
            consume_queue_check,
            downstream_queue_check,
            mongodb_check,
            mysql_check
        );

        let dependencies = DependencySet {
            rabbitmq,
            consume_queue,
            downstream_queue,
            mongodb,
            mysql,
        };

        let all_ok = dependencies.rabbitmq.ok
            && dependencies.consume_queue.ok
            && dependencies.downstream_queue.ok
            && dependencies.mongodb.ok
            && dependencies.mysql.ok;

        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        HealthResponse {
            status: if all_ok { "ok" } else { "degraded" },
            timestamp_ms,
            dependencies,
        }
    }
}

async fn dependency_status<F>(name: &'static str, fut: F) -> DependencyStatus
where
    F: Future<Output = Result<(), AppError>>,
{
    match tokio::time::timeout(Duration::from_secs(3), fut).await {
        Ok(Ok(())) => DependencyStatus::healthy(),
        Ok(Err(e)) => DependencyStatus::unhealthy(format!("{name} check failed: {e}")),
        Err(_) => DependencyStatus::unhealthy(format!("{name} check timed out")),
    }
}

async fn check_rabbitmq_connection(config: &RabbitMqConfig) -> Result<(), AppError> {
    let _conn = Connection::connect(&config.uri, ConnectionProperties::default()).await?;
    Ok(())
}

async fn check_consume_queue(config: &RabbitMqConfig) -> Result<(), AppError> {
    let conn = Connection::connect(&config.uri, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    channel
        .queue_declare(
            &config.consume_queue,
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

/// HTTP service on the configured port.
/// Supports `/healthz`, `/readyz`, `/metrics`, and demo-only `/mock-llm`.
///
/// This intentionally keeps dependencies minimal for the current scope.
/// If route/middleware complexity grows, migrate this server to hyper/axum.
async fn run_health_server(
    port: u16,
    state: Arc<HealthState>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await {
        Ok(l) => {
            info!(port, "Health check server listening");
            l
        }
        Err(e) => {
            error!(port, error = %e, "Failed to bind health check port");
            return;
        }
    };

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info!("Health check server received shutdown signal");
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            handle_health_connection(stream, state).await;
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "Health check accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_health_connection(mut stream: tokio::net::TcpStream, state: Arc<HealthState>) {
    let mut req_buf = [0_u8; 1024];
    let (method, path) =
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut req_buf)).await {
            Ok(Ok(n)) if n > 0 => {
                let req = String::from_utf8_lossy(&req_buf[..n]);
                let mut parts = req.lines().next().unwrap_or("").split_whitespace();
                let method = parts.next().unwrap_or("GET").to_string();
                let path = parts.next().unwrap_or("/healthz").to_string();
                (method, path)
            }
            _ => ("GET".to_string(), "/healthz".to_string()),
        };

    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);

    if path == "/metrics" {
        state
            .metrics
            .metrics_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let body = state.metrics.render_prometheus();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    if path == "/mock-llm" {
        if !state.mock_llm_enabled {
            let body = r#"{"status":"not_found"}"#;
            let response = format!(
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
            return;
        }

        if method.as_str() != "POST" {
            let body = r#"{"error":"method_not_allowed"}"#;
            let response = format!(
                "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
            return;
        }

        state
            .metrics
            .mock_llm_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let body =
            r#"{"content":"[demo] prompt generated by local mock LLM","usage":{"total_tokens":0}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    if path == "/healthz" || path == "/readyz" {
        if path == "/healthz" {
            state
                .metrics
                .healthz_requests_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            state
                .metrics
                .readyz_requests_total
                .fetch_add(1, Ordering::Relaxed);
        }

        let started = Instant::now();
        let report = state.check().await;
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        state
            .metrics
            .health_checks_total
            .fetch_add(1, Ordering::Relaxed);
        state
            .metrics
            .health_check_duration_ms_sum
            .fetch_add(duration_ms, Ordering::Relaxed);
        state
            .metrics
            .health_check_last_duration_ms
            .store(duration_ms, Ordering::Relaxed);

        let is_ok = if report.status == "ok" { 1 } else { 0 };
        state
            .metrics
            .health_check_last_ok
            .store(is_ok, Ordering::Relaxed);
        if is_ok == 0 {
            state
                .metrics
                .health_checks_failed_total
                .fetch_add(1, Ordering::Relaxed);
        }

        let body =
            serde_json::to_string(&report).unwrap_or_else(|_| r#"{"status":"error"}"#.to_string());
        let status_line = if report.status == "ok" {
            "200 OK"
        } else {
            "503 Service Unavailable"
        };
        let response = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );

        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    let body = r#"{"status":"not_found"}"#;
    let response = format!(
        "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
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
