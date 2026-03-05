use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};
use serde::Serialize;

use crate::config::RabbitMqConfig;
use crate::db::mongo::MongoRepo;
use crate::db::mysql::MySqlRepo;
use crate::error::AppError;
use crate::mq::producer::MqProducer;

#[derive(Clone)]
pub struct HealthState {
    pub rabbitmq: RabbitMqConfig,
    pub mongo: MongoRepo,
    pub mysql: MySqlRepo,
    pub producer: MqProducer,
    pub metrics: Arc<ServiceMetrics>,
    pub mock_llm_enabled: bool,
}

#[derive(Default)]
pub struct ServiceMetrics {
    pub http_requests_total: AtomicU64,
    pub healthz_requests_total: AtomicU64,
    pub readyz_requests_total: AtomicU64,
    pub metrics_requests_total: AtomicU64,
    pub mock_llm_requests_total: AtomicU64,
    pub health_checks_total: AtomicU64,
    pub health_checks_failed_total: AtomicU64,
    pub health_check_duration_ms_sum: AtomicU64,
    pub health_check_last_duration_ms: AtomicU64,
    pub health_check_last_ok: AtomicU64,
    pub rate_limited_total: AtomicU64,
}

impl ServiceMetrics {
    pub fn render_prometheus(&self) -> String {
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
             media_prompt_health_check_last_ok {}\n\
             # HELP media_prompt_rate_limited_total Total rate-limited requests.\n\
             # TYPE media_prompt_rate_limited_total counter\n\
             media_prompt_rate_limited_total {}\n",
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
            self.rate_limited_total.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub timestamp_ms: u128,
    pub dependencies: DependencySet,
}

#[derive(Debug, Serialize)]
pub struct DependencySet {
    pub rabbitmq: DependencyStatus,
    pub consume_queue: DependencyStatus,
    pub downstream_queue: DependencyStatus,
    pub mongodb: DependencyStatus,
    pub mysql: DependencyStatus,
}

#[derive(Debug, Serialize)]
pub struct DependencyStatus {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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
    pub async fn check(&self) -> HealthResponse {
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
