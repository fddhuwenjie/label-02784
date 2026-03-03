use serde::Deserialize;
use std::path::Path;

use crate::error::AppError;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub rabbitmq: RabbitMqConfig,
    pub mongodb: MongoDbConfig,
    pub mysql: MySqlConfig,
    pub llm: LlmConfig,
    pub concurrency: ConcurrencyConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RabbitMqConfig {
    pub uri: String,
    pub consume_queue: String,
    pub publish_queue: String,
    pub prefetch_count: u16,
    #[serde(default = "default_reconnect_initial_delay_ms")]
    pub reconnect_initial_delay_ms: u64,
    #[serde(default = "default_reconnect_max_delay_ms")]
    pub reconnect_max_delay_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MongoDbConfig {
    pub uri: String,
    pub database: String,
    pub collection: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MySqlConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LlmConfig {
    #[serde(default = "default_llm_provider")]
    pub provider: String,
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    #[serde(default)]
    pub allow_empty_api_key: bool,
    pub timeout_secs: u64,
    pub max_retries: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConcurrencyConfig {
    pub max_tasks: usize,
    pub max_llm_requests: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub health_port: u16,
}

impl AppConfig {
    /// Load configuration from a YAML file, with environment variable substitution.
    ///
    /// Supports `${ENV_VAR}` placeholders in the YAML content.
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self, AppError> {
        let path_ref = path.as_ref();
        let content = tokio::fs::read_to_string(path_ref).await.map_err(|e| {
            AppError::Config(format!(
                "Failed to read config file {}: {}",
                path_ref.display(),
                e
            ))
        })?;

        let content = substitute_env_vars(&content);

        let config: AppConfig = serde_yaml::from_str(&content)
            .map_err(|e| AppError::Config(format!("Failed to parse config YAML: {e}")))?;

        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<(), AppError> {
        if self.rabbitmq.uri.trim().is_empty() {
            return Err(AppError::Config("rabbitmq.uri is required".into()));
        }
        if self.rabbitmq.consume_queue.trim().is_empty() {
            return Err(AppError::Config(
                "rabbitmq.consume_queue is required".into(),
            ));
        }
        if self.rabbitmq.publish_queue.trim().is_empty() {
            return Err(AppError::Config(
                "rabbitmq.publish_queue is required".into(),
            ));
        }
        if self.rabbitmq.prefetch_count == 0 {
            return Err(AppError::Config(
                "rabbitmq.prefetch_count must be > 0".into(),
            ));
        }
        if self.rabbitmq.reconnect_initial_delay_ms == 0 {
            return Err(AppError::Config(
                "rabbitmq.reconnect_initial_delay_ms must be > 0".into(),
            ));
        }
        if self.rabbitmq.reconnect_max_delay_ms < self.rabbitmq.reconnect_initial_delay_ms {
            return Err(AppError::Config(
                "rabbitmq.reconnect_max_delay_ms must be >= reconnect_initial_delay_ms".into(),
            ));
        }
        if self.mongodb.uri.trim().is_empty() {
            return Err(AppError::Config("mongodb.uri is required".into()));
        }
        if self.mongodb.database.trim().is_empty() {
            return Err(AppError::Config("mongodb.database is required".into()));
        }
        if self.mongodb.collection.trim().is_empty() {
            return Err(AppError::Config("mongodb.collection is required".into()));
        }
        if self.mysql.url.trim().is_empty() {
            return Err(AppError::Config("mysql.url is required".into()));
        }
        if self.mysql.max_connections == 0 {
            return Err(AppError::Config("mysql.max_connections must be > 0".into()));
        }
        if self.llm.provider.trim().is_empty() {
            return Err(AppError::Config("llm.provider is required".into()));
        }
        if self.llm.base_url.trim().is_empty() {
            return Err(AppError::Config("llm.base_url is required".into()));
        }
        if self.llm.model.trim().is_empty() {
            return Err(AppError::Config("llm.model is required".into()));
        }
        if self.llm.api_key.trim().is_empty() && !self.llm.allow_empty_api_key {
            return Err(AppError::Config(
                "llm.api_key is required unless llm.allow_empty_api_key=true".into(),
            ));
        }
        if self.llm.timeout_secs < 1 {
            return Err(AppError::Config("llm.timeout_secs must be >= 1".into()));
        }
        if self.llm.max_retries > 10 {
            return Err(AppError::Config("llm.max_retries must be <= 10".into()));
        }
        if self.concurrency.max_tasks == 0 {
            return Err(AppError::Config("concurrency.max_tasks must be > 0".into()));
        }
        if self.concurrency.max_llm_requests == 0 {
            return Err(AppError::Config(
                "concurrency.max_llm_requests must be > 0".into(),
            ));
        }
        if self.server.health_port == 0 {
            return Err(AppError::Config("server.health_port must be > 0".into()));
        }
        Ok(())
    }
}

fn default_reconnect_initial_delay_ms() -> u64 {
    1_000
}

fn default_reconnect_max_delay_ms() -> u64 {
    30_000
}

fn default_llm_provider() -> String {
    "openai_compatible".to_string()
}

/// Replace `${VAR_NAME}` patterns with environment variable values.
fn substitute_env_vars(content: &str) -> String {
    let mut result = content.to_string();
    let mut start = 0;

    while let Some(begin) = result[start..].find("${") {
        let begin = start + begin;
        if let Some(end) = result[begin..].find('}') {
            let end = begin + end;
            let var_name = &result[begin + 2..end];
            let replacement = std::env::var(var_name).unwrap_or_default();
            result.replace_range(begin..=end, &replacement);
            start = begin + replacement.len();
        } else {
            break;
        }
    }

    result
}
