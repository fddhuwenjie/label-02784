use std::fmt;

/// Unified error type for the entire service.
#[derive(Debug)]
pub enum AppError {
    Config(String),
    RabbitMq(lapin::Error),
    MongoDb(mongodb::error::Error),
    MySql(sqlx::Error),
    Http(reqwest::Error),
    Serde(serde_json::Error),
    Llm(LlmError),
    TaskFailed(String),
}

/// LLM-specific error with retry semantics.
#[derive(Debug, Clone)]
pub struct LlmError {
    pub status_code: Option<u16>,
    pub message: String,
    pub retryable: bool,
    pub error_code: Option<String>,
    pub response_snippet: Option<String>,
    pub retries: Option<u32>,
    pub duration_ms: Option<i64>,
}

impl LlmError {
    pub fn non_retryable(status: u16, message: String) -> Self {
        Self {
            status_code: Some(status),
            message,
            retryable: false,
            error_code: Some(format!("HTTP_{status}")),
            response_snippet: None,
            retries: None,
            duration_ms: None,
        }
    }

    pub fn non_retryable_with_meta(
        status: u16,
        error_code: impl Into<String>,
        message: String,
        response_snippet: Option<String>,
    ) -> Self {
        Self {
            status_code: Some(status),
            message,
            retryable: false,
            error_code: Some(error_code.into()),
            response_snippet,
            retries: None,
            duration_ms: None,
        }
    }

    pub fn retryable(status: Option<u16>, message: String) -> Self {
        let error_code = status
            .map(|s| format!("HTTP_{s}"))
            .unwrap_or_else(|| "RETRYABLE_ERROR".to_string());
        Self {
            status_code: status,
            message,
            retryable: true,
            error_code: Some(error_code),
            response_snippet: None,
            retries: None,
            duration_ms: None,
        }
    }

    pub fn retryable_with_meta(
        status: Option<u16>,
        error_code: impl Into<String>,
        message: String,
        response_snippet: Option<String>,
    ) -> Self {
        Self {
            status_code: status,
            message,
            retryable: true,
            error_code: Some(error_code.into()),
            response_snippet,
            retries: None,
            duration_ms: None,
        }
    }

    pub fn network(message: String) -> Self {
        Self {
            status_code: None,
            message,
            retryable: true,
            error_code: Some("NETWORK_ERROR".to_string()),
            response_snippet: None,
            retries: None,
            duration_ms: None,
        }
    }

    pub fn with_attempt_meta(mut self, retries: u32, duration_ms: i64) -> Self {
        self.retries = Some(retries);
        self.duration_ms = Some(duration_ms);
        self
    }

    pub fn duration_ms(&self) -> Option<i64> {
        self.duration_ms
    }

    pub fn retries(&self) -> Option<u32> {
        self.retries
    }

    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    pub fn response_snippet(&self) -> Option<&str> {
        self.response_snippet.as_deref()
    }
}

impl Default for LlmError {
    fn default() -> Self {
        Self {
            status_code: None,
            message: String::new(),
            retryable: false,
            error_code: None,
            response_snippet: None,
            retries: None,
            duration_ms: None,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "Config error: {msg}"),
            Self::RabbitMq(e) => write!(f, "RabbitMQ error: {e}"),
            Self::MongoDb(e) => write!(f, "MongoDB error: {e}"),
            Self::MySql(e) => write!(f, "MySQL error: {e}"),
            Self::Http(e) => write!(f, "HTTP error: {e}"),
            Self::Serde(e) => write!(f, "Serialization error: {e}"),
            Self::Llm(e) => write!(f, "LLM error: {}", e.message),
            Self::TaskFailed(msg) => write!(f, "Task failed: {msg}"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<lapin::Error> for AppError {
    fn from(e: lapin::Error) -> Self {
        Self::RabbitMq(e)
    }
}

impl From<mongodb::error::Error> for AppError {
    fn from(e: mongodb::error::Error) -> Self {
        Self::MongoDb(e)
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        Self::MySql(e)
    }
}

impl From<reqwest::Error> for AppError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

impl From<LlmError> for AppError {
    fn from(e: LlmError) -> Self {
        Self::Llm(e)
    }
}
