use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::config::LlmConfig;
use crate::error::{AppError, LlmError};
use crate::llm::types::{
    supported_llm_providers, CallMetrics, LlmAdapter, LlmResult, OpenAiCompatibleAdapter,
    ParsedLlmResponse, PromptContext, SimpleTextJsonAdapter, PROVIDER_OPENAI_COMPATIBLE,
    PROVIDER_SIMPLE_TEXT_JSON,
};

/// HTTP client for LLM API calls with retry logic and concurrency control.
#[derive(Clone)]
pub struct LlmClient {
    http: Client,
    semaphore: Arc<Semaphore>,
    config: LlmConfig,
    adapter: Arc<dyn LlmAdapter>,
}

impl LlmClient {
    /// Create a new LLM client with the given config and shared semaphore.
    pub fn new(config: LlmConfig, semaphore: Arc<Semaphore>) -> Result<Self, AppError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .pool_max_idle_per_host(20)
            .build()?;

        let adapter = build_adapter(&config.provider)?;
        info!(provider = %adapter.provider(), "LLM adapter selected");

        Ok(Self {
            http,
            semaphore,
            config,
            adapter,
        })
    }

    /// Generate a prompt by calling the LLM API with retry logic.
    ///
    /// Acquires a semaphore permit before making the HTTP call.
    /// Retry policy:
    ///   - 4xx: no retry (client error)
    ///   - 5xx / network error: up to max_retries with exponential backoff
    pub async fn generate(&self, ctx: &PromptContext) -> Result<LlmResult, AppError> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| AppError::TaskFailed("LLM semaphore closed".into()))?;

        let request_body = self.adapter.build_request(&self.config, ctx);

        let mut last_error: Option<LlmError> = None;
        let mut retries: u32 = 0;
        let start = std::time::Instant::now();

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let backoff = Duration::from_millis(1000 * 2u64.pow(attempt - 1));
                warn!(
                    prompt_type = %ctx.prompt_type,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying LLM call"
                );
                tokio::time::sleep(backoff).await;
                retries = attempt;
            }

            match self.do_request(&request_body).await {
                Ok(response) => {
                    let duration_ms = start.elapsed().as_millis() as i64;
                    let token_usage = response.token_usage;
                    let content = response.content;

                    if content.trim().is_empty() {
                        warn!(
                            prompt_type = %ctx.prompt_type,
                            scene = ctx.scene_index,
                            storyboard = ?ctx.storyboard_index,
                            provider = %self.adapter.provider(),
                            "LLM response content is empty"
                        );
                    }

                    let metrics = CallMetrics {
                        prompt_type: ctx.prompt_type,
                        scene_index: ctx.scene_index,
                        storyboard_index: ctx.storyboard_index,
                        duration_ms,
                        token_usage,
                        retries,
                        error: None,
                    };

                    info!(
                        prompt_type = %ctx.prompt_type,
                        scene = ctx.scene_index,
                        storyboard = ?ctx.storyboard_index,
                        duration_ms,
                        tokens = ?token_usage,
                        retries,
                        "LLM call succeeded"
                    );

                    return Ok(LlmResult { content, metrics });
                }
                Err(llm_err) => {
                    if !llm_err.retryable {
                        error!(
                            prompt_type = %ctx.prompt_type,
                            status = ?llm_err.status_code,
                            error = %llm_err.message,
                            "LLM call failed (non-retryable)"
                        );
                        let duration_ms = start.elapsed().as_millis() as i64;
                        let _metrics = CallMetrics {
                            prompt_type: ctx.prompt_type,
                            scene_index: ctx.scene_index,
                            storyboard_index: ctx.storyboard_index,
                            duration_ms,
                            token_usage: None,
                            retries,
                            error: Some(llm_err.message.clone()),
                        };
                        // Log metrics even on failure
                        info!(
                            prompt_type = %ctx.prompt_type,
                            duration_ms,
                            retries,
                            error = %llm_err.message,
                            "LLM call metrics (failed)"
                        );
                        return Err(AppError::Llm(
                            llm_err.with_attempt_meta(retries, duration_ms),
                        ));
                    }
                    last_error = Some(llm_err);
                }
            }
        }

        // All retries exhausted
        let err = last_error.unwrap_or_else(|| LlmError::retryable(None, "Unknown error".into()));
        let duration_ms = start.elapsed().as_millis() as i64;

        error!(
            prompt_type = %ctx.prompt_type,
            retries,
            duration_ms,
            error = %err.message,
            "LLM call failed after all retries"
        );

        Err(AppError::Llm(err.with_attempt_meta(retries, duration_ms)))
    }

    /// Execute a single HTTP request to the LLM API.
    async fn do_request(&self, body: &Value) -> Result<ParsedLlmResponse, LlmError> {
        let mut request = self
            .http
            .post(&self.config.base_url)
            .header("Content-Type", "application/json")
            .json(body);

        if !self.config.api_key.trim().is_empty() {
            request = request.header("Authorization", format!("Bearer {}", self.config.api_key));
        }

        let response = request
            .send()
            .await
            .map_err(|e| LlmError::network(format!("Network error: {e}")))?;

        let status = response.status().as_u16();

        if status >= 200 && status < 300 {
            let body_text = response.text().await.map_err(|e| {
                LlmError::non_retryable(
                    status,
                    format!("Failed to read successful response body: {e}"),
                )
            })?;

            self.adapter
                .parse_response(&body_text, status)
                .map_err(|e| {
                    warn!(
                        status,
                        provider = %self.adapter.provider(),
                        error = %e.message,
                        "Unexpected LLM response structure"
                    );
                    e
                })
        } else if status >= 400 && status < 500 {
            let body_text = response.text().await.unwrap_or_default();
            Err(LlmError::non_retryable_with_meta(
                status,
                format!("HTTP_{status}"),
                format!("Client error {status}"),
                Some(response_snippet(&body_text)),
            ))
        } else {
            let body_text = response.text().await.unwrap_or_default();
            Err(LlmError::retryable_with_meta(
                Some(status),
                format!("HTTP_{status}"),
                format!("Server error {status}"),
                Some(response_snippet(&body_text)),
            ))
        }
    }
}

fn response_snippet(body: &str) -> String {
    body.chars().take(500).collect()
}

fn build_adapter(provider: &str) -> Result<Arc<dyn LlmAdapter>, AppError> {
    let normalized = provider.trim().to_ascii_lowercase();
    match normalized.as_str() {
        PROVIDER_OPENAI_COMPATIBLE => Ok(Arc::new(OpenAiCompatibleAdapter)),
        PROVIDER_SIMPLE_TEXT_JSON => Ok(Arc::new(SimpleTextJsonAdapter)),
        _ => Err(AppError::Config(format!(
            "Unsupported llm.provider `{provider}`. Supported values: {}",
            supported_llm_providers().join(", ")
        ))),
    }
}
