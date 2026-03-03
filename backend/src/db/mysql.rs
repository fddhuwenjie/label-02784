use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use tracing::{info, instrument};

use crate::config::MySqlConfig;
use crate::error::AppError;
use crate::model::prompt::{MediaPrompt, PromptStatus};

/// MySQL repository for batch-writing prompt results.
#[derive(Clone)]
pub struct MySqlRepo {
    pool: MySqlPool,
}

impl MySqlRepo {
    /// Initialize MySQL connection pool.
    pub async fn connect(config: &MySqlConfig) -> Result<Self, AppError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

        info!("Connected to MySQL");
        Ok(Self { pool })
    }

    /// Batch insert prompt results in a single transaction.
    ///
    /// Uses chunked multi-row INSERT for efficiency while staying within
    /// MySQL's max_allowed_packet limits.
    #[instrument(skip(self, prompts), fields(count = prompts.len()))]
    pub async fn batch_insert_prompts(&self, prompts: &[MediaPrompt]) -> Result<(), AppError> {
        if prompts.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        // Process in chunks of 100 to avoid overly large SQL statements
        for chunk in prompts.chunks(100) {
            let mut sql = String::from(
                "INSERT INTO tb_media_prompt \
                 (task_id, scene_index, storyboard_index, prompt_type, prompt_content, \
                  status, error_message, llm_error_code, llm_response_snippet, \
                  llm_duration_ms, token_usage, llm_retries) VALUES ",
            );

            let mut params: Vec<String> = Vec::with_capacity(chunk.len());

            for _ in chunk {
                params.push("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".to_string());
            }

            sql.push_str(&params.join(", "));

            let mut query = sqlx::query(&sql);

            for prompt in chunk {
                query = query
                    .bind(prompt.task_id)
                    .bind(prompt.scene_index)
                    .bind(prompt.storyboard_index)
                    .bind(prompt.prompt_type.as_str())
                    .bind(prompt.prompt_content.as_deref())
                    .bind(prompt.status as i8)
                    .bind(prompt.error_message.as_deref())
                    .bind(prompt.llm_error_code.as_deref())
                    .bind(prompt.llm_response_snippet.as_deref())
                    .bind(prompt.llm_duration_ms)
                    .bind(prompt.token_usage)
                    .bind(prompt.llm_retries);
            }

            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;

        info!(
            count = prompts.len(),
            success = prompts
                .iter()
                .filter(|p| p.status == PromptStatus::Success)
                .count(),
            failed = prompts
                .iter()
                .filter(|p| p.status == PromptStatus::Failed)
                .count(),
            "Batch inserted prompt results"
        );

        Ok(())
    }

    /// Get the underlying pool reference (for health checks).
    #[allow(dead_code)]
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Ping MySQL for health checks.
    pub async fn ping(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}
