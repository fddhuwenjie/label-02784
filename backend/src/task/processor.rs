use tracing::{error, info};

use crate::db::mongo::MongoRepo;
use crate::db::mysql::MySqlRepo;
use crate::error::AppError;
use crate::llm::client::LlmClient;
use crate::model::prompt::MediaPrompt;
use crate::model::script::TaskMessage;
use crate::mq::producer::MqProducer;
use crate::task::scene;

/// Orchestrates the full task pipeline:
///   MongoDB query → scene processing → MySQL write → downstream publish.
pub struct TaskProcessor {
    mongo: MongoRepo,
    mysql: MySqlRepo,
    llm_client: LlmClient,
    producer: MqProducer,
}

impl TaskProcessor {
    pub fn new(
        mongo: MongoRepo,
        mysql: MySqlRepo,
        llm_client: LlmClient,
        producer: MqProducer,
    ) -> Self {
        Self {
            mongo,
            mysql,
            llm_client,
            producer,
        }
    }

    /// Process a single task end-to-end.
    ///
    /// Errors at the scene level (video_prompt / multi_view_prompt failure)
    /// cause the entire task to fail. Storyboard-level failures are recorded
    /// but do not prevent the task from completing.
    pub async fn process(&self, msg: TaskMessage) -> Result<(), AppError> {
        let task_id = msg.task_id;
        info!(task_id, "Starting task processing");

        // Step 1: Fetch script from MongoDB
        let script = self.mongo.find_script_by_task_id(task_id).await?;

        if script.scenes.is_empty() {
            return Err(AppError::TaskFailed(format!(
                "Script for task_id={task_id} has no scenes"
            )));
        }

        info!(
            task_id,
            scenes = script.scenes.len(),
            "Script loaded, beginning prompt generation"
        );

        // Step 2: Process each scene sequentially
        let mut all_prompts: Vec<MediaPrompt> = Vec::new();
        let mut total_failed_storyboards = 0usize;

        for s in &script.scenes {
            let scene_result = scene::process_scene(&self.llm_client, task_id, s).await?;

            total_failed_storyboards += scene_result.failed_storyboards;
            all_prompts.extend(scene_result.prompts);
        }

        // Step 3: Batch write all results to MySQL
        info!(
            task_id,
            total_prompts = all_prompts.len(),
            failed_storyboards = total_failed_storyboards,
            "Writing prompt results to MySQL"
        );

        self.mysql
            .batch_insert_prompts(&all_prompts)
            .await
            .map_err(|e| {
                error!(task_id, error = %e, "MySQL batch insert failed");
                e
            })?;

        // Step 4: Publish completion message to downstream queue
        self.producer
            .publish_completion(task_id)
            .await
            .map_err(|e| {
                error!(task_id, error = %e, "Failed to publish completion message");
                e
            })?;

        info!(
            task_id,
            total_prompts = all_prompts.len(),
            failed_storyboards = total_failed_storyboards,
            "Task completed successfully"
        );

        Ok(())
    }
}
