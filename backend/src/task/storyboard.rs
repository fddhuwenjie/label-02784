use tracing::{error, info, warn};

use crate::error::AppError;
use crate::llm::client::LlmClient;
use crate::llm::types::PromptContext;
use crate::model::prompt::{MediaPrompt, PromptType};
use crate::model::script::Storyboard;

/// Result of processing a single storyboard, including partial successes.
#[derive(Debug)]
#[allow(dead_code)]
pub struct StoryboardResult {
    pub storyboard_index: i32,
    pub prompts: Vec<MediaPrompt>,
    pub has_error: bool,
}

/// Process all prompt types for a single storyboard in sequence.
///
/// Short-circuit on first failure: stops generating subsequent prompts
/// but preserves already-succeeded ones and records the error.
pub async fn process_storyboard(
    llm_client: &LlmClient,
    task_id: i64,
    scene_index: i32,
    storyboard: &Storyboard,
    scene_description: &str,
    video_prompt: &str,
    multi_view_prompt: &str,
) -> StoryboardResult {
    let sb_index = storyboard.storyboard_index;
    let storyboard_content = storyboard.content.clone().unwrap_or_default();

    let mut prompts: Vec<MediaPrompt> = Vec::new();
    let mut previous_prompts: Vec<(PromptType, String)> = Vec::new();
    let mut has_error = false;

    let sequence = PromptType::storyboard_sequence();

    for &prompt_type in sequence {
        let ctx = PromptContext {
            prompt_type,
            scene_index,
            storyboard_index: Some(sb_index),
            scene_description: scene_description.to_string(),
            storyboard_content: Some(storyboard_content.clone()),
            video_prompt: Some(video_prompt.to_string()),
            multi_view_prompt: Some(multi_view_prompt.to_string()),
            previous_prompts: previous_prompts.clone(),
        };

        match llm_client.generate(&ctx).await {
            Ok(result) => {
                info!(
                    task_id,
                    scene_index,
                    storyboard_index = sb_index,
                    prompt_type = %prompt_type,
                    "Storyboard prompt generated"
                );

                prompts.push(MediaPrompt::success(
                    task_id,
                    scene_index,
                    Some(sb_index),
                    prompt_type,
                    result.content.clone(),
                    result.metrics.duration_ms,
                    result.metrics.token_usage,
                    result.metrics.retries,
                ));

                previous_prompts.push((prompt_type, result.content));
            }
            Err(e) => {
                let error_msg = format!("{e}");
                error!(
                    task_id,
                    scene_index,
                    storyboard_index = sb_index,
                    prompt_type = %prompt_type,
                    error = %error_msg,
                    "Storyboard prompt failed, short-circuiting remaining prompts"
                );

                prompts.push(MediaPrompt::failed(
                    task_id,
                    scene_index,
                    Some(sb_index),
                    prompt_type,
                    error_msg,
                    extract_error_code(&e),
                    extract_response_snippet(&e),
                    extract_duration_ms(&e),
                    extract_retries(&e),
                ));

                has_error = true;

                // Record remaining prompt types as skipped
                let failed_idx = sequence.iter().position(|&t| t == prompt_type).unwrap();
                for &skipped_type in &sequence[failed_idx + 1..] {
                    warn!(
                        task_id,
                        scene_index,
                        storyboard_index = sb_index,
                        prompt_type = %skipped_type,
                        "Skipped due to prior failure"
                    );
                }

                break;
            }
        }
    }

    StoryboardResult {
        storyboard_index: sb_index,
        prompts,
        has_error,
    }
}

fn extract_duration_ms(err: &AppError) -> Option<i64> {
    match err {
        AppError::Llm(llm_err) => llm_err.duration_ms(),
        _ => None,
    }
}

fn extract_retries(err: &AppError) -> Option<u32> {
    match err {
        AppError::Llm(llm_err) => llm_err.retries(),
        _ => None,
    }
}

fn extract_error_code(err: &AppError) -> Option<String> {
    match err {
        AppError::Llm(llm_err) => llm_err.error_code().map(str::to_string),
        _ => None,
    }
}

fn extract_response_snippet(err: &AppError) -> Option<String> {
    match err {
        AppError::Llm(llm_err) => llm_err.response_snippet().map(str::to_string),
        _ => None,
    }
}
