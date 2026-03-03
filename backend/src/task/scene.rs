use tracing::{error, info};

use crate::error::AppError;
use crate::llm::client::LlmClient;
use crate::llm::types::PromptContext;
use crate::model::prompt::{MediaPrompt, PromptType};
use crate::model::script::Scene;
use crate::task::storyboard::{self, StoryboardResult};

/// Result of processing an entire scene, including all storyboard results.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SceneResult {
    pub scene_index: i32,
    pub prompts: Vec<MediaPrompt>,
    pub video_prompt_content: String,
    pub multi_view_prompt_content: String,
    pub failed_storyboards: usize,
}

/// Process a single scene: generate scene-level prompts, then iterate storyboards.
///
/// If either scene-level prompt (video_prompt / multi_view_prompt) fails,
/// the entire task must be aborted (propagated as Err).
pub async fn process_scene(
    llm_client: &LlmClient,
    task_id: i64,
    scene: &Scene,
) -> Result<SceneResult, AppError> {
    let scene_index = scene.scene_index;
    let scene_desc = scene.description.clone().unwrap_or_default();
    let mut all_prompts: Vec<MediaPrompt> = Vec::new();

    // --- Stage 1: video_prompt (scene-level, failure = task failure) ---
    info!(task_id, scene_index, "Generating video_prompt");

    let vp_ctx = PromptContext {
        prompt_type: PromptType::VideoPrompt,
        scene_index,
        storyboard_index: None,
        scene_description: scene_desc.clone(),
        storyboard_content: None,
        video_prompt: None,
        multi_view_prompt: None,
        previous_prompts: vec![],
    };

    let vp_result = llm_client.generate(&vp_ctx).await.map_err(|e| {
        error!(
            task_id,
            scene_index, "video_prompt failed — aborting entire task"
        );
        e
    })?;

    let video_prompt_content = vp_result.content.clone();
    all_prompts.push(MediaPrompt::success(
        task_id,
        scene_index,
        None,
        PromptType::VideoPrompt,
        vp_result.content,
        vp_result.metrics.duration_ms,
        vp_result.metrics.token_usage,
        vp_result.metrics.retries,
    ));

    // --- Stage 2: multi_view_prompt (scene-level, depends on video_prompt) ---
    info!(task_id, scene_index, "Generating multi_view_prompt");

    let mvp_ctx = PromptContext {
        prompt_type: PromptType::MultiViewPrompt,
        scene_index,
        storyboard_index: None,
        scene_description: scene_desc.clone(),
        storyboard_content: None,
        video_prompt: Some(video_prompt_content.clone()),
        multi_view_prompt: None,
        previous_prompts: vec![],
    };

    let mvp_result = llm_client.generate(&mvp_ctx).await.map_err(|e| {
        error!(
            task_id,
            scene_index, "multi_view_prompt failed — aborting entire task"
        );
        e
    })?;

    let multi_view_prompt_content = mvp_result.content.clone();
    all_prompts.push(MediaPrompt::success(
        task_id,
        scene_index,
        None,
        PromptType::MultiViewPrompt,
        mvp_result.content,
        mvp_result.metrics.duration_ms,
        mvp_result.metrics.token_usage,
        mvp_result.metrics.retries,
    ));

    // --- Stage 3: Process each storyboard ---
    let mut failed_storyboards = 0usize;

    for sb in &scene.storyboards {
        info!(
            task_id,
            scene_index,
            storyboard_index = sb.storyboard_index,
            "Processing storyboard"
        );

        let sb_result: StoryboardResult = storyboard::process_storyboard(
            llm_client,
            task_id,
            scene_index,
            sb,
            &scene_desc,
            &video_prompt_content,
            &multi_view_prompt_content,
        )
        .await;

        if sb_result.has_error {
            failed_storyboards += 1;
        }

        all_prompts.extend(sb_result.prompts);
    }

    info!(
        task_id,
        scene_index,
        total_prompts = all_prompts.len(),
        failed_storyboards,
        "Scene processing completed"
    );

    Ok(SceneResult {
        scene_index,
        prompts: all_prompts,
        video_prompt_content,
        multi_view_prompt_content,
        failed_storyboards,
    })
}
