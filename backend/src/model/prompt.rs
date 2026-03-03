use serde::{Deserialize, Serialize};
use std::fmt;

/// Enum representing all prompt types in the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptType {
    VideoPrompt,
    MultiViewPrompt,
    TextToImage,
    SpatialCompositionDrawing,
    FusionImage,
    SoundEffect,
    SpecialEffect,
}

impl PromptType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::VideoPrompt => "video_prompt",
            Self::MultiViewPrompt => "multi_view_prompt",
            Self::TextToImage => "text_to_image",
            Self::SpatialCompositionDrawing => "spatial_composition_drawing",
            Self::FusionImage => "fusion_image",
            Self::SoundEffect => "sound_effect",
            Self::SpecialEffect => "special_effect",
        }
    }

    /// Returns the ordered sequence of storyboard-level prompt types.
    pub fn storyboard_sequence() -> &'static [PromptType] {
        &[
            Self::TextToImage,
            Self::SpatialCompositionDrawing,
            Self::FusionImage,
            Self::SoundEffect,
            Self::SpecialEffect,
        ]
    }
}

impl fmt::Display for PromptType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Status of a prompt generation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptStatus {
    Success = 0,
    Failed = 1,
}

/// Entity mapping to MySQL `tb_media_prompt` table.
#[derive(Debug, Clone)]
pub struct MediaPrompt {
    pub task_id: i64,
    pub scene_index: i32,
    pub storyboard_index: Option<i32>,
    pub prompt_type: PromptType,
    pub prompt_content: Option<String>,
    pub status: PromptStatus,
    pub error_message: Option<String>,
    pub llm_error_code: Option<String>,
    pub llm_response_snippet: Option<String>,
    pub llm_duration_ms: Option<i64>,
    pub token_usage: Option<i32>,
    pub llm_retries: Option<i32>,
}

impl MediaPrompt {
    pub fn success(
        task_id: i64,
        scene_index: i32,
        storyboard_index: Option<i32>,
        prompt_type: PromptType,
        content: String,
        duration_ms: i64,
        token_usage: Option<i32>,
        retries: u32,
    ) -> Self {
        Self {
            task_id,
            scene_index,
            storyboard_index,
            prompt_type,
            prompt_content: Some(content),
            status: PromptStatus::Success,
            error_message: None,
            llm_error_code: None,
            llm_response_snippet: None,
            llm_duration_ms: Some(duration_ms),
            token_usage,
            llm_retries: i32::try_from(retries).ok(),
        }
    }

    pub fn failed(
        task_id: i64,
        scene_index: i32,
        storyboard_index: Option<i32>,
        prompt_type: PromptType,
        error: String,
        error_code: Option<String>,
        response_snippet: Option<String>,
        duration_ms: Option<i64>,
        retries: Option<u32>,
    ) -> Self {
        Self {
            task_id,
            scene_index,
            storyboard_index,
            prompt_type,
            prompt_content: None,
            status: PromptStatus::Failed,
            error_message: Some(error),
            llm_error_code: error_code,
            llm_response_snippet: response_snippet,
            llm_duration_ms: duration_ms,
            token_usage: None,
            llm_retries: retries.and_then(|r| i32::try_from(r).ok()),
        }
    }
}
