use std::convert::TryFrom;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::LlmConfig;
use crate::error::LlmError;
use crate::model::prompt::PromptType;

pub const PROVIDER_OPENAI_COMPATIBLE: &str = "openai_compatible";
pub const PROVIDER_SIMPLE_TEXT_JSON: &str = "simple_text_json";

pub fn supported_llm_providers() -> &'static [&'static str] {
    &[PROVIDER_OPENAI_COMPATIBLE, PROVIDER_SIMPLE_TEXT_JSON]
}

/// Request body sent to the LLM API (OpenAI-compatible format).
#[derive(Debug, Serialize)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: f32,
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Response from the LLM API.
#[derive(Debug, Deserialize)]
pub struct LlmResponse {
    pub choices: Vec<Choice>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct Choice {
    pub message: ResponseMessage,
}

#[derive(Debug, Deserialize)]
pub struct ResponseMessage {
    pub content: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Usage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub total_tokens: i32,
}

/// Adapter-agnostic successful response payload.
#[derive(Debug, Clone)]
pub struct ParsedLlmResponse {
    pub content: String,
    pub token_usage: Option<i32>,
}

/// Adapter interface for provider-specific request/response formats.
pub trait LlmAdapter: Send + Sync {
    fn provider(&self) -> &'static str;
    fn build_request(&self, config: &LlmConfig, ctx: &PromptContext) -> Value;
    fn parse_response(&self, body_text: &str, status: u16) -> Result<ParsedLlmResponse, LlmError>;
}

/// OpenAI-compatible chat-completions adapter.
#[derive(Debug, Default)]
pub struct OpenAiCompatibleAdapter;

impl LlmAdapter for OpenAiCompatibleAdapter {
    fn provider(&self) -> &'static str {
        PROVIDER_OPENAI_COMPATIBLE
    }

    fn build_request(&self, config: &LlmConfig, ctx: &PromptContext) -> Value {
        let request_body = LlmRequest {
            model: config.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: ctx.build_system_prompt(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: format!("请生成 {} 提示词。", ctx.prompt_type.as_str()),
                },
            ],
            temperature: 0.7,
            max_tokens: Some(2000),
        };

        serde_json::to_value(request_body).unwrap_or_else(|_| json!({}))
    }

    fn parse_response(&self, body_text: &str, status: u16) -> Result<ParsedLlmResponse, LlmError> {
        let llm_response: LlmResponse = serde_json::from_str(body_text).map_err(|e| {
            LlmError::non_retryable_with_meta(
                status,
                "RESPONSE_PARSE_ERROR",
                format!("Response parse error: {e}"),
                Some(body_snippet(body_text)),
            )
        })?;

        let token_usage = llm_response.usage.as_ref().map(|u| u.total_tokens);
        let content = llm_response
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .unwrap_or_default();

        Ok(ParsedLlmResponse {
            content,
            token_usage,
        })
    }
}

/// Generic JSON adapter for providers that return text in non-OpenAI shapes.
#[derive(Debug, Default)]
pub struct SimpleTextJsonAdapter;

impl LlmAdapter for SimpleTextJsonAdapter {
    fn provider(&self) -> &'static str {
        PROVIDER_SIMPLE_TEXT_JSON
    }

    fn build_request(&self, config: &LlmConfig, ctx: &PromptContext) -> Value {
        let prompt = format!(
            "系统指令:\n{}\n\n用户请求:\n请生成 {} 提示词。",
            ctx.build_system_prompt(),
            ctx.prompt_type.as_str()
        );

        json!({
            "model": config.model,
            "prompt": prompt,
            "temperature": 0.7,
            "max_tokens": 2000
        })
    }

    fn parse_response(&self, body_text: &str, status: u16) -> Result<ParsedLlmResponse, LlmError> {
        let value: Value = serde_json::from_str(body_text).map_err(|e| {
            LlmError::non_retryable_with_meta(
                status,
                "RESPONSE_PARSE_ERROR",
                format!("Response parse error: {e}"),
                Some(body_snippet(body_text)),
            )
        })?;

        let content = extract_first_text(&value).ok_or_else(|| {
            LlmError::non_retryable_with_meta(
                status,
                "RESPONSE_SCHEMA_ERROR",
                "No text field found in response".to_string(),
                Some(body_snippet(body_text)),
            )
        })?;

        Ok(ParsedLlmResponse {
            content,
            token_usage: extract_token_usage(&value),
        })
    }
}

fn body_snippet(body_text: &str) -> String {
    body_text.chars().take(300).collect()
}

fn extract_first_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        if !text.trim().is_empty() {
            return Some(text.to_string());
        }
    }

    for path in [
        "/text",
        "/content",
        "/output",
        "/result",
        "/data/text",
        "/data/content",
        "/choices/0/text",
        "/choices/0/message/content",
    ] {
        if let Some(text) = value.pointer(path).and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return Some(text.to_string());
            }
        }
    }

    None
}

fn extract_token_usage(value: &Value) -> Option<i32> {
    for path in ["/usage/total_tokens", "/token_usage", "/total_tokens"] {
        if let Some(node) = value.pointer(path) {
            if let Some(tokens) = node.as_i64().and_then(|v| i32::try_from(v).ok()) {
                return Some(tokens);
            }
            if let Some(tokens) = node.as_u64().and_then(|v| i32::try_from(v).ok()) {
                return Some(tokens);
            }
        }
    }
    None
}

/// Metrics collected for each LLM call.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CallMetrics {
    pub prompt_type: PromptType,
    pub scene_index: i32,
    pub storyboard_index: Option<i32>,
    pub duration_ms: i64,
    pub token_usage: Option<i32>,
    pub retries: u32,
    pub error: Option<String>,
}

/// Result of a successful LLM call.
#[derive(Debug, Clone)]
pub struct LlmResult {
    pub content: String,
    pub metrics: CallMetrics,
}

/// Context passed to the LLM for prompt generation.
#[derive(Debug, Clone)]
pub struct PromptContext {
    pub prompt_type: PromptType,
    pub scene_index: i32,
    pub storyboard_index: Option<i32>,
    pub scene_description: String,
    pub storyboard_content: Option<String>,
    pub video_prompt: Option<String>,
    pub multi_view_prompt: Option<String>,
    pub previous_prompts: Vec<(PromptType, String)>,
}

impl PromptContext {
    /// Build the system prompt based on prompt type and available context.
    pub fn build_system_prompt(&self) -> String {
        match self.prompt_type {
            PromptType::VideoPrompt => {
                format!(
                    "你是一个专业的影视视频提示词生成专家。\n\
                     请根据以下场景描述，生成一段详细的视频提示词(video prompt)。\n\
                     场景描述：{}\n\
                     要求：提示词应包含画面风格、色调、运镜方式等关键信息。",
                    self.scene_description
                )
            }
            PromptType::MultiViewPrompt => {
                format!(
                    "你是一个专业的多视角提示词生成专家。\n\
                     请根据以下场景描述和已生成的视频提示词，生成多视角提示词(multi-view prompt)。\n\
                     场景描述：{}\n\
                     视频提示词：{}\n\
                     要求：提示词应涵盖不同机位和视角的描述。",
                    self.scene_description,
                    self.video_prompt.as_deref().unwrap_or("")
                )
            }
            PromptType::TextToImage => {
                format!(
                    "你是一个专业的文生图提示词生成专家。\n\
                     请根据以下分镜内容及上下文，生成 text-to-image 提示词。\n\
                     视频提示词：{}\n\
                     多视角提示词：{}\n\
                     分镜内容：{}\n\
                     要求：提示词应精确描述画面主体、构图、光影和风格。",
                    self.video_prompt.as_deref().unwrap_or(""),
                    self.multi_view_prompt.as_deref().unwrap_or(""),
                    self.storyboard_content.as_deref().unwrap_or("")
                )
            }
            PromptType::SpatialCompositionDrawing => {
                let prev = self.get_previous(PromptType::TextToImage);
                format!(
                    "你是一个专业的空间构图绘画提示词生成专家。\n\
                     请根据以下上下文，生成空间构图提示词。\n\
                     视频提示词：{}\n\
                     多视角提示词：{}\n\
                     文生图提示词：{}\n\
                     分镜内容：{}\n\
                     要求：提示词应精确描述空间布局、前中后景层次和透视关系。",
                    self.video_prompt.as_deref().unwrap_or(""),
                    self.multi_view_prompt.as_deref().unwrap_or(""),
                    prev,
                    self.storyboard_content.as_deref().unwrap_or("")
                )
            }
            PromptType::FusionImage => {
                let t2i = self.get_previous(PromptType::TextToImage);
                let scd = self.get_previous(PromptType::SpatialCompositionDrawing);
                format!(
                    "你是一个专业的融合图像提示词生成专家。\n\
                     请根据以下上下文，生成融合图像提示词。\n\
                     视频提示词：{}\n\
                     多视角提示词：{}\n\
                     文生图提示词：{}\n\
                     空间构图提示词：{}\n\
                     分镜内容：{}\n\
                     要求：提示词应描述如何将各元素融合为最终画面。",
                    self.video_prompt.as_deref().unwrap_or(""),
                    self.multi_view_prompt.as_deref().unwrap_or(""),
                    t2i,
                    scd,
                    self.storyboard_content.as_deref().unwrap_or("")
                )
            }
            PromptType::SoundEffect => {
                format!(
                    "你是一个专业的音效设计提示词生成专家。\n\
                     请根据以下分镜内容和视觉提示词，生成音效提示词。\n\
                     视频提示词：{}\n\
                     分镜内容：{}\n\
                     要求：提示词应描述环境音、音效类型、节奏和情绪氛围。",
                    self.video_prompt.as_deref().unwrap_or(""),
                    self.storyboard_content.as_deref().unwrap_or("")
                )
            }
            PromptType::SpecialEffect => {
                format!(
                    "你是一个专业的特效设计提示词生成专家。\n\
                     请根据以下分镜内容和视觉提示词，生成特效提示词。\n\
                     视频提示词：{}\n\
                     分镜内容：{}\n\
                     要求：提示词应描述粒子效果、光影特效、转场效果等。",
                    self.video_prompt.as_deref().unwrap_or(""),
                    self.storyboard_content.as_deref().unwrap_or("")
                )
            }
        }
    }

    fn get_previous(&self, pt: PromptType) -> String {
        self.previous_prompts
            .iter()
            .find(|(t, _)| *t == pt)
            .map(|(_, c)| c.clone())
            .unwrap_or_default()
    }
}
