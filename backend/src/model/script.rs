use serde::{Deserialize, Serialize};

/// Root document fetched from MongoDB for a given task_id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptDocument {
    #[serde(rename = "_id")]
    pub id: mongodb::bson::oid::ObjectId,
    pub task_id: i64,
    pub title: Option<String>,
    pub scenes: Vec<Scene>,
}

/// A single scene ("第N场") containing metadata and a list of storyboards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scene {
    pub scene_index: i32,
    pub description: Option<String>,
    pub storyboards: Vec<Storyboard>,
}

/// A single storyboard ("分镜") within a scene.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Storyboard {
    pub storyboard_index: i32,
    pub content: Option<String>,
    pub camera_angle: Option<String>,
    pub dialogue: Option<String>,
    pub action: Option<String>,
}

/// Incoming RabbitMQ message payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMessage {
    pub task_id: i64,
}
