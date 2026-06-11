use serde::{Deserialize, Serialize};

pub const CAPABILITIES: &[MilenaCapability] = &[
    MilenaCapability::CommandBoundary,
    MilenaCapability::EventChannel,
    MilenaCapability::MacosDevBuild,
];

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum MilenaCapability {
    CommandBoundary,
    EventChannel,
    MacosDevBuild,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AppState {
    pub app_name: String,
    pub version: String,
    pub platform: String,
    pub capabilities: Vec<MilenaCapability>,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicSessionPreviewRequest {
    pub topic: String,
    pub mode: TopicSessionMode,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum TopicSessionMode {
    Poll,
    Publish,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicSessionPreview {
    pub session_id: String,
    pub topic: String,
    pub mode: TopicSessionMode,
    pub status: String,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "event",
    content = "data"
)]
pub enum MilenaBoundaryEvent {
    BoundaryOpened {
        session_id: String,
        topic: String,
        mode: TopicSessionMode,
    },
    BoundaryReady {
        session_id: String,
        capabilities: Vec<MilenaCapability>,
    },
}

pub type CommandResult<T> = Result<T, MilenaCommandError>;

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MilenaCommandError {
    pub code: MilenaCommandErrorCode,
    pub message: String,
}

impl MilenaCommandError {
    pub fn topic_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::TopicRequired,
            message: "topic is required".to_string(),
        }
    }

    pub fn event_delivery_failed(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EventDeliveryFailed,
            message: cause.to_string(),
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum MilenaCommandErrorCode {
    TopicRequired,
    EventDeliveryFailed,
}
