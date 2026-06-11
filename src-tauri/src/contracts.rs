use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SaveEnvironmentRequest {
    pub name: String,
    pub brokers: Vec<String>,
    pub username: String,
    pub password: String,
    pub auth_properties_template: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SavedEnvironment {
    pub name: String,
    pub brokers: Vec<String>,
    pub username: String,
    pub auth_properties_template: String,
    pub password_secret_ref: String,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeAuthConfig {
    pub environment: String,
    pub brokers: Vec<String>,
    pub properties: BTreeMap<String, String>,
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

    pub fn environment_name_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentNameRequired,
            message: "environment name is required".to_string(),
        }
    }

    pub fn environment_brokers_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentBrokersRequired,
            message: "at least one broker is required".to_string(),
        }
    }

    pub fn environment_username_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentUsernameRequired,
            message: "environment username is required".to_string(),
        }
    }

    pub fn environment_password_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentPasswordRequired,
            message: "environment password is required".to_string(),
        }
    }

    pub fn environment_auth_template_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentAuthTemplateRequired,
            message: "auth.properties template is required".to_string(),
        }
    }

    pub fn environment_not_found(name: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentNotFound,
            message: format!("environment '{}' was not found", name.to_string()),
        }
    }

    pub fn environment_storage_failed(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentStorageFailed,
            message: cause.to_string(),
        }
    }

    pub fn environment_secret_store_failed(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentSecretStoreFailed,
            message: cause.to_string(),
        }
    }

    pub fn environment_secret_missing(secret_ref: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentSecretMissing,
            message: format!("missing password secret '{}'", secret_ref.to_string()),
        }
    }

    pub fn environment_auth_template_invalid(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentAuthTemplateInvalid,
            message: cause.to_string(),
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum MilenaCommandErrorCode {
    TopicRequired,
    EventDeliveryFailed,
    EnvironmentNameRequired,
    EnvironmentBrokersRequired,
    EnvironmentUsernameRequired,
    EnvironmentPasswordRequired,
    EnvironmentAuthTemplateRequired,
    EnvironmentNotFound,
    EnvironmentStorageFailed,
    EnvironmentSecretStoreFailed,
    EnvironmentSecretMissing,
    EnvironmentAuthTemplateInvalid,
}
