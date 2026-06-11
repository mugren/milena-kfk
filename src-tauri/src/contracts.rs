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

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ImportKafkaShellEnvironmentRequest {
    pub environment_name: String,
    pub environment_file_path: String,
    pub auth_properties_path: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ImportKafkaShellEnvironmentResponse {
    pub environment: SavedEnvironment,
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeAuthConfig {
    pub environment: String,
    pub brokers: Vec<String>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListKafkaTopicsRequest {
    pub auth: RuntimeAuthConfig,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicList {
    pub topics: Vec<KafkaTopicMetadata>,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicMetadata {
    pub name: String,
    pub partition_count: i32,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishKafkaRecordRequest {
    pub auth: RuntimeAuthConfig,
    pub topic: String,
    pub key: Option<String>,
    pub payload: String,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishKafkaRecordResponse {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub status: String,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StartKafkaConsumerSessionRequest {
    pub auth: RuntimeAuthConfig,
    pub topics: Vec<String>,
    #[serde(default)]
    pub from_beginning: bool,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StopKafkaConsumerSessionRequest {
    pub session_id: String,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConsumerSession {
    pub session_id: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub status: String,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StopKafkaConsumerSessionResponse {
    pub session_id: String,
    pub group_id: String,
    pub status: String,
    pub cleanup: KafkaConsumerGroupCleanupAttempt,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConsumerGroupCleanupAttempt {
    pub group_id: String,
    pub attempted: bool,
    pub succeeded: bool,
    pub error: Option<String>,
}

#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRecordEvent {
    pub session_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub payload: Option<String>,
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
    KafkaConsumerStarted {
        session_id: String,
        group_id: String,
        topics: Vec<String>,
    },
    KafkaRecord {
        record: KafkaRecordEvent,
    },
    KafkaConsumerError {
        session_id: String,
        message: String,
    },
    KafkaConsumerStopped {
        session_id: String,
        group_id: String,
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

    pub fn environment_import_invalid(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::EnvironmentImportInvalid,
            message: cause.to_string(),
        }
    }

    pub fn kafka_auth_unsupported(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::KafkaAuthUnsupported,
            message: cause.to_string(),
        }
    }

    pub fn kafka_operation_failed(cause: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::KafkaOperationFailed,
            message: cause.to_string(),
        }
    }

    pub fn kafka_session_not_found(session_id: impl ToString) -> Self {
        Self {
            code: MilenaCommandErrorCode::KafkaSessionNotFound,
            message: format!("Kafka consumer session '{}' was not found", session_id.to_string()),
        }
    }

    pub fn kafka_payload_required() -> Self {
        Self {
            code: MilenaCommandErrorCode::KafkaPayloadRequired,
            message: "payload is required".to_string(),
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
    EnvironmentImportInvalid,
    KafkaAuthUnsupported,
    KafkaOperationFailed,
    KafkaSessionNotFound,
    KafkaPayloadRequired,
}
