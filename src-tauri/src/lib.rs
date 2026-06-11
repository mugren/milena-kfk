use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;

const CAPABILITIES: &[MilenaCapability] = &[
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
    app_name: String,
    version: String,
    platform: String,
    capabilities: Vec<MilenaCapability>,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicSessionPreviewRequest {
    topic: String,
    mode: TopicSessionMode,
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
    session_id: String,
    topic: String,
    mode: TopicSessionMode,
    status: String,
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

#[tauri::command]
fn get_app_state() -> AppState {
    app_state()
}

#[tauri::command]
fn preview_topic_session(
    request: TopicSessionPreviewRequest,
    on_event: Channel<MilenaBoundaryEvent>,
) -> Result<TopicSessionPreview, String> {
    let preview = topic_session_preview(request)?;

    on_event
        .send(MilenaBoundaryEvent::BoundaryOpened {
            session_id: preview.session_id.clone(),
            topic: preview.topic.clone(),
            mode: preview.mode.clone(),
        })
        .map_err(|error| error.to_string())?;

    on_event
        .send(MilenaBoundaryEvent::BoundaryReady {
            session_id: preview.session_id.clone(),
            capabilities: CAPABILITIES.to_vec(),
        })
        .map_err(|error| error.to_string())?;

    Ok(preview)
}

fn app_state() -> AppState {
    AppState {
        app_name: "Milena".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        platform: "macos-dev".to_string(),
        capabilities: CAPABILITIES.to_vec(),
    }
}

fn topic_session_preview(
    request: TopicSessionPreviewRequest,
) -> Result<TopicSessionPreview, String> {
    let topic = request.topic.trim();

    if topic.is_empty() {
        return Err("topic is required".to_string());
    }

    let mode_suffix = match request.mode {
        TopicSessionMode::Poll => "poll",
        TopicSessionMode::Publish => "publish",
    };

    Ok(TopicSessionPreview {
        session_id: format!("preview-{}-{}", mode_suffix, stable_topic_id(topic)),
        topic: topic.to_string(),
        mode: request.mode,
        status: "ready".to_string(),
    })
}

fn stable_topic_id(topic: &str) -> String {
    topic
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            get_app_state,
            preview_topic_session
        ])
        .run(tauri::generate_context!())
        .expect("error while running Milena");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_state_exposes_initial_boundary_capabilities() {
        let state = app_state();

        assert_eq!(state.app_name, "Milena");
        assert_eq!(state.platform, "macos-dev");
        assert_eq!(
            state.capabilities,
            vec![
                MilenaCapability::CommandBoundary,
                MilenaCapability::EventChannel,
                MilenaCapability::MacosDevBuild
            ]
        );
    }

    #[test]
    fn topic_session_preview_is_stable_and_ready() {
        let preview = topic_session_preview(TopicSessionPreviewRequest {
            topic: "orders.created".to_string(),
            mode: TopicSessionMode::Poll,
        })
        .expect("preview should be built");

        assert_eq!(preview.session_id, "preview-poll-orders-created");
        assert_eq!(preview.topic, "orders.created");
        assert_eq!(preview.mode, TopicSessionMode::Poll);
        assert_eq!(preview.status, "ready");
    }

    #[test]
    fn topic_session_preview_rejects_blank_topic() {
        let error = topic_session_preview(TopicSessionPreviewRequest {
            topic: " ".to_string(),
            mode: TopicSessionMode::Publish,
        })
        .expect_err("blank topics should fail");

        assert_eq!(error, "topic is required");
    }
}
