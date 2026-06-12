use tauri::ipc::Channel;

use crate::contracts::{
    AppState, CommandResult, KafkaConsumerSession, KafkaTopicList, ListKafkaTopicsRequest,
    MilenaBoundaryEvent, MilenaCommandError, PublishKafkaRecordRequest, PublishKafkaRecordResponse,
    StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
    StopKafkaConsumerSessionResponse, TopicSessionMode, TopicSessionPreview,
    TopicSessionPreviewRequest, CAPABILITIES,
};
use crate::kafka_adapter::KafkaAdapter;

pub trait BoundaryEventEmitter {
    fn emit(&self, event: MilenaBoundaryEvent) -> CommandResult<()>;
}

impl BoundaryEventEmitter for Channel<MilenaBoundaryEvent> {
    fn emit(&self, event: MilenaBoundaryEvent) -> CommandResult<()> {
        self.send(event)
            .map_err(MilenaCommandError::event_delivery_failed)
    }
}

pub fn app_state() -> AppState {
    AppState {
        app_name: "Milena".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        platform: "macos-dev".to_string(),
        capabilities: CAPABILITIES.to_vec(),
    }
}

pub fn preview_topic_session(
    request: TopicSessionPreviewRequest,
    event_emitter: &impl BoundaryEventEmitter,
) -> CommandResult<TopicSessionPreview> {
    let preview = build_topic_session_preview(request)?;

    event_emitter.emit(MilenaBoundaryEvent::BoundaryOpened {
        session_id: preview.session_id.clone(),
        topic: preview.topic.clone(),
        mode: preview.mode.clone(),
    })?;

    event_emitter.emit(MilenaBoundaryEvent::BoundaryReady {
        session_id: preview.session_id.clone(),
        capabilities: CAPABILITIES.to_vec(),
    })?;

    Ok(preview)
}

pub fn list_kafka_topics(
    request: ListKafkaTopicsRequest,
    adapter: &impl KafkaAdapter,
) -> CommandResult<KafkaTopicList> {
    Ok(KafkaTopicList {
        topics: adapter.list_topics(&request.auth)?,
    })
}

pub fn publish_kafka_record(
    request: PublishKafkaRecordRequest,
    adapter: &impl KafkaAdapter,
) -> CommandResult<PublishKafkaRecordResponse> {
    adapter.publish_record(&request)
}

pub fn start_kafka_consumer_session<E>(
    request: StartKafkaConsumerSessionRequest,
    event_emitter: E,
    adapter: &impl KafkaAdapter,
) -> CommandResult<KafkaConsumerSession>
where
    E: BoundaryEventEmitter + Send + Sync + 'static,
{
    adapter.start_consumer_session(request, event_emitter)
}

pub fn stop_kafka_consumer_session(
    request: StopKafkaConsumerSessionRequest,
    adapter: &impl KafkaAdapter,
) -> CommandResult<StopKafkaConsumerSessionResponse> {
    adapter.stop_consumer_session(request)
}

fn build_topic_session_preview(
    request: TopicSessionPreviewRequest,
) -> CommandResult<TopicSessionPreview> {
    let topic = request.topic.trim();

    if topic.is_empty() {
        return Err(MilenaCommandError::topic_required());
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
