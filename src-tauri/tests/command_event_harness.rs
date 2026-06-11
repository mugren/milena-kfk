use std::cell::RefCell;

use milena_lib::command_boundary::{app_state, preview_topic_session, BoundaryEventEmitter};
use milena_lib::contracts::{
    MilenaBoundaryEvent, MilenaCapability, MilenaCommandErrorCode, TopicSessionMode,
    TopicSessionPreviewRequest,
};
use serde_json::json;

#[derive(Default)]
struct RecordingBoundaryEventEmitter {
    events: RefCell<Vec<MilenaBoundaryEvent>>,
}

impl RecordingBoundaryEventEmitter {
    fn events(&self) -> Vec<MilenaBoundaryEvent> {
        self.events.borrow().clone()
    }
}

impl BoundaryEventEmitter for RecordingBoundaryEventEmitter {
    fn emit(&self, event: MilenaBoundaryEvent) -> milena_lib::contracts::CommandResult<()> {
        self.events.borrow_mut().push(event);
        Ok(())
    }
}

#[test]
fn app_state_command_response_contract_serializes_for_the_frontend() {
    let state = app_state();

    assert_eq!(state.app_name, "Milena");
    assert_eq!(state.platform, "macos-dev");
    assert_eq!(
        state.capabilities,
        vec![
            MilenaCapability::CommandBoundary,
            MilenaCapability::EventChannel,
            MilenaCapability::MacosDevBuild,
        ]
    );
    assert_eq!(
        serde_json::to_value(state).expect("state should serialize"),
        json!({
            "appName": "Milena",
            "version": env!("CARGO_PKG_VERSION"),
            "platform": "macos-dev",
            "capabilities": ["command-boundary", "event-channel", "macos-dev-build"],
        })
    );
}

#[test]
fn topic_session_command_accepts_typed_request_and_emits_ordered_events() {
    let request: TopicSessionPreviewRequest = serde_json::from_value(json!({
        "topic": "orders.created",
        "mode": "poll",
    }))
    .expect("request should deserialize from frontend payload");
    let events = RecordingBoundaryEventEmitter::default();

    let preview = preview_topic_session(request, &events).expect("preview should be built");

    assert_eq!(preview.session_id, "preview-poll-orders-created");
    assert_eq!(preview.topic, "orders.created");
    assert_eq!(preview.mode, TopicSessionMode::Poll);
    assert_eq!(preview.status, "ready");
    assert_eq!(
        events.events(),
        vec![
            MilenaBoundaryEvent::BoundaryOpened {
                session_id: "preview-poll-orders-created".to_string(),
                topic: "orders.created".to_string(),
                mode: TopicSessionMode::Poll,
            },
            MilenaBoundaryEvent::BoundaryReady {
                session_id: "preview-poll-orders-created".to_string(),
                capabilities: vec![
                    MilenaCapability::CommandBoundary,
                    MilenaCapability::EventChannel,
                    MilenaCapability::MacosDevBuild,
                ],
            },
        ]
    );
}

#[test]
fn topic_session_command_rejects_blank_topic_without_emitting_events() {
    let events = RecordingBoundaryEventEmitter::default();

    let error = preview_topic_session(
        TopicSessionPreviewRequest {
            topic: " ".to_string(),
            mode: TopicSessionMode::Publish,
        },
        &events,
    )
    .expect_err("blank topics should fail");

    assert_eq!(error.code, MilenaCommandErrorCode::TopicRequired);
    assert_eq!(error.message, "topic is required");
    assert_eq!(events.events(), Vec::<MilenaBoundaryEvent>::new());
}

#[test]
fn backend_event_payload_contract_serializes_to_the_ui_discriminated_union() {
    let event = MilenaBoundaryEvent::BoundaryOpened {
        session_id: "preview-publish-payments-authorized".to_string(),
        topic: "payments.authorized".to_string(),
        mode: TopicSessionMode::Publish,
    };

    assert_eq!(
        serde_json::to_value(event).expect("event should serialize"),
        json!({
            "event": "boundaryOpened",
            "data": {
                "sessionId": "preview-publish-payments-authorized",
                "topic": "payments.authorized",
                "mode": "publish",
            },
        })
    );
}
