use std::cell::RefCell;

use milena_lib::command_boundary::{app_state, preview_topic_session, BoundaryEventEmitter};
use milena_lib::contracts::{
    EnvironmentAuthMode, KafkaConsumerGroupCleanupAttempt, KafkaConsumerSession, KafkaRecordEvent,
    KafkaTopicList, KafkaTopicMetadata, ListKafkaTopicsRequest, MilenaBoundaryEvent,
    MilenaCapability, MilenaCommandError, MilenaCommandErrorCode, PublishKafkaRecordRequest,
    PublishKafkaRecordResponse, RuntimeAuthConfig, SavedEnvironment,
    StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
    StopKafkaConsumerSessionResponse, TopicSessionMode, TopicSessionPreview,
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

struct FailingBoundaryEventEmitter;

impl BoundaryEventEmitter for FailingBoundaryEventEmitter {
    fn emit(&self, _event: MilenaBoundaryEvent) -> milena_lib::contracts::CommandResult<()> {
        Err(MilenaCommandError::event_delivery_failed(
            "frontend channel closed",
        ))
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
fn topic_session_command_accepts_poll_request_and_emits_ordered_events() {
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
fn topic_session_command_trims_topic_and_uses_publish_mode_in_preview_id() {
    let events = RecordingBoundaryEventEmitter::default();

    let preview = preview_topic_session(
        TopicSessionPreviewRequest {
            topic: " payments.authorized ".to_string(),
            mode: TopicSessionMode::Publish,
        },
        &events,
    )
    .expect("publish preview should be built");

    assert_eq!(preview.session_id, "preview-publish-payments-authorized");
    assert_eq!(preview.topic, "payments.authorized");
    assert_eq!(preview.mode, TopicSessionMode::Publish);
    assert_eq!(
        events.events(),
        vec![
            MilenaBoundaryEvent::BoundaryOpened {
                session_id: "preview-publish-payments-authorized".to_string(),
                topic: "payments.authorized".to_string(),
                mode: TopicSessionMode::Publish,
            },
            MilenaBoundaryEvent::BoundaryReady {
                session_id: "preview-publish-payments-authorized".to_string(),
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
fn topic_session_command_propagates_event_delivery_failures() {
    let error = preview_topic_session(
        TopicSessionPreviewRequest {
            topic: "orders.created".to_string(),
            mode: TopicSessionMode::Poll,
        },
        &FailingBoundaryEventEmitter,
    )
    .expect_err("emitter failures should fail the command");

    assert_eq!(error.code, MilenaCommandErrorCode::EventDeliveryFailed);
    assert_eq!(error.message, "frontend channel closed");
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

#[test]
fn frontend_facing_json_contracts_round_trip_with_expected_casing() {
    let preview_request: TopicSessionPreviewRequest = serde_json::from_value(json!({
        "topic": "orders.created",
        "mode": "publish",
    }))
    .expect("preview request should deserialize");
    assert_eq!(preview_request.mode, TopicSessionMode::Publish);

    let preview = TopicSessionPreview {
        session_id: "preview-publish-orders-created".to_string(),
        topic: "orders.created".to_string(),
        mode: TopicSessionMode::Publish,
        status: "ready".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&preview).expect("preview should serialize"),
        json!({
            "sessionId": "preview-publish-orders-created",
            "topic": "orders.created",
            "mode": "publish",
            "status": "ready",
        })
    );
    assert_eq!(
        serde_json::from_value::<TopicSessionPreview>(json!({
            "sessionId": "preview-publish-orders-created",
            "topic": "orders.created",
            "mode": "publish",
            "status": "ready",
        }))
        .expect("preview should deserialize"),
        preview
    );

    let mut properties = std::collections::BTreeMap::new();
    properties.insert("security.protocol".to_string(), "SASL_SSL".to_string());
    let auth = RuntimeAuthConfig {
        environment: "dev".to_string(),
        brokers: vec!["localhost:9092".to_string()],
        properties,
    };
    assert_eq!(
        serde_json::to_value(&auth).expect("runtime auth should serialize"),
        json!({
            "environment": "dev",
            "brokers": ["localhost:9092"],
            "properties": {
                "security.protocol": "SASL_SSL",
            },
        })
    );
    assert_eq!(
        serde_json::from_value::<RuntimeAuthConfig>(serde_json::to_value(&auth).unwrap())
            .expect("runtime auth should deserialize"),
        auth
    );

    let list_request = ListKafkaTopicsRequest { auth: auth.clone() };
    assert_eq!(
        serde_json::from_value::<ListKafkaTopicsRequest>(
            serde_json::to_value(&list_request).unwrap()
        )
        .expect("list request should deserialize"),
        list_request
    );

    let publish_request = PublishKafkaRecordRequest {
        auth: auth.clone(),
        topic: "orders.created".to_string(),
        key: Some("order-1".to_string()),
        payload: "{\"id\":1}".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&publish_request).expect("publish request should serialize"),
        json!({
            "auth": {
                "environment": "dev",
                "brokers": ["localhost:9092"],
                "properties": {
                    "security.protocol": "SASL_SSL",
                },
            },
            "topic": "orders.created",
            "key": "order-1",
            "payload": "{\"id\":1}",
        })
    );
    assert_eq!(
        serde_json::from_value::<PublishKafkaRecordRequest>(
            serde_json::to_value(&publish_request).unwrap()
        )
        .expect("publish request should deserialize"),
        publish_request
    );

    let start_request = StartKafkaConsumerSessionRequest {
        auth: auth.clone(),
        topics: vec!["orders.created".to_string()],
        group_id: Some("milena-poll-tab-1".to_string()),
        from_beginning: true,
    };
    assert_eq!(
        serde_json::to_value(&start_request).expect("start request should serialize"),
        json!({
            "auth": {
                "environment": "dev",
                "brokers": ["localhost:9092"],
                "properties": {
                    "security.protocol": "SASL_SSL",
                },
            },
            "topics": ["orders.created"],
            "groupId": "milena-poll-tab-1",
            "fromBeginning": true,
        })
    );
    assert_eq!(
        serde_json::from_value::<StartKafkaConsumerSessionRequest>(
            serde_json::to_value(&start_request).unwrap()
        )
        .expect("start request should deserialize"),
        start_request
    );

    let stop_request = StopKafkaConsumerSessionRequest {
        session_id: "session-1".to_string(),
    };
    assert_eq!(
        serde_json::from_value::<StopKafkaConsumerSessionRequest>(
            serde_json::to_value(&stop_request).unwrap()
        )
        .expect("stop request should deserialize"),
        stop_request
    );

    let topic_list = KafkaTopicList {
        topics: vec![KafkaTopicMetadata {
            name: "orders.created".to_string(),
            partition_count: 3,
        }],
    };
    assert_eq!(
        serde_json::to_value(&topic_list).expect("topic list should serialize"),
        json!({
            "topics": [{
                "name": "orders.created",
                "partitionCount": 3,
            }],
        })
    );

    let publish_response = PublishKafkaRecordResponse {
        topic: "orders.created".to_string(),
        partition: 1,
        offset: 42,
        status: "delivered".to_string(),
    };
    assert_eq!(
        serde_json::from_value::<PublishKafkaRecordResponse>(
            serde_json::to_value(&publish_response).unwrap()
        )
        .expect("publish response should deserialize"),
        publish_response
    );

    let session = KafkaConsumerSession {
        session_id: "session-1".to_string(),
        group_id: "group-1".to_string(),
        topics: vec!["orders.created".to_string()],
        status: "started".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&session).expect("session should serialize"),
        json!({
            "sessionId": "session-1",
            "groupId": "group-1",
            "topics": ["orders.created"],
            "status": "started",
        })
    );

    let stopped = StopKafkaConsumerSessionResponse {
        session_id: "session-1".to_string(),
        group_id: "group-1".to_string(),
        status: "stopped".to_string(),
        cleanup: KafkaConsumerGroupCleanupAttempt {
            group_id: "group-1".to_string(),
            attempted: true,
            succeeded: false,
            error: Some("not empty".to_string()),
        },
    };
    assert_eq!(
        serde_json::from_value::<StopKafkaConsumerSessionResponse>(
            serde_json::to_value(&stopped).unwrap()
        )
        .expect("stop response should deserialize"),
        stopped
    );

    let error = MilenaCommandError::kafka_session_not_found("session-404");
    assert_eq!(
        serde_json::to_value(&error).expect("error should serialize"),
        json!({
            "code": "kafka-session-not-found",
            "message": "Kafka consumer session 'session-404' was not found",
        })
    );
    assert_eq!(
        serde_json::from_value::<MilenaCommandError>(serde_json::to_value(&error).unwrap())
            .expect("error should deserialize"),
        error
    );
}

#[test]
fn kafka_event_contracts_round_trip_as_frontend_discriminated_unions() {
    let record = MilenaBoundaryEvent::KafkaRecord {
        record: KafkaRecordEvent {
            session_id: "session-1".to_string(),
            topic: "orders.created".to_string(),
            partition: 0,
            offset: 12,
            key: Some("order-1".to_string()),
            payload: Some("{\"id\":1}".to_string()),
        },
    };

    assert_eq!(
        serde_json::to_value(&record).expect("record event should serialize"),
        json!({
            "event": "kafkaRecord",
            "data": {
                "record": {
                    "sessionId": "session-1",
                    "topic": "orders.created",
                    "partition": 0,
                    "offset": 12,
                    "key": "order-1",
                    "payload": "{\"id\":1}",
                },
            },
        })
    );
    assert_eq!(
        serde_json::from_value::<MilenaBoundaryEvent>(serde_json::to_value(&record).unwrap())
            .expect("record event should deserialize"),
        record
    );

    let error = MilenaBoundaryEvent::KafkaConsumerError {
        session_id: "session-1".to_string(),
        message: "poll failed".to_string(),
    };
    assert_eq!(
        serde_json::from_value::<MilenaBoundaryEvent>(serde_json::to_value(&error).unwrap())
            .expect("error event should deserialize"),
        error
    );
}

#[test]
fn saved_environment_contract_includes_structured_metadata_without_secret_references() {
    let environment = SavedEnvironment {
        schema_version: 1,
        name: "dev".to_string(),
        brokers: vec!["localhost:9092".to_string()],
        auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
        username: Some("alice".to_string()),
        advanced_properties: "client.id=milena-dev".to_string(),
    };

    let value = serde_json::to_value(&environment).expect("environment should serialize");
    assert_eq!(
        value,
        json!({
            "schemaVersion": 1,
            "name": "dev",
            "brokers": ["localhost:9092"],
            "authMode": "saslSslScramSha512",
            "username": "alice",
            "advancedProperties": "client.id=milena-dev",
        })
    );
    assert!(!value.to_string().contains("password"));
    assert!(!value.to_string().contains("secret"));
    assert_eq!(
        serde_json::from_value::<SavedEnvironment>(value).expect("environment should deserialize"),
        environment
    );
}
