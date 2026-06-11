use std::{cell::RefCell, collections::BTreeMap, sync::Mutex};

use milena_lib::{
    command_boundary::{
        list_kafka_topics, publish_kafka_record, start_kafka_consumer_session,
        stop_kafka_consumer_session, BoundaryEventEmitter,
    },
    contracts::{
        CommandResult, KafkaConsumerGroupCleanupAttempt, KafkaConsumerSession, KafkaTopicMetadata,
        ListKafkaTopicsRequest, MilenaBoundaryEvent, MilenaCommandErrorCode,
        PublishKafkaRecordRequest, PublishKafkaRecordResponse, RuntimeAuthConfig,
        StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
        StopKafkaConsumerSessionResponse,
    },
    kafka_adapter::{build_native_client_config, next_consumer_group_id, KafkaAdapter},
};

#[test]
fn native_config_maps_plain_sasl_ssl_auth_into_librdkafka_entries() {
    let native = build_native_client_config(&auth("PLAIN"), Some("milena-poll-dev-1"))
        .expect("plain auth should map");

    assert_eq!(
        native.entries().get("bootstrap.servers"),
        Some(&"kafka-a:9092,kafka-b:9092".to_string())
    );
    assert_eq!(
        native.entries().get("security.protocol"),
        Some(&"SASL_SSL".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.mechanism"),
        Some(&"PLAIN".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.username"),
        Some(&"dev-user".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.password"),
        Some(&"dev-pass".to_string())
    );
    assert_eq!(
        native.entries().get("group.id"),
        Some(&"milena-poll-dev-1".to_string())
    );
    assert!(!native.entries().contains_key("sasl.jaas.config"));
}

#[test]
fn native_config_maps_scram_auth_and_rejects_unsupported_auth() {
    let scram = build_native_client_config(&auth("SCRAM-SHA-512"), None)
        .expect("scram auth should map");
    assert_eq!(
        scram.entries().get("sasl.mechanism"),
        Some(&"SCRAM-SHA-512".to_string())
    );

    let mut unsupported = auth("PLAIN");
    unsupported
        .properties
        .insert("security.protocol".to_string(), "PLAINTEXT".to_string());
    let error = build_native_client_config(&unsupported, None)
        .expect_err("unsupported protocols should fail");
    assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
    assert!(error.message.contains("SASL_SSL"));
}

#[test]
fn generated_consumer_group_ids_use_recognizable_prefix_and_are_unique() {
    let first = next_consumer_group_id("Dev Cluster");
    let second = next_consumer_group_id("Dev Cluster");

    assert!(first.starts_with("milena-poll-dev-cluster-"));
    assert!(second.starts_with("milena-poll-dev-cluster-"));
    assert_ne!(first, second);
}

#[test]
fn command_boundary_routes_topic_publish_consumer_and_cleanup_contracts() {
    let adapter = RecordingKafkaAdapter::default();
    let auth = auth("PLAIN");

    let topics = list_kafka_topics(
        ListKafkaTopicsRequest { auth: auth.clone() },
        &adapter,
    )
    .expect("topic list should route through adapter");
    assert_eq!(
        topics.topics,
        vec![KafkaTopicMetadata {
            name: "orders.created".to_string(),
            partition_count: 12,
        }]
    );

    let publish = publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: auth.clone(),
            topic: "orders.created".to_string(),
            key: Some("order-1".to_string()),
            payload: "{\"id\":1}".to_string(),
        },
        &adapter,
    )
    .expect("producer should route through adapter");
    assert_eq!(publish.status, "delivered");
    assert_eq!(adapter.published.borrow().as_ref().unwrap().topic, "orders.created");

    let session = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth,
            topics: vec!["orders.created".to_string()],
            from_beginning: false,
        },
        RecordingBoundaryEventEmitter::default(),
        &adapter,
    )
    .expect("consumer start should route through adapter");
    assert_eq!(session.group_id, "milena-poll-test-1");

    let stopped = stop_kafka_consumer_session(
        StopKafkaConsumerSessionRequest {
            session_id: session.session_id,
        },
        &adapter,
    )
    .expect("consumer stop should route through adapter");
    assert!(stopped.cleanup.attempted);
    assert!(stopped.cleanup.succeeded);
}

fn auth(mechanism: &str) -> RuntimeAuthConfig {
    let mut properties = BTreeMap::new();
    properties.insert("security.protocol".to_string(), "SASL_SSL".to_string());
    properties.insert("sasl.mechanism".to_string(), mechanism.to_string());
    properties.insert(
        "sasl.jaas.config".to_string(),
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"dev-user\" password=\"dev-pass\";".to_string(),
    );

    RuntimeAuthConfig {
        environment: "dev".to_string(),
        brokers: vec!["kafka-a:9092".to_string(), "kafka-b:9092".to_string()],
        properties,
    }
}

#[derive(Default)]
struct RecordingBoundaryEventEmitter {
    events: Mutex<Vec<MilenaBoundaryEvent>>,
}

impl BoundaryEventEmitter for RecordingBoundaryEventEmitter {
    fn emit(&self, event: MilenaBoundaryEvent) -> CommandResult<()> {
        self.events
            .lock()
            .expect("event lock should not be poisoned")
            .push(event);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingKafkaAdapter {
    published: RefCell<Option<PublishKafkaRecordRequest>>,
}

impl KafkaAdapter for RecordingKafkaAdapter {
    fn list_topics(&self, _auth: &RuntimeAuthConfig) -> CommandResult<Vec<KafkaTopicMetadata>> {
        Ok(vec![KafkaTopicMetadata {
            name: "orders.created".to_string(),
            partition_count: 12,
        }])
    }

    fn publish_record(
        &self,
        request: &PublishKafkaRecordRequest,
    ) -> CommandResult<PublishKafkaRecordResponse> {
        self.published.replace(Some(request.clone()));
        Ok(PublishKafkaRecordResponse {
            topic: request.topic.clone(),
            partition: 0,
            offset: 42,
            status: "delivered".to_string(),
        })
    }

    fn start_consumer_session<E>(
        &self,
        request: StartKafkaConsumerSessionRequest,
        event_emitter: E,
    ) -> CommandResult<KafkaConsumerSession>
    where
        E: BoundaryEventEmitter + Send + Sync + 'static,
    {
        event_emitter.emit(MilenaBoundaryEvent::KafkaConsumerStarted {
            session_id: "session-1".to_string(),
            group_id: "milena-poll-test-1".to_string(),
            topics: request.topics.clone(),
        })?;
        Ok(KafkaConsumerSession {
            session_id: "session-1".to_string(),
            group_id: "milena-poll-test-1".to_string(),
            topics: request.topics,
            status: "started".to_string(),
        })
    }

    fn stop_consumer_session(
        &self,
        request: StopKafkaConsumerSessionRequest,
    ) -> CommandResult<StopKafkaConsumerSessionResponse> {
        Ok(StopKafkaConsumerSessionResponse {
            session_id: request.session_id,
            group_id: "milena-poll-test-1".to_string(),
            status: "stopped".to_string(),
            cleanup: KafkaConsumerGroupCleanupAttempt {
                group_id: "milena-poll-test-1".to_string(),
                attempted: true,
                succeeded: true,
                error: None,
            },
        })
    }
}
