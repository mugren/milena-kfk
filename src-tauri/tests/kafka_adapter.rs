use std::{cell::RefCell, collections::BTreeMap, sync::Mutex};

use milena_lib::{
    command_boundary::{
        list_kafka_topics, publish_kafka_record, start_kafka_consumer_session,
        stop_kafka_consumer_session, BoundaryEventEmitter,
    },
    contracts::{
        CommandResult, KafkaConsumerGroupCleanupAttempt, KafkaConsumerSession, KafkaTopicMetadata,
        ListKafkaTopicsRequest, MilenaBoundaryEvent, MilenaCommandError, MilenaCommandErrorCode,
        PublishKafkaRecordRequest, PublishKafkaRecordResponse, RuntimeAuthConfig,
        StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
        StopKafkaConsumerSessionResponse,
    },
    kafka_adapter::{
        build_native_client_config, cleanup_consumer_group, next_consumer_group_id, KafkaAdapter,
        NativeKafkaAdapter,
    },
};

#[test]
fn native_config_maps_plaintext_auth_into_librdkafka_entries() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth.properties.clear();
    runtime_auth
        .properties
        .insert("security.protocol".to_string(), "PLAINTEXT".to_string());
    runtime_auth
        .properties
        .insert("client.id".to_string(), "milena-dev".to_string());
    runtime_auth
        .properties
        .insert("sasl.username".to_string(), "ignored-user".to_string());
    runtime_auth
        .properties
        .insert("sasl.password".to_string(), "ignored-pass".to_string());
    runtime_auth
        .properties
        .insert("sasl.jaas.config".to_string(), "ignored".to_string());

    let native = build_native_client_config(&runtime_auth, Some("milena-poll-dev-1"))
        .expect("plaintext auth should map");

    assert_eq!(
        native.entries().get("bootstrap.servers"),
        Some(&"kafka-a:9092,kafka-b:9092".to_string())
    );
    assert_eq!(
        native.entries().get("security.protocol"),
        Some(&"PLAINTEXT".to_string())
    );
    assert_eq!(
        native.entries().get("client.id"),
        Some(&"milena-dev".to_string())
    );
    assert_eq!(
        native.entries().get("group.id"),
        Some(&"milena-poll-dev-1".to_string())
    );
    assert!(!native.entries().contains_key("sasl.mechanism"));
    assert!(!native.entries().contains_key("sasl.username"));
    assert!(!native.entries().contains_key("sasl.password"));
    assert!(!native.entries().contains_key("sasl.jaas.config"));
    assert_eq!(
        native.entries().get("log.connection.close"),
        Some(&"false".to_string())
    );
    assert_eq!(
        native.entries().get("socket.timeout.ms"),
        Some(&"3000".to_string())
    );
}

#[test]
fn native_config_maps_plain_sasl_ssl_auth_into_librdkafka_entries() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth.brokers = vec![
        " kafka-a:9092 ".to_string(),
        "".to_string(),
        "kafka-b:9092".to_string(),
    ];
    runtime_auth
        .properties
        .insert("ssl.ca.location".to_string(), "/tmp/ca.pem".to_string());

    let native = build_native_client_config(&runtime_auth, Some("milena-poll-dev-1"))
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
    assert_eq!(
        native.entries().get("ssl.ca.location"),
        Some(&"/tmp/ca.pem".to_string())
    );
    assert_eq!(
        native.entries().get("socket.connection.setup.timeout.ms"),
        Some(&"3000".to_string())
    );
    assert!(!native.entries().contains_key("sasl.jaas.config"));
}

#[test]
fn native_config_preserves_user_timeout_overrides_while_adding_low_noise_defaults() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth
        .properties
        .insert("request.timeout.ms".to_string(), "1000".to_string());
    runtime_auth
        .properties
        .insert("socket.timeout.ms".to_string(), "1000".to_string());

    let native = build_native_client_config(&runtime_auth, None)
        .expect("auth should map with user timeouts");

    assert_eq!(
        native.entries().get("request.timeout.ms"),
        Some(&"1000".to_string())
    );
    assert_eq!(
        native.entries().get("socket.timeout.ms"),
        Some(&"1000".to_string())
    );
    assert_eq!(
        native.entries().get("log.connection.close"),
        Some(&"false".to_string())
    );
    assert_eq!(
        native.entries().get("reconnect.backoff.max.ms"),
        Some(&"1000".to_string())
    );
}

#[test]
fn native_config_maps_scram_sha_512_direct_credentials_into_librdkafka_entries() {
    let mut runtime_auth = auth("SCRAM-SHA-512");
    runtime_auth.properties.remove("sasl.jaas.config");
    runtime_auth
        .properties
        .insert("sasl.username".to_string(), "scram-user".to_string());
    runtime_auth
        .properties
        .insert("sasl.password".to_string(), "scram-pass".to_string());
    runtime_auth
        .properties
        .insert("client.id".to_string(), "milena-scram".to_string());

    let native = build_native_client_config(&runtime_auth, None)
        .expect("SCRAM-SHA-512 direct credentials should map");

    assert_eq!(
        native.entries().get("security.protocol"),
        Some(&"SASL_SSL".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.mechanism"),
        Some(&"SCRAM-SHA-512".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.username"),
        Some(&"scram-user".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.password"),
        Some(&"scram-pass".to_string())
    );
    assert_eq!(
        native.entries().get("client.id"),
        Some(&"milena-scram".to_string())
    );
    assert!(!native.entries().contains_key("sasl.jaas.config"));
}

#[test]
fn native_config_resolves_relative_ssl_ca_location_before_librdkafka_uses_it() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth.properties.insert(
        "ssl.ca.location".to_string(),
        "tests/fixtures/kafka/order_created.json".to_string(),
    );

    let native =
        build_native_client_config(&runtime_auth, None).expect("relative CA locations should map");
    let ca_location = native
        .entries()
        .get("ssl.ca.location")
        .expect("CA location should be present");

    assert!(
        ca_location.ends_with("src-tauri/tests/fixtures/kafka/order_created.json"),
        "expected src-tauri-relative absolute path, got {ca_location}"
    );
    assert!(
        ca_location.starts_with('/'),
        "expected absolute CA path, got {ca_location}"
    );
}

#[test]
fn native_config_maps_plain_and_scram_mechanisms_and_optional_group_id() {
    for mechanism in ["plain", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let native = build_native_client_config(&auth(mechanism), None)
            .expect("supported SASL mechanism should map");
        assert_eq!(
            native.entries().get("sasl.mechanism"),
            Some(&mechanism.to_ascii_uppercase())
        );
        assert!(!native.entries().contains_key("group.id"));
    }
}

#[test]
fn native_config_accepts_direct_credentials_over_jaas_credentials() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth
        .properties
        .insert("sasl.username".to_string(), "direct-user".to_string());
    runtime_auth
        .properties
        .insert("sasl.password".to_string(), "direct-pass".to_string());

    let native =
        build_native_client_config(&runtime_auth, None).expect("direct credentials should map");

    assert_eq!(
        native.entries().get("sasl.username"),
        Some(&"direct-user".to_string())
    );
    assert_eq!(
        native.entries().get("sasl.password"),
        Some(&"direct-pass".to_string())
    );
    assert!(!native.entries().contains_key("sasl.jaas.config"));
}

#[test]
fn native_config_rejects_missing_or_unsupported_auth_material() {
    let mut missing_brokers = auth("SCRAM-SHA-512");
    missing_brokers.brokers = vec![" ".to_string(), "".to_string()];
    let error = build_native_client_config(&missing_brokers, None)
        .expect_err("missing brokers should fail before auth mapping");
    assert_eq!(
        error.code,
        MilenaCommandErrorCode::EnvironmentBrokersRequired
    );
    assert!(error.message.contains("broker"));

    let mut missing_protocol = auth("PLAIN");
    missing_protocol.properties.remove("security.protocol");
    let error = build_native_client_config(&missing_protocol, None)
        .expect_err("missing protocol should fail");
    assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
    assert!(error.message.contains("security.protocol"));

    let mut unsupported = auth("PLAIN");
    unsupported
        .properties
        .insert("security.protocol".to_string(), "SSL".to_string());
    let error = build_native_client_config(&unsupported, None)
        .expect_err("unsupported protocols should fail");
    assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
    assert!(error.message.contains("PLAINTEXT and SASL_SSL"));

    let mut unsupported_mechanism = auth("GSSAPI");
    let error = build_native_client_config(&unsupported_mechanism, None)
        .expect_err("unsupported mechanisms should fail");
    assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
    assert!(error.message.contains("sasl.mechanism"));

    unsupported_mechanism
        .properties
        .insert("sasl.mechanism".to_string(), "PLAIN".to_string());
    unsupported_mechanism.properties.remove("sasl.jaas.config");
    let error = build_native_client_config(&unsupported_mechanism, None)
        .expect_err("missing credentials should fail");
    assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
    assert!(error.message.contains("sasl.username/sasl.password"));
}

#[test]
fn native_config_rejects_malformed_jaas_credentials() {
    for (jaas, expected_message) in [
        (
            "module required password=\"dev-pass\";",
            "missing 'username'",
        ),
        (
            "module required username=\"dev-user\";",
            "missing 'password'",
        ),
        (
            "module required username=\"dev-user password=dev-pass;",
            "unterminated quoted 'username'",
        ),
        (
            "module required username=\"\" password=\"dev-pass\";",
            "empty 'username'",
        ),
    ] {
        let mut runtime_auth = auth("PLAIN");
        runtime_auth
            .properties
            .insert("sasl.jaas.config".to_string(), jaas.to_string());

        let error = build_native_client_config(&runtime_auth, None)
            .expect_err("malformed JAAS credentials should fail");

        assert_eq!(error.code, MilenaCommandErrorCode::KafkaAuthUnsupported);
        assert!(error.message.contains(expected_message));
    }
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
fn generated_consumer_group_ids_use_fallback_slug_for_blank_environment() {
    let group_id = next_consumer_group_id(" ! ");

    assert!(group_id.starts_with("milena-poll-environment-"));
}

#[test]
fn command_boundary_delegates_exact_requests_and_preserves_consumer_flags() {
    let adapter = RecordingKafkaAdapter::default();
    let auth = auth("PLAIN");

    let topics = list_kafka_topics(ListKafkaTopicsRequest { auth: auth.clone() }, &adapter)
        .expect("topic list should route through adapter");
    assert_eq!(
        topics.topics,
        vec![KafkaTopicMetadata {
            name: "orders.created".to_string(),
            partition_count: 12,
        }]
    );
    assert_eq!(adapter.listed_auth.borrow().as_ref(), Some(&auth));

    let publish_request = PublishKafkaRecordRequest {
        auth: auth.clone(),
        topic: " orders.created ".to_string(),
        key: Some("order-1".to_string()),
        payload: "{\"id\":1}".to_string(),
    };
    let publish = publish_kafka_record(publish_request.clone(), &adapter)
        .expect("producer should route through adapter");
    assert_eq!(publish.status, "delivered");
    assert_eq!(adapter.published.borrow().as_ref(), Some(&publish_request));

    let start_request = StartKafkaConsumerSessionRequest {
        auth: auth.clone(),
        topics: vec!["orders.created".to_string()],
        group_id: None,
        from_beginning: true,
    };
    let session = start_kafka_consumer_session(
        start_request.clone(),
        RecordingBoundaryEventEmitter::default(),
        &adapter,
    )
    .expect("consumer start should route through adapter");
    assert_eq!(session.group_id, "milena-poll-test-1");
    assert_eq!(adapter.started.borrow().as_ref(), Some(&start_request));

    let stop_request = StopKafkaConsumerSessionRequest {
        session_id: "session-to-stop".to_string(),
    };
    let stopped = stop_kafka_consumer_session(stop_request.clone(), &adapter)
        .expect("consumer stop should route through adapter");
    assert_eq!(stopped.session_id, "session-to-stop");
    assert!(stopped.cleanup.attempted);
    assert!(stopped.cleanup.succeeded);
    assert_eq!(adapter.stopped.borrow().as_ref(), Some(&stop_request));
}

#[test]
fn command_boundary_propagates_adapter_and_emitter_failures() {
    let adapter = FailingKafkaAdapter;
    let auth = auth("PLAIN");

    let list_error = list_kafka_topics(ListKafkaTopicsRequest { auth: auth.clone() }, &adapter)
        .expect_err("list errors should propagate");
    assert_eq!(
        list_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );
    assert_eq!(list_error.message, "adapter failed");

    let publish_error = publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: auth.clone(),
            topic: "orders.created".to_string(),
            key: None,
            payload: "{}".to_string(),
        },
        &adapter,
    )
    .expect_err("publish errors should propagate");
    assert_eq!(
        publish_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );

    let start_error = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth: auth.clone(),
            topics: vec!["orders.created".to_string()],
            group_id: None,
            from_beginning: false,
        },
        RecordingBoundaryEventEmitter::default(),
        &adapter,
    )
    .expect_err("start errors should propagate");
    assert_eq!(
        start_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );

    let stop_error = stop_kafka_consumer_session(
        StopKafkaConsumerSessionRequest {
            session_id: "session-1".to_string(),
        },
        &adapter,
    )
    .expect_err("stop errors should propagate");
    assert_eq!(
        stop_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );

    let emitter_error = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth,
            topics: vec!["orders.created".to_string()],
            group_id: None,
            from_beginning: false,
        },
        FailingBoundaryEventEmitter,
        &RecordingKafkaAdapter::default(),
    )
    .expect_err("emitter failures from the adapter should propagate");
    assert_eq!(
        emitter_error.code,
        MilenaCommandErrorCode::EventDeliveryFailed
    );
    assert_eq!(emitter_error.message, "frontend channel closed");
}

#[test]
fn native_adapter_preflight_rejects_blank_topics_payload_and_unknown_sessions() {
    let adapter = NativeKafkaAdapter::default();
    let auth = auth("PLAIN");

    let blank_topic_error = publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: auth.clone(),
            topic: " ".to_string(),
            key: None,
            payload: "{}".to_string(),
        },
        &adapter,
    )
    .expect_err("blank publish topic should fail before broker access");
    assert_eq!(
        blank_topic_error.code,
        MilenaCommandErrorCode::TopicRequired
    );

    let blank_payload_error = publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: auth.clone(),
            topic: "orders.created".to_string(),
            key: None,
            payload: "".to_string(),
        },
        &adapter,
    )
    .expect_err("blank payload should fail before broker access");
    assert_eq!(
        blank_payload_error.code,
        MilenaCommandErrorCode::KafkaPayloadRequired
    );

    let empty_topics_error = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth: auth.clone(),
            topics: vec![" ".to_string()],
            group_id: None,
            from_beginning: false,
        },
        RecordingBoundaryEventEmitter::default(),
        &adapter,
    )
    .expect_err("empty consumer topic list should fail before broker access");
    assert_eq!(
        empty_topics_error.code,
        MilenaCommandErrorCode::TopicRequired
    );

    let unknown_stop_error = stop_kafka_consumer_session(
        StopKafkaConsumerSessionRequest {
            session_id: "missing-session".to_string(),
        },
        &adapter,
    )
    .expect_err("unknown session should fail before broker access");
    assert_eq!(
        unknown_stop_error.code,
        MilenaCommandErrorCode::KafkaSessionNotFound
    );
    assert!(unknown_stop_error.message.contains("missing-session"));
}

#[test]
fn cleanup_failure_response_is_reported_without_requiring_a_broker() {
    let mut runtime_auth = auth("PLAIN");
    runtime_auth.brokers = vec![" ".to_string()];

    let cleanup = cleanup_consumer_group(&runtime_auth, "group-1");

    assert_eq!(cleanup.group_id, "group-1");
    assert!(cleanup.attempted);
    assert!(!cleanup.succeeded);
    assert_eq!(
        cleanup.error.as_deref(),
        Some("at least one broker is required")
    );
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
    listed_auth: RefCell<Option<RuntimeAuthConfig>>,
    published: RefCell<Option<PublishKafkaRecordRequest>>,
    started: RefCell<Option<StartKafkaConsumerSessionRequest>>,
    stopped: RefCell<Option<StopKafkaConsumerSessionRequest>>,
}

impl KafkaAdapter for RecordingKafkaAdapter {
    fn list_topics(&self, auth: &RuntimeAuthConfig) -> CommandResult<Vec<KafkaTopicMetadata>> {
        self.listed_auth.replace(Some(auth.clone()));
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
        self.started.replace(Some(request.clone()));
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
        self.stopped.replace(Some(request.clone()));
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

struct FailingBoundaryEventEmitter;

impl BoundaryEventEmitter for FailingBoundaryEventEmitter {
    fn emit(&self, _event: MilenaBoundaryEvent) -> CommandResult<()> {
        Err(MilenaCommandError::event_delivery_failed(
            "frontend channel closed",
        ))
    }
}

struct FailingKafkaAdapter;

impl KafkaAdapter for FailingKafkaAdapter {
    fn list_topics(&self, _auth: &RuntimeAuthConfig) -> CommandResult<Vec<KafkaTopicMetadata>> {
        Err(MilenaCommandError::kafka_operation_failed("adapter failed"))
    }

    fn publish_record(
        &self,
        _request: &PublishKafkaRecordRequest,
    ) -> CommandResult<PublishKafkaRecordResponse> {
        Err(MilenaCommandError::kafka_operation_failed("adapter failed"))
    }

    fn start_consumer_session<E>(
        &self,
        _request: StartKafkaConsumerSessionRequest,
        _event_emitter: E,
    ) -> CommandResult<KafkaConsumerSession>
    where
        E: BoundaryEventEmitter + Send + Sync + 'static,
    {
        Err(MilenaCommandError::kafka_operation_failed("adapter failed"))
    }

    fn stop_consumer_session(
        &self,
        _request: StopKafkaConsumerSessionRequest,
    ) -> CommandResult<StopKafkaConsumerSessionResponse> {
        Err(MilenaCommandError::kafka_operation_failed("adapter failed"))
    }
}
