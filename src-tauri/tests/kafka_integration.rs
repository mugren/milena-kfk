use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use milena_lib::{
    command_boundary::{
        list_kafka_topics, publish_kafka_record, start_kafka_consumer_session,
        stop_kafka_consumer_session, BoundaryEventEmitter,
    },
    contracts::{
        CommandResult, KafkaRecordEvent, ListKafkaTopicsRequest, MilenaBoundaryEvent,
        MilenaCommandError, MilenaCommandErrorCode, PublishKafkaRecordRequest, RuntimeAuthConfig,
        StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
    },
    kafka_adapter::NativeKafkaAdapter,
};
use serde_json::json;

const INTEGRATION_ENV: &str = "MILENA_KAFKA_INTEGRATION";
const DEFAULT_BROKERS: &str = "localhost:19092";
const DEFAULT_USERNAME: &str = "milena_plain";
const DEFAULT_PASSWORD: &str = "milena-plain-secret";
const DEFAULT_MECHANISM: &str = "PLAIN";

const RECORDS_TOPIC: &str = "milena.issue14.records";
const POLLING_TOPIC: &str = "milena.issue14.polling";
const PUBLISH_TOPIC: &str = "milena.issue14.publish";
const KEYED_TOPIC: &str = "milena.issue14.keyed";
const ERRORS_TOPIC: &str = "milena.issue14.errors";
const CLEANUP_TOPIC: &str = "milena.issue14.cleanup";
const INTEROP_TOPIC: &str = "milena.issue14.interop";

const UNKEYED_FIXTURE: &str = include_str!("fixtures/kafka/order_created.json");
const KEYED_FIXTURE: &str = include_str!("fixtures/kafka/order_keyed.json");

#[test]
fn broker_lists_deterministic_fixture_topics_and_usable_metadata() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();

    let topics = list_kafka_topics(
        ListKafkaTopicsRequest {
            auth: settings.auth.clone(),
        },
        &adapter,
    )
    .expect("broker should list topics with the configured auth");

    assert!(
        topics
            .topics
            .iter()
            .all(|topic| !topic.name.starts_with("__")),
        "internal topics should be filtered from Milena metadata: {:?}",
        topics.topics
    );

    for expected in settings.fixture_topics() {
        let metadata = topics
            .topics
            .iter()
            .find(|topic| topic.name == expected)
            .unwrap_or_else(|| {
                panic!(
                    "expected deterministic topic '{expected}' in broker metadata, got {:?}",
                    topics.topics
                )
            });
        assert!(
            metadata.partition_count > 0,
            "topic '{}' should report usable partition metadata, got {:?}",
            expected,
            metadata
        );
    }
}

#[test]
fn publish_ack_and_consumed_records_preserve_unkeyed_and_keyed_json() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();
    let events = RecordingBoundaryEventEmitter::default();
    let test_id = unique_id();

    assert_valid_json(UNKEYED_FIXTURE);
    assert_valid_json(KEYED_FIXTURE);

    let unkeyed_payload = fixture_payload(UNKEYED_FIXTURE, &test_id, "unkeyed-publish");
    let unkeyed_session = start_session(
        &adapter,
        &settings,
        &events,
        settings.publish_topic.clone(),
        false,
    );
    let (unkeyed_ack, unkeyed_record) = publish_until_observed_by_payload(
        &adapter,
        &settings,
        &events,
        &unkeyed_session.session_id,
        &settings.publish_topic,
        &unkeyed_payload,
    )
    .unwrap_or_else(|| {
        panic!(
            "consumer did not receive unkeyed payload '{}'; events: {:?}",
            unkeyed_payload,
            events.snapshot()
        )
    });
    assert_delivered_ack(&unkeyed_ack, &settings.publish_topic);
    assert_eq!(unkeyed_record.topic, settings.publish_topic);
    assert_eq!(unkeyed_record.key, None);
    stop_session(&adapter, &events, &unkeyed_session.session_id);

    let keyed_key = format!("issue-14-keyed-{test_id}");
    let keyed_payload = fixture_payload(KEYED_FIXTURE, &test_id, "keyed-publish");
    let keyed_session = start_session(
        &adapter,
        &settings,
        &events,
        settings.keyed_topic.clone(),
        false,
    );
    let (keyed_ack, keyed_record) = publish_until_observed_by_key(
        &adapter,
        &settings,
        &events,
        &keyed_session.session_id,
        &settings.keyed_topic,
        &keyed_key,
        &keyed_payload,
    )
    .unwrap_or_else(|| {
        panic!(
            "consumer did not receive keyed record '{}'; events: {:?}",
            keyed_key,
            events.snapshot()
        )
    });
    assert_delivered_ack(&keyed_ack, &settings.keyed_topic);
    assert_eq!(keyed_record.topic, settings.keyed_topic);
    assert_eq!(keyed_record.key.as_deref(), Some(keyed_key.as_str()));
    assert_eq!(
        keyed_record.payload.as_deref(),
        Some(keyed_payload.as_str())
    );
    stop_session(&adapter, &events, &keyed_session.session_id);
}

#[test]
fn latest_consumer_session_ignores_pre_session_records_and_emits_post_session_records() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();
    let events = RecordingBoundaryEventEmitter::default();
    let test_id = unique_id();

    let before_key = format!("issue-14-before-{test_id}");
    let before_payload = consumer_payload(&test_id, "before-start");
    publish_to_topic(
        &adapter,
        &settings,
        settings.polling_topic.clone(),
        Some(before_key.clone()),
        &before_payload,
    )
    .expect("pre-session publish should succeed");

    let session = start_session(
        &adapter,
        &settings,
        &events,
        settings.polling_topic.clone(),
        false,
    );

    let after_key = format!("issue-14-after-{test_id}");
    let after_payload = consumer_payload(&test_id, "after-start");
    let record = publish_until_observed_by_key(
        &adapter,
        &settings,
        &events,
        &session.session_id,
        &settings.polling_topic,
        &after_key,
        &after_payload,
    );

    stop_session(&adapter, &events, &session.session_id);

    let (_ack, record) = record.unwrap_or_else(|| {
        panic!(
            "consumer did not receive post-start record with key '{}'; events: {:?}",
            after_key,
            events.snapshot()
        )
    });
    assert_eq!(record.topic, settings.polling_topic);
    assert_eq!(record.key.as_deref(), Some(after_key.as_str()));
    assert_eq!(record.payload.as_deref(), Some(after_payload.as_str()));

    assert!(
        !events.saw_record_with_key(&session.session_id, &before_key),
        "latest-only consumer unexpectedly received the pre-session record; events: {:?}",
        events.snapshot()
    );
}

#[test]
fn stop_reports_lifecycle_event_and_cleanup_result() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();
    let events = RecordingBoundaryEventEmitter::default();
    let test_id = unique_id();
    let key = format!("issue-14-cleanup-{test_id}");
    let payload = consumer_payload(&test_id, "cleanup");

    let session = start_session(
        &adapter,
        &settings,
        &events,
        settings.cleanup_topic.clone(),
        false,
    );
    let (_ack, record) = publish_until_observed_by_key(
        &adapter,
        &settings,
        &events,
        &session.session_id,
        &settings.cleanup_topic,
        &key,
        &payload,
    )
    .unwrap_or_else(|| {
        panic!(
            "cleanup session did not receive test record '{}'; events: {:?}",
            key,
            events.snapshot()
        )
    });
    assert_eq!(record.payload.as_deref(), Some(payload.as_str()));

    let stopped = stop_kafka_consumer_session(
        StopKafkaConsumerSessionRequest {
            session_id: session.session_id.clone(),
        },
        &adapter,
    )
    .expect("consumer session should stop");
    assert_eq!(stopped.status, "stopped");
    assert_eq!(stopped.group_id, session.group_id);
    assert_eq!(stopped.cleanup.group_id, session.group_id);
    assert!(stopped.cleanup.attempted);
    if stopped.cleanup.succeeded {
        assert_eq!(stopped.cleanup.error, None);
    } else {
        assert!(
            stopped.cleanup.error.is_some(),
            "cleanup failures should include the broker error"
        );
    }
    assert!(
        events.wait_for_stopped(&session.session_id, Duration::from_secs(5)),
        "consumer stop event was not observed; events: {:?}",
        events.snapshot()
    );
}

#[test]
fn cli_produced_record_is_visible_to_milena_consumer() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    if env::var("MILENA_KAFKA_CLI_INTEROP_SEEDED").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping CLI interop assertion; run through scripts/kafka-test or set MILENA_KAFKA_CLI_INTEROP_SEEDED=1"
        );
        return;
    }

    let adapter = NativeKafkaAdapter::default();
    let events = RecordingBoundaryEventEmitter::default();
    let interop_key = env_or("MILENA_KAFKA_CLI_INTEROP_KEY", "issue14-cli-seed");
    let interop_payload = env_or(
        "MILENA_KAFKA_CLI_INTEROP_PAYLOAD",
        r#"{"fixture":"issue14-cli-interop","source":"kafka-cli"}"#,
    );

    let session = start_session(
        &adapter,
        &settings,
        &events,
        settings.interop_topic.clone(),
        true,
    );
    let record = events
        .wait_for_record_with_key(&session.session_id, &interop_key, Duration::from_secs(15))
        .unwrap_or_else(|| {
            panic!(
                "Milena did not observe CLI-produced key '{}'; events: {:?}",
                interop_key,
                events.snapshot()
            )
        });
    assert_eq!(record.topic, settings.interop_topic);
    assert_eq!(record.key.as_deref(), Some(interop_key.as_str()));
    assert_eq!(record.payload.as_deref(), Some(interop_payload.as_str()));
    stop_session(&adapter, &events, &session.session_id);
}

#[test]
fn failure_scenarios_surface_command_errors_without_hanging() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();

    assert_error_code(
        list_kafka_topics(
            ListKafkaTopicsRequest {
                auth: settings.auth_with_password(bad_password(&settings)),
            },
            &adapter,
        )
        .expect_err("bad credentials should surface as a command error"),
        MilenaCommandErrorCode::KafkaOperationFailed,
    );

    assert_error_code(
        list_kafka_topics(
            ListKafkaTopicsRequest {
                auth: settings
                    .auth_with_brokers(vec![env_or(
                        "MILENA_KAFKA_UNAVAILABLE_BROKERS",
                        "127.0.0.1:1",
                    )])
                    .with_property("request.timeout.ms", "1000")
                    .with_property("socket.timeout.ms", "1000"),
            },
            &adapter,
        )
        .expect_err("unavailable broker should surface as a command error"),
        MilenaCommandErrorCode::KafkaOperationFailed,
    );

    assert_error_code(
        list_kafka_topics(
            ListKafkaTopicsRequest {
                auth: settings
                    .auth
                    .clone()
                    .with_property("ssl.ca.location", missing_ca_path()),
            },
            &adapter,
        )
        .expect_err("bad CA path should surface as a command error"),
        MilenaCommandErrorCode::KafkaOperationFailed,
    );

    assert_error_code(
        publish_to_topic(&adapter, &settings, "   ".to_string(), None, "{}")
            .expect_err("blank topic should be rejected before Kafka I/O"),
        MilenaCommandErrorCode::TopicRequired,
    );

    assert_error_code(
        publish_to_topic(&adapter, &settings, settings.errors_topic.clone(), None, "")
            .expect_err("empty payload should be rejected before Kafka I/O"),
        MilenaCommandErrorCode::KafkaPayloadRequired,
    );

    assert_error_code(
        start_kafka_consumer_session(
            StartKafkaConsumerSessionRequest {
                auth: settings.auth.clone(),
                topics: vec![" ".to_string()],
                from_beginning: false,
            },
            RecordingBoundaryEventEmitter::default(),
            &adapter,
        )
        .expect_err("blank consumer topic should be rejected before Kafka I/O"),
        MilenaCommandErrorCode::TopicRequired,
    );

    let missing_topic = format!("milena.issue14.missing.{}", unique_id());
    assert_error_code(
        publish_to_topic(
            &adapter,
            &settings.with_message_timeout_ms("1000"),
            missing_topic,
            None,
            r#"{"fixture":"missing-topic"}"#,
        )
        .expect_err("publishing to a non-existent topic should fail with auto-create disabled"),
        MilenaCommandErrorCode::KafkaOperationFailed,
    );
}

#[derive(Clone, Debug)]
struct IntegrationSettings {
    auth: RuntimeAuthConfig,
    password: String,
    records_topic: String,
    polling_topic: String,
    publish_topic: String,
    keyed_topic: String,
    errors_topic: String,
    cleanup_topic: String,
    interop_topic: String,
}

impl IntegrationSettings {
    fn from_env() -> Option<Self> {
        if env::var(INTEGRATION_ENV).ok().as_deref() != Some("1") {
            eprintln!("skipping Kafka integration test; set {INTEGRATION_ENV}=1 to run");
            return None;
        }

        let brokers = env_or("MILENA_KAFKA_BROKERS", DEFAULT_BROKERS)
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let username = env_or("MILENA_KAFKA_USERNAME", DEFAULT_USERNAME);
        let password = env_or("MILENA_KAFKA_PASSWORD", DEFAULT_PASSWORD);
        let mechanism = env_or("MILENA_KAFKA_SASL_MECHANISM", DEFAULT_MECHANISM);
        let mut properties = BTreeMap::new();
        properties.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        properties.insert("sasl.mechanism".to_string(), mechanism);
        properties.insert("sasl.username".to_string(), username);
        properties.insert("sasl.password".to_string(), password.clone());
        properties.insert(
            "client.id".to_string(),
            format!("milena-issue14-{}", unique_id()),
        );
        properties.insert(
            "request.timeout.ms".to_string(),
            env_or("MILENA_KAFKA_REQUEST_TIMEOUT_MS", "3000"),
        );
        properties.insert(
            "socket.timeout.ms".to_string(),
            env_or("MILENA_KAFKA_SOCKET_TIMEOUT_MS", "3000"),
        );
        properties.insert(
            "message.timeout.ms".to_string(),
            env_or("MILENA_KAFKA_MESSAGE_TIMEOUT_MS", "3000"),
        );
        properties.insert("metadata.max.age.ms".to_string(), "1000".to_string());

        if let Some(ca_location) = ssl_ca_location() {
            properties.insert("ssl.ca.location".to_string(), ca_location);
        }
        if let Ok(value) = env::var("MILENA_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM") {
            properties.insert("ssl.endpoint.identification.algorithm".to_string(), value);
        }
        if let Ok(value) = env::var("MILENA_KAFKA_ENABLE_SSL_CERTIFICATE_VERIFICATION") {
            properties.insert("enable.ssl.certificate.verification".to_string(), value);
        }

        Some(Self {
            auth: RuntimeAuthConfig {
                environment: env_or("MILENA_KAFKA_ENVIRONMENT", "local-issue14"),
                brokers,
                properties,
            },
            password,
            records_topic: env_or("MILENA_KAFKA_RECORDS_TOPIC", RECORDS_TOPIC),
            polling_topic: env_or("MILENA_KAFKA_POLLING_TOPIC", POLLING_TOPIC),
            publish_topic: env_or("MILENA_KAFKA_PUBLISH_TOPIC", PUBLISH_TOPIC),
            keyed_topic: env_or("MILENA_KAFKA_KEYED_TOPIC", KEYED_TOPIC),
            errors_topic: env_or("MILENA_KAFKA_ERRORS_TOPIC", ERRORS_TOPIC),
            cleanup_topic: env_or("MILENA_KAFKA_CLEANUP_TOPIC", CLEANUP_TOPIC),
            interop_topic: env_or("MILENA_KAFKA_CLI_INTEROP_TOPIC", INTEROP_TOPIC),
        })
    }

    fn fixture_topics(&self) -> Vec<&str> {
        vec![
            self.records_topic.as_str(),
            self.polling_topic.as_str(),
            self.publish_topic.as_str(),
            self.keyed_topic.as_str(),
            self.errors_topic.as_str(),
            self.cleanup_topic.as_str(),
            self.interop_topic.as_str(),
        ]
    }

    fn auth_with_password(&self, password: String) -> RuntimeAuthConfig {
        self.auth.clone().with_property("sasl.password", password)
    }

    fn auth_with_brokers(&self, brokers: Vec<String>) -> RuntimeAuthConfig {
        let mut auth = self.auth.clone();
        auth.brokers = brokers;
        auth
    }

    fn with_message_timeout_ms(&self, timeout_ms: &str) -> Self {
        let mut settings = self.clone();
        settings.auth = settings
            .auth
            .clone()
            .with_property("message.timeout.ms", timeout_ms)
            .with_property("request.timeout.ms", timeout_ms)
            .with_property("socket.timeout.ms", timeout_ms);
        settings
    }
}

trait RuntimeAuthConfigTestExt {
    fn with_property(self, key: impl Into<String>, value: impl Into<String>) -> Self;
}

impl RuntimeAuthConfigTestExt for RuntimeAuthConfig {
    fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Default)]
struct RecordingBoundaryEventEmitter {
    events: Arc<Mutex<Vec<MilenaBoundaryEvent>>>,
}

impl RecordingBoundaryEventEmitter {
    fn snapshot(&self) -> Vec<MilenaBoundaryEvent> {
        self.events
            .lock()
            .expect("event lock should not be poisoned")
            .clone()
    }

    fn wait_for_started(&self, session_id: &str, timeout: Duration) -> bool {
        self.wait_until(timeout, |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MilenaBoundaryEvent::KafkaConsumerStarted { session_id: observed, .. }
                        if observed == session_id
                )
            })
        })
    }

    fn wait_for_stopped(&self, session_id: &str, timeout: Duration) -> bool {
        self.wait_until(timeout, |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MilenaBoundaryEvent::KafkaConsumerStopped { session_id: observed, .. }
                        if observed == session_id
                )
            })
        })
    }

    fn wait_for_record_with_key(
        &self,
        session_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Option<KafkaRecordEvent> {
        self.wait_for_record(session_id, timeout, |record| {
            record.key.as_deref() == Some(key)
        })
    }

    fn wait_for_record_with_payload(
        &self,
        session_id: &str,
        payload: &str,
        timeout: Duration,
    ) -> Option<KafkaRecordEvent> {
        self.wait_for_record(session_id, timeout, |record| {
            record.payload.as_deref() == Some(payload)
        })
    }

    fn wait_for_record(
        &self,
        session_id: &str,
        timeout: Duration,
        predicate: impl Fn(&KafkaRecordEvent) -> bool,
    ) -> Option<KafkaRecordEvent> {
        let started = Instant::now();
        loop {
            if let Some(record) = self.snapshot().into_iter().find_map(|event| match event {
                MilenaBoundaryEvent::KafkaRecord { record }
                    if record.session_id == session_id && predicate(&record) =>
                {
                    Some(record)
                }
                _ => None,
            }) {
                return Some(record);
            }

            if started.elapsed() >= timeout {
                return None;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn saw_record_with_key(&self, session_id: &str, key: &str) -> bool {
        self.snapshot().iter().any(|event| {
            matches!(
                event,
                MilenaBoundaryEvent::KafkaRecord { record }
                    if record.session_id == session_id && record.key.as_deref() == Some(key)
            )
        })
    }

    fn wait_until(
        &self,
        timeout: Duration,
        predicate: impl Fn(&[MilenaBoundaryEvent]) -> bool,
    ) -> bool {
        let started = Instant::now();
        loop {
            let events = self.snapshot();
            if predicate(&events) {
                return true;
            }
            if started.elapsed() >= timeout {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
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

fn start_session(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    events: &RecordingBoundaryEventEmitter,
    topic: String,
    from_beginning: bool,
) -> milena_lib::contracts::KafkaConsumerSession {
    let session = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth: settings.auth.clone(),
            topics: vec![topic],
            from_beginning,
        },
        events.clone(),
        adapter,
    )
    .expect("consumer session should start");
    assert_eq!(session.status, "started");
    assert!(
        events.wait_for_started(&session.session_id, Duration::from_secs(5)),
        "consumer start event was not observed; events: {:?}",
        events.snapshot()
    );
    session
}

fn stop_session(
    adapter: &NativeKafkaAdapter,
    events: &RecordingBoundaryEventEmitter,
    session_id: &str,
) {
    let stopped = stop_kafka_consumer_session(
        StopKafkaConsumerSessionRequest {
            session_id: session_id.to_string(),
        },
        adapter,
    )
    .expect("consumer session should stop");
    assert_eq!(stopped.status, "stopped");
    assert!(stopped.cleanup.attempted);
    assert!(
        events.wait_for_stopped(session_id, Duration::from_secs(5)),
        "consumer stop event was not observed; events: {:?}",
        events.snapshot()
    );
}

fn publish_to_topic(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    topic: String,
    key: Option<String>,
    payload: &str,
) -> CommandResult<milena_lib::contracts::PublishKafkaRecordResponse> {
    publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: settings.auth.clone(),
            topic,
            key,
            payload: payload.to_string(),
        },
        adapter,
    )
}

fn publish_until_observed_by_payload(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    events: &RecordingBoundaryEventEmitter,
    session_id: &str,
    topic: &str,
    payload: &str,
) -> Option<(
    milena_lib::contracts::PublishKafkaRecordResponse,
    KafkaRecordEvent,
)> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        let ack = publish_to_topic(adapter, settings, topic.to_string(), None, payload)
            .expect("post-session publish should succeed");
        if let Some(record) =
            events.wait_for_record_with_payload(session_id, payload, Duration::from_millis(700))
        {
            return Some((ack, record));
        }
    }
    None
}

fn publish_until_observed_by_key(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    events: &RecordingBoundaryEventEmitter,
    session_id: &str,
    topic: &str,
    key: &str,
    payload: &str,
) -> Option<(
    milena_lib::contracts::PublishKafkaRecordResponse,
    KafkaRecordEvent,
)> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        let ack = publish_to_topic(
            adapter,
            settings,
            topic.to_string(),
            Some(key.to_string()),
            payload,
        )
        .expect("post-session publish should succeed");
        if let Some(record) =
            events.wait_for_record_with_key(session_id, key, Duration::from_millis(700))
        {
            return Some((ack, record));
        }
    }
    None
}

fn assert_delivered_ack(
    ack: &milena_lib::contracts::PublishKafkaRecordResponse,
    expected_topic: &str,
) {
    assert_eq!(ack.topic, expected_topic);
    assert_eq!(ack.status, "delivered");
    assert!(ack.partition >= 0);
    assert!(ack.offset >= 0);
}

fn assert_error_code(error: MilenaCommandError, expected: MilenaCommandErrorCode) {
    assert_eq!(
        error.code, expected,
        "unexpected error for expected code {:?}: {:?}",
        expected, error
    );
}

fn assert_valid_json(payload: &str) {
    serde_json::from_str::<serde_json::Value>(payload)
        .expect("fixture payload should be valid JSON");
}

fn fixture_payload(fixture: &str, test_id: &str, phase: &str) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(fixture)
        .expect("fixture payload should be valid JSON");
    value["issue14"] = json!({
        "phase": phase,
        "testId": test_id
    });
    value.to_string()
}

fn consumer_payload(test_id: &str, phase: &str) -> String {
    json!({
        "fixture": "issue-14-consumer-record",
        "phase": phase,
        "testId": test_id
    })
    .to_string()
}

fn unique_id() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos()
        .to_string()
}

fn bad_password(settings: &IntegrationSettings) -> String {
    env::var("MILENA_KAFKA_BAD_PASSWORD")
        .ok()
        .filter(|password| password != &settings.password)
        .unwrap_or_else(|| format!("{}-wrong", settings.password))
}

fn ssl_ca_location() -> Option<String> {
    if let Ok(path) = env::var("MILENA_KAFKA_SSL_CA_LOCATION") {
        return Some(path);
    }

    let default_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/kafka/ca.crt");
    if default_path.exists() {
        Some(default_path.to_string_lossy().into_owned())
    } else {
        None
    }
}

fn missing_ca_path() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kafka/missing-ca.crt")
        .to_string_lossy()
        .into_owned()
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}
