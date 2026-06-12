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
        MilenaCommandErrorCode, PublishKafkaRecordRequest, RuntimeAuthConfig,
        StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
    },
    kafka_adapter::NativeKafkaAdapter,
};
use serde_json::json;

const INTEGRATION_ENV: &str = "MILENA_KAFKA_INTEGRATION";
const DEFAULT_TOPIC: &str = "milena.issue14.records";
const DEFAULT_BROKERS: &str = "localhost:19092";
const DEFAULT_USERNAME: &str = "milena_plain";
const DEFAULT_PASSWORD: &str = "milena-plain-secret";
const DEFAULT_MECHANISM: &str = "PLAIN";

const UNKEYED_FIXTURE: &str = include_str!("fixtures/kafka/order_created.json");
const KEYED_FIXTURE: &str = include_str!("fixtures/kafka/order_keyed.json");

#[test]
fn broker_lists_fixture_topic_and_publishes_valid_json_with_and_without_key() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();

    assert_valid_json(UNKEYED_FIXTURE);
    assert_valid_json(KEYED_FIXTURE);

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
            .any(|topic| topic.name == settings.topic && topic.partition_count > 0),
        "expected topic '{}' in broker metadata, got {:?}",
        settings.topic,
        topics.topics
    );

    let unkeyed = publish_json(&adapter, &settings, None, UNKEYED_FIXTURE)
        .expect("unkeyed JSON publish should succeed");
    assert_eq!(unkeyed.topic, settings.topic);
    assert_eq!(unkeyed.status, "delivered");
    assert!(unkeyed.partition >= 0);
    assert!(unkeyed.offset >= 0);

    let keyed = publish_json(
        &adapter,
        &settings,
        Some(format!("issue-14-keyed-{}", unique_id())),
        KEYED_FIXTURE,
    )
    .expect("keyed JSON publish should succeed");
    assert_eq!(keyed.topic, settings.topic);
    assert_eq!(keyed.status, "delivered");
    assert!(keyed.partition >= 0);
    assert!(keyed.offset >= 0);
}

#[test]
fn latest_consumer_session_receives_only_records_published_after_start_and_reports_cleanup() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();
    let events = RecordingBoundaryEventEmitter::default();
    let test_id = unique_id();

    let before_key = format!("issue-14-before-{test_id}");
    let before_payload = consumer_payload(&test_id, "before-start");
    publish_json(
        &adapter,
        &settings,
        Some(before_key.clone()),
        &before_payload,
    )
    .expect("pre-session publish should succeed");

    let session = start_kafka_consumer_session(
        StartKafkaConsumerSessionRequest {
            auth: settings.auth.clone(),
            topics: vec![settings.topic.clone()],
            from_beginning: false,
        },
        events.clone(),
        &adapter,
    )
    .expect("consumer session should start");
    assert_eq!(session.status, "started");
    assert!(
        events.wait_for_started(&session.session_id, Duration::from_secs(5)),
        "consumer start event was not observed; events: {:?}",
        events.snapshot()
    );

    let after_key = format!("issue-14-after-{test_id}");
    let after_payload = consumer_payload(&test_id, "after-start");
    let record = publish_until_observed(
        &adapter,
        &settings,
        &events,
        &session.session_id,
        &after_key,
        &after_payload,
    );

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
    assert!(
        events.wait_for_stopped(&session.session_id, Duration::from_secs(5)),
        "consumer stop event was not observed; events: {:?}",
        events.snapshot()
    );

    let record = record.unwrap_or_else(|| {
        panic!(
            "consumer did not receive post-start record with key '{}'; events: {:?}",
            after_key,
            events.snapshot()
        )
    });
    assert_eq!(record.topic, settings.topic);
    assert_eq!(record.key.as_deref(), Some(after_key.as_str()));
    assert_eq!(record.payload.as_deref(), Some(after_payload.as_str()));

    assert!(
        !events.saw_record_with_key(&session.session_id, &before_key),
        "latest-only consumer unexpectedly received the pre-session record; events: {:?}",
        events.snapshot()
    );
}

#[test]
fn auth_and_broker_failures_surface_as_command_errors() {
    let settings = match IntegrationSettings::from_env() {
        Some(settings) => settings,
        None => return,
    };
    let adapter = NativeKafkaAdapter::default();

    let bad_auth_error = list_kafka_topics(
        ListKafkaTopicsRequest {
            auth: settings.auth_with_password(bad_password(&settings)),
        },
        &adapter,
    )
    .expect_err("bad credentials should surface as a command error");
    assert_eq!(
        bad_auth_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );

    let bad_broker_error = list_kafka_topics(
        ListKafkaTopicsRequest {
            auth: settings.auth_with_brokers(vec![env_or(
                "MILENA_KAFKA_UNAVAILABLE_BROKERS",
                "127.0.0.1:1",
            )]),
        },
        &adapter,
    )
    .expect_err("unavailable broker should surface as a command error");
    assert_eq!(
        bad_broker_error.code,
        MilenaCommandErrorCode::KafkaOperationFailed
    );
}

#[derive(Clone, Debug)]
struct IntegrationSettings {
    auth: RuntimeAuthConfig,
    topic: String,
    password: String,
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
            topic: env_or("MILENA_KAFKA_TOPIC", DEFAULT_TOPIC),
            password,
        })
    }

    fn auth_with_password(&self, password: String) -> RuntimeAuthConfig {
        let mut auth = self.auth.clone();
        auth.properties
            .insert("sasl.password".to_string(), password);
        auth
    }

    fn auth_with_brokers(&self, brokers: Vec<String>) -> RuntimeAuthConfig {
        let mut auth = self.auth.clone();
        auth.brokers = brokers;
        auth
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
        let started = Instant::now();
        loop {
            if let Some(record) = self.snapshot().into_iter().find_map(|event| match event {
                MilenaBoundaryEvent::KafkaRecord { record }
                    if record.session_id == session_id && record.key.as_deref() == Some(key) =>
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

fn publish_json(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    key: Option<String>,
    payload: &str,
) -> CommandResult<milena_lib::contracts::PublishKafkaRecordResponse> {
    publish_kafka_record(
        PublishKafkaRecordRequest {
            auth: settings.auth.clone(),
            topic: settings.topic.clone(),
            key,
            payload: payload.to_string(),
        },
        adapter,
    )
}

fn publish_until_observed(
    adapter: &NativeKafkaAdapter,
    settings: &IntegrationSettings,
    events: &RecordingBoundaryEventEmitter,
    session_id: &str,
    key: &str,
    payload: &str,
) -> Option<KafkaRecordEvent> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        publish_json(adapter, settings, Some(key.to_string()), payload)
            .expect("post-session publish should succeed");
        if let Some(record) =
            events.wait_for_record_with_key(session_id, key, Duration::from_millis(700))
        {
            return Some(record);
        }
    }
    None
}

fn assert_valid_json(payload: &str) {
    serde_json::from_str::<serde_json::Value>(payload)
        .expect("fixture payload should be valid JSON");
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

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}
