use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rdkafka::{
    admin::{AdminClient, AdminOptions},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use tokio::sync::oneshot;

use crate::{
    command_boundary::BoundaryEventEmitter,
    contracts::{
        CommandResult, KafkaConsumerGroupCleanupAttempt, KafkaConsumerSession, KafkaRecordEvent,
        KafkaTopicMetadata, MilenaBoundaryEvent, MilenaCommandError, PublishKafkaRecordRequest,
        PublishKafkaRecordResponse, RuntimeAuthConfig, StartKafkaConsumerSessionRequest,
        StopKafkaConsumerSessionRequest, StopKafkaConsumerSessionResponse,
    },
};

const GROUP_ID_PREFIX: &str = "milena-poll";
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub trait KafkaAdapter {
    fn list_topics(&self, auth: &RuntimeAuthConfig) -> CommandResult<Vec<KafkaTopicMetadata>>;

    fn publish_record(
        &self,
        request: &PublishKafkaRecordRequest,
    ) -> CommandResult<PublishKafkaRecordResponse>;

    fn start_consumer_session<E>(
        &self,
        request: StartKafkaConsumerSessionRequest,
        event_emitter: E,
    ) -> CommandResult<KafkaConsumerSession>
    where
        E: BoundaryEventEmitter + Send + Sync + 'static;

    fn stop_consumer_session(
        &self,
        request: StopKafkaConsumerSessionRequest,
    ) -> CommandResult<StopKafkaConsumerSessionResponse>;
}

#[derive(Default)]
pub struct NativeKafkaAdapter {
    sessions: Arc<Mutex<BTreeMap<String, NativeConsumerSession>>>,
}

impl KafkaAdapter for NativeKafkaAdapter {
    fn list_topics(&self, auth: &RuntimeAuthConfig) -> CommandResult<Vec<KafkaTopicMetadata>> {
        let native = build_native_client_config(auth, None)?;
        let consumer: StreamConsumer = native.client_config().create().map_err(|error| {
            MilenaCommandError::kafka_operation_failed(format!(
                "failed to create Kafka client: {error}"
            ))
        })?;
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|error| MilenaCommandError::kafka_operation_failed(error))?;

        Ok(metadata
            .topics()
            .iter()
            .filter(|topic| !topic.name().starts_with("__"))
            .map(|topic| KafkaTopicMetadata {
                name: topic.name().to_string(),
                partition_count: topic.partitions().len() as i32,
            })
            .collect())
    }

    fn publish_record(
        &self,
        request: &PublishKafkaRecordRequest,
    ) -> CommandResult<PublishKafkaRecordResponse> {
        let topic = normalize_required_topic(&request.topic)?;
        if request.payload.is_empty() {
            return Err(MilenaCommandError::kafka_payload_required());
        }

        let native = build_native_client_config(&request.auth, None)?;
        let producer: FutureProducer = native.client_config().create().map_err(|error| {
            MilenaCommandError::kafka_operation_failed(format!(
                "failed to create Kafka producer: {error}"
            ))
        })?;

        let mut record = FutureRecord::to(&topic).payload(&request.payload);
        if let Some(key) = request.key.as_deref() {
            record = record.key(key);
        }

        let delivery = tauri::async_runtime::block_on(async {
            producer.send(record, Duration::from_secs(10)).await
        })
        .map_err(|(error, _message)| MilenaCommandError::kafka_operation_failed(error))?;

        Ok(PublishKafkaRecordResponse {
            topic,
            partition: delivery.0,
            offset: delivery.1,
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
        let topics = normalize_topics(request.topics)?;
        let group_id = next_consumer_group_id(&request.auth.environment);
        let session_id = group_id.clone();
        let mut native = build_native_client_config(&request.auth, Some(&group_id))?;
        native.set("enable.auto.commit", "false");
        native.set("enable.partition.eof", "false");
        native.set(
            "auto.offset.reset",
            if request.from_beginning {
                "earliest"
            } else {
                "latest"
            },
        );

        let consumer: StreamConsumer = native.client_config().create().map_err(|error| {
            MilenaCommandError::kafka_operation_failed(format!(
                "failed to create Kafka consumer: {error}"
            ))
        })?;
        let topic_refs = topics.iter().map(String::as_str).collect::<Vec<_>>();
        consumer
            .subscribe(&topic_refs)
            .map_err(|error| MilenaCommandError::kafka_operation_failed(error))?;

        event_emitter.emit(MilenaBoundaryEvent::KafkaConsumerStarted {
            session_id: session_id.clone(),
            group_id: group_id.clone(),
            topics: topics.clone(),
        })?;

        let (stop_tx, stop_rx) = oneshot::channel();
        self.sessions
            .lock()
            .expect("Kafka session registry should not be poisoned")
            .insert(
                session_id.clone(),
                NativeConsumerSession {
                    auth: request.auth,
                    group_id: group_id.clone(),
                    stop_tx: Some(stop_tx),
                },
            );

        tauri::async_runtime::spawn(run_consumer_loop(
            session_id.clone(),
            group_id.clone(),
            consumer,
            event_emitter,
            stop_rx,
        ));

        Ok(KafkaConsumerSession {
            session_id,
            group_id,
            topics,
            status: "started".to_string(),
        })
    }

    fn stop_consumer_session(
        &self,
        request: StopKafkaConsumerSessionRequest,
    ) -> CommandResult<StopKafkaConsumerSessionResponse> {
        let session = self
            .sessions
            .lock()
            .expect("Kafka session registry should not be poisoned")
            .remove(&request.session_id)
            .ok_or_else(|| MilenaCommandError::kafka_session_not_found(&request.session_id))?;

        if let Some(stop_tx) = session.stop_tx {
            let _ = stop_tx.send(());
        }

        let cleanup = cleanup_consumer_group(&session.auth, &session.group_id);
        Ok(StopKafkaConsumerSessionResponse {
            session_id: request.session_id,
            group_id: session.group_id,
            status: "stopped".to_string(),
            cleanup,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeClientConfig {
    entries: BTreeMap<String, String>,
}

impl NativeClientConfig {
    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.entries
    }

    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.entries.insert(key.into(), value.into());
    }

    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        for (key, value) in &self.entries {
            config.set(key, value);
        }
        config
    }
}

pub fn build_native_client_config(
    auth: &RuntimeAuthConfig,
    group_id: Option<&str>,
) -> CommandResult<NativeClientConfig> {
    let brokers = auth
        .brokers
        .iter()
        .map(|broker| broker.trim())
        .filter(|broker| !broker.is_empty())
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        return Err(MilenaCommandError::environment_brokers_required());
    }

    let security_protocol = required_property(auth, "security.protocol")?;
    if !security_protocol.eq_ignore_ascii_case("SASL_SSL") {
        return Err(MilenaCommandError::kafka_auth_unsupported(format!(
            "unsupported Kafka security.protocol '{security_protocol}'; MVP supports SASL_SSL"
        )));
    }

    let mechanism = normalize_sasl_mechanism(required_property(auth, "sasl.mechanism")?)?;
    let credentials = extract_credentials(&auth.properties)?;

    let mut entries = auth.properties.clone();
    entries.insert("bootstrap.servers".to_string(), brokers.join(","));
    entries.insert("security.protocol".to_string(), "SASL_SSL".to_string());
    entries.insert("sasl.mechanism".to_string(), mechanism);
    entries.insert("sasl.username".to_string(), credentials.username);
    entries.insert("sasl.password".to_string(), credentials.password);
    entries.remove("sasl.jaas.config");
    resolve_ssl_ca_location(&mut entries);
    if let Some(group_id) = group_id {
        entries.insert("group.id".to_string(), group_id.to_string());
    }

    Ok(NativeClientConfig { entries })
}

pub fn next_consumer_group_id(environment: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let environment = sanitize_group_id_component(environment);
    format!("{GROUP_ID_PREFIX}-{environment}-{timestamp}-{counter}")
}

pub fn cleanup_consumer_group(
    auth: &RuntimeAuthConfig,
    group_id: &str,
) -> KafkaConsumerGroupCleanupAttempt {
    let attempted = true;
    let result = (|| -> CommandResult<()> {
        let native = build_native_client_config(auth, None)?;
        let admin: AdminClient<DefaultClientContext> =
            native.client_config().create().map_err(|error| {
                MilenaCommandError::kafka_operation_failed(format!(
                    "failed to create Kafka admin client: {error}"
                ))
            })?;
        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
        let groups = [group_id];
        let results =
            tauri::async_runtime::block_on(async { admin.delete_groups(&groups, &options).await })
                .map_err(|error| MilenaCommandError::kafka_operation_failed(error))?;

        for result in results {
            if let Err((name, error)) = result {
                return Err(MilenaCommandError::kafka_operation_failed(format!(
                    "failed to delete Kafka consumer group '{name}': {error}"
                )));
            }
        }

        Ok(())
    })();

    KafkaConsumerGroupCleanupAttempt {
        group_id: group_id.to_string(),
        attempted,
        succeeded: result.is_ok(),
        error: result.err().map(|error| error.message),
    }
}

async fn run_consumer_loop<E>(
    session_id: String,
    group_id: String,
    consumer: StreamConsumer,
    event_emitter: E,
    mut stop_rx: oneshot::Receiver<()>,
) where
    E: BoundaryEventEmitter + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                let _ = event_emitter.emit(MilenaBoundaryEvent::KafkaConsumerStopped {
                    session_id,
                    group_id,
                });
                return;
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        let payload = message.payload().map(|payload| String::from_utf8_lossy(payload).to_string());
                        let key = message.key().map(|key| String::from_utf8_lossy(key).to_string());
                        let _ = event_emitter.emit(MilenaBoundaryEvent::KafkaRecord {
                            record: KafkaRecordEvent {
                                session_id: session_id.clone(),
                                topic: message.topic().to_string(),
                                partition: message.partition(),
                                offset: message.offset(),
                                key,
                                payload,
                            },
                        });
                    }
                    Err(error) => {
                        let _ = event_emitter.emit(MilenaBoundaryEvent::KafkaConsumerError {
                            session_id: session_id.clone(),
                            message: error.to_string(),
                        });
                    }
                }
            }
        }
    }
}

fn normalize_topics(topics: Vec<String>) -> CommandResult<Vec<String>> {
    let topics = topics
        .into_iter()
        .map(|topic| topic.trim().to_string())
        .filter(|topic| !topic.is_empty())
        .collect::<Vec<_>>();

    if topics.is_empty() {
        Err(MilenaCommandError::topic_required())
    } else {
        Ok(topics)
    }
}

fn normalize_required_topic(topic: &str) -> CommandResult<String> {
    let topic = topic.trim();
    if topic.is_empty() {
        Err(MilenaCommandError::topic_required())
    } else {
        Ok(topic.to_string())
    }
}

fn required_property<'a>(auth: &'a RuntimeAuthConfig, key: &str) -> CommandResult<&'a str> {
    auth.properties
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            MilenaCommandError::kafka_auth_unsupported(format!(
                "missing required Kafka auth property '{key}'"
            ))
        })
}

fn resolve_ssl_ca_location(entries: &mut BTreeMap<String, String>) {
    let Some(ca_location) = entries.get("ssl.ca.location") else {
        return;
    };
    let ca_path = Path::new(ca_location);
    if ca_path.is_absolute() {
        return;
    }

    if let Some(resolved) = resolve_existing_path(ca_path) {
        entries.insert(
            "ssl.ca.location".to_string(),
            resolved.to_string_lossy().into_owned(),
        );
    }
}

fn resolve_existing_path(relative_path: &Path) -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        PathBuf::from(relative_path),
        manifest_dir.join(relative_path),
        manifest_dir.join("..").join(relative_path),
    ];

    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .map(|candidate| candidate.canonicalize().unwrap_or(candidate))
}

fn normalize_sasl_mechanism(mechanism: &str) -> CommandResult<String> {
    let mechanism = mechanism.trim().to_ascii_uppercase();
    match mechanism.as_str() {
        "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" => Ok(mechanism),
        _ => Err(MilenaCommandError::kafka_auth_unsupported(format!(
            "unsupported Kafka sasl.mechanism '{mechanism}'; MVP supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512"
        ))),
    }
}

fn extract_credentials(properties: &BTreeMap<String, String>) -> CommandResult<KafkaCredentials> {
    let username = properties
        .get("sasl.username")
        .map(|value| value.trim().to_string());
    let password = properties
        .get("sasl.password")
        .map(|value| value.trim().to_string());

    match (username, password) {
        (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => {
            Ok(KafkaCredentials { username, password })
        }
        _ => {
            let jaas = properties.get("sasl.jaas.config").ok_or_else(|| {
                MilenaCommandError::kafka_auth_unsupported(
                    "missing sasl.username/sasl.password or sasl.jaas.config",
                )
            })?;
            parse_jaas_credentials(jaas)
        }
    }
}

fn parse_jaas_credentials(jaas: &str) -> CommandResult<KafkaCredentials> {
    let username = parse_jaas_assignment(jaas, "username")?;
    let password = parse_jaas_assignment(jaas, "password")?;
    Ok(KafkaCredentials { username, password })
}

fn parse_jaas_assignment(jaas: &str, key: &str) -> CommandResult<String> {
    let needle = format!("{key}=");
    let start = jaas.find(&needle).ok_or_else(|| {
        MilenaCommandError::kafka_auth_unsupported(format!("sasl.jaas.config is missing '{key}'"))
    })? + needle.len();
    let remainder = jaas[start..].trim_start();

    let value = if let Some(quoted) = remainder.strip_prefix('"') {
        let end = quoted.find('"').ok_or_else(|| {
            MilenaCommandError::kafka_auth_unsupported(format!(
                "sasl.jaas.config has unterminated quoted '{key}'"
            ))
        })?;
        quoted[..end].to_string()
    } else {
        remainder
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches(';')
            .to_string()
    };

    if value.is_empty() {
        Err(MilenaCommandError::kafka_auth_unsupported(format!(
            "sasl.jaas.config has empty '{key}'"
        )))
    } else {
        Ok(value)
    }
}

fn sanitize_group_id_component(value: &str) -> String {
    let sanitized = value
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
        .to_string();

    if sanitized.is_empty() {
        "environment".to_string()
    } else {
        sanitized
    }
}

#[derive(Debug, PartialEq, Eq)]
struct KafkaCredentials {
    username: String,
    password: String,
}

struct NativeConsumerSession {
    auth: RuntimeAuthConfig,
    group_id: String,
    stop_tx: Option<oneshot::Sender<()>>,
}
