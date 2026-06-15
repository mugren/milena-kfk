use tauri::ipc::Channel;
use tauri::Manager;

pub mod command_boundary;
pub mod contracts;
pub mod environments;
pub mod kafka_adapter;

use contracts::{
    AppState, CommandResult, DeleteEnvironmentResponse, KafkaConsumerSession, KafkaTopicList,
    ListKafkaTopicsRequest, ListSavedEnvironmentsResponse, MilenaBoundaryEvent, MilenaCommandError,
    PublishKafkaRecordRequest, PublishKafkaRecordResponse, RuntimeAuthConfig,
    SaveEnvironmentRequest, SavedEnvironment, StartKafkaConsumerSessionRequest,
    StopKafkaConsumerSessionRequest, StopKafkaConsumerSessionResponse, TopicSessionPreview,
    TopicSessionPreviewRequest,
};
use environments::MacosKeychainEnvironmentSecretStore;
use kafka_adapter::NativeKafkaAdapter;

#[tauri::command]
fn get_app_state() -> AppState {
    command_boundary::app_state()
}

#[tauri::command]
fn preview_topic_session(
    request: TopicSessionPreviewRequest,
    on_event: Channel<MilenaBoundaryEvent>,
) -> CommandResult<TopicSessionPreview> {
    command_boundary::preview_topic_session(request, &on_event)
}

#[tauri::command]
fn save_environment(
    app_handle: tauri::AppHandle,
    request: SaveEnvironmentRequest,
) -> CommandResult<SavedEnvironment> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::save_environment(&config_dir, request, &MacosKeychainEnvironmentSecretStore)
}

#[tauri::command]
fn load_environment(app_handle: tauri::AppHandle, name: String) -> CommandResult<SavedEnvironment> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::load_environment(&config_dir, &name)
}

#[tauri::command]
fn list_environments(app_handle: tauri::AppHandle) -> CommandResult<ListSavedEnvironmentsResponse> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::list_environments(&config_dir)
}

#[tauri::command]
fn delete_environment(
    app_handle: tauri::AppHandle,
    name: String,
) -> CommandResult<DeleteEnvironmentResponse> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::delete_environment(&config_dir, &name, &MacosKeychainEnvironmentSecretStore)
}

#[tauri::command]
fn materialize_runtime_auth_config(
    app_handle: tauri::AppHandle,
    name: String,
) -> CommandResult<RuntimeAuthConfig> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::materialize_runtime_auth_config(
        &config_dir,
        &name,
        &MacosKeychainEnvironmentSecretStore,
    )
}

#[tauri::command]
fn materialize_temporary_runtime_auth_config(
    app_handle: tauri::AppHandle,
    request: SaveEnvironmentRequest,
) -> CommandResult<RuntimeAuthConfig> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environments::materialize_temporary_runtime_auth_config(
        &config_dir,
        request,
        &MacosKeychainEnvironmentSecretStore,
    )
}

#[tauri::command]
async fn list_kafka_topics(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: ListKafkaTopicsRequest,
) -> CommandResult<KafkaTopicList> {
    let kafka = kafka.inner().clone();
    run_blocking_kafka_command(move || command_boundary::list_kafka_topics(request, &kafka)).await
}

#[tauri::command]
async fn publish_kafka_record(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: PublishKafkaRecordRequest,
) -> CommandResult<PublishKafkaRecordResponse> {
    let kafka = kafka.inner().clone();
    run_blocking_kafka_command(move || command_boundary::publish_kafka_record(request, &kafka))
        .await
}

#[tauri::command]
async fn start_kafka_consumer_session(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: StartKafkaConsumerSessionRequest,
    on_event: Channel<MilenaBoundaryEvent>,
) -> CommandResult<KafkaConsumerSession> {
    let kafka = kafka.inner().clone();
    run_blocking_kafka_command(move || {
        command_boundary::start_kafka_consumer_session(request, on_event, &kafka)
    })
    .await
}

#[tauri::command]
async fn stop_kafka_consumer_session(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: StopKafkaConsumerSessionRequest,
) -> CommandResult<StopKafkaConsumerSessionResponse> {
    let kafka = kafka.inner().clone();
    run_blocking_kafka_command(move || {
        command_boundary::stop_kafka_consumer_session(request, &kafka)
    })
    .await
}

async fn run_blocking_kafka_command<T, F>(command: F) -> CommandResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> CommandResult<T> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(command)
        .await
        .map_err(|error| {
            MilenaCommandError::kafka_operation_failed(format!(
                "Kafka command worker failed: {error}"
            ))
        })?
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(NativeKafkaAdapter::default())
        .invoke_handler(tauri::generate_handler![
            get_app_state,
            preview_topic_session,
            save_environment,
            load_environment,
            list_environments,
            delete_environment,
            materialize_runtime_auth_config,
            materialize_temporary_runtime_auth_config,
            list_kafka_topics,
            publish_kafka_record,
            start_kafka_consumer_session,
            stop_kafka_consumer_session
        ])
        .run(tauri::generate_context!())
        .expect("error while running Milena");
}
