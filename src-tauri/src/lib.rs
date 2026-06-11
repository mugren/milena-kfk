use tauri::ipc::Channel;
use tauri::Manager;

pub mod command_boundary;
pub mod contracts;
pub mod environment_import;
pub mod environments;
pub mod kafka_adapter;

use contracts::{
    AppState, CommandResult, ImportKafkaShellEnvironmentRequest,
    ImportKafkaShellEnvironmentResponse, KafkaConsumerSession, KafkaTopicList,
    ListKafkaTopicsRequest, MilenaBoundaryEvent, PublishKafkaRecordRequest,
    PublishKafkaRecordResponse, RuntimeAuthConfig, SaveEnvironmentRequest, SavedEnvironment,
    StartKafkaConsumerSessionRequest, StopKafkaConsumerSessionRequest,
    StopKafkaConsumerSessionResponse, TopicSessionPreview, TopicSessionPreviewRequest,
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
fn import_kafka_shell_environment(
    app_handle: tauri::AppHandle,
    request: ImportKafkaShellEnvironmentRequest,
) -> CommandResult<ImportKafkaShellEnvironmentResponse> {
    let config_dir = app_handle
        .path()
        .app_config_dir()
        .map_err(contracts::MilenaCommandError::environment_storage_failed)?;
    environment_import::import_kafka_shell_environment(
        &config_dir,
        request,
        &MacosKeychainEnvironmentSecretStore,
    )
}

#[tauri::command]
fn list_kafka_topics(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: ListKafkaTopicsRequest,
) -> CommandResult<KafkaTopicList> {
    command_boundary::list_kafka_topics(request, &*kafka)
}

#[tauri::command]
fn publish_kafka_record(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: PublishKafkaRecordRequest,
) -> CommandResult<PublishKafkaRecordResponse> {
    command_boundary::publish_kafka_record(request, &*kafka)
}

#[tauri::command]
fn start_kafka_consumer_session(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: StartKafkaConsumerSessionRequest,
    on_event: Channel<MilenaBoundaryEvent>,
) -> CommandResult<KafkaConsumerSession> {
    command_boundary::start_kafka_consumer_session(request, on_event, &*kafka)
}

#[tauri::command]
fn stop_kafka_consumer_session(
    kafka: tauri::State<'_, NativeKafkaAdapter>,
    request: StopKafkaConsumerSessionRequest,
) -> CommandResult<StopKafkaConsumerSessionResponse> {
    command_boundary::stop_kafka_consumer_session(request, &*kafka)
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
            materialize_runtime_auth_config,
            import_kafka_shell_environment,
            list_kafka_topics,
            publish_kafka_record,
            start_kafka_consumer_session,
            stop_kafka_consumer_session
        ])
        .run(tauri::generate_context!())
        .expect("error while running Milena");
}
