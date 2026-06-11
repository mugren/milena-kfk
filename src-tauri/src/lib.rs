use tauri::ipc::Channel;
use tauri::Manager;

pub mod command_boundary;
pub mod contracts;
pub mod environments;

use contracts::{
    AppState, CommandResult, MilenaBoundaryEvent, RuntimeAuthConfig, SaveEnvironmentRequest,
    SavedEnvironment, TopicSessionPreview, TopicSessionPreviewRequest,
};
use environments::MacosKeychainEnvironmentSecretStore;

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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            get_app_state,
            preview_topic_session,
            save_environment,
            load_environment,
            materialize_runtime_auth_config
        ])
        .run(tauri::generate_context!())
        .expect("error while running Milena");
}
