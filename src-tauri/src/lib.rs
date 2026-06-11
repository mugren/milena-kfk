use tauri::ipc::Channel;

pub mod command_boundary;
pub mod contracts;

use contracts::{
    AppState, CommandResult, MilenaBoundaryEvent, TopicSessionPreview, TopicSessionPreviewRequest,
};

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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            get_app_state,
            preview_topic_session
        ])
        .run(tauri::generate_context!())
        .expect("error while running Milena");
}
