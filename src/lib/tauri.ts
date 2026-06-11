import { Channel, invoke } from "@tauri-apps/api/core";

export type MilenaCapability =
  | "command-boundary"
  | "event-channel"
  | "macos-dev-build";

export type AppState = {
  appName: string;
  version: string;
  platform: "macos-dev";
  capabilities: MilenaCapability[];
};

export type TopicSessionPreviewRequest = {
  topic: string;
  mode: "poll" | "publish";
};

export type TopicSessionPreview = {
  sessionId: string;
  topic: string;
  mode: TopicSessionPreviewRequest["mode"];
  status: "ready";
};

export type MilenaBoundaryEvent =
  | {
      event: "boundaryOpened";
      data: {
        sessionId: string;
        topic: string;
        mode: TopicSessionPreviewRequest["mode"];
      };
    }
  | {
      event: "boundaryReady";
      data: {
        sessionId: string;
        capabilities: MilenaCapability[];
      };
    };

export async function loadAppState(): Promise<AppState> {
  return invoke<AppState>("get_app_state");
}

export async function previewTopicSession(
  request: TopicSessionPreviewRequest,
  onEvent: (event: MilenaBoundaryEvent) => void,
): Promise<TopicSessionPreview> {
  const eventChannel = new Channel<MilenaBoundaryEvent>();
  eventChannel.onmessage = onEvent;

  return invoke<TopicSessionPreview>("preview_topic_session", {
    request,
    onEvent: eventChannel,
  });
}
