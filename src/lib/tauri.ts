import { setTheme } from "@tauri-apps/api/app";
import { Channel, invoke, isTauri } from "@tauri-apps/api/core";
import type { Theme } from "@tauri-apps/api/window";

export type AppAppearancePreference = "system" | "light" | "dark";

type NativeAppearanceTheme = Theme | null;

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

export type MilenaCommandErrorCode =
  | "topic-required"
  | "event-delivery-failed"
  | "environment-name-required"
  | "environment-brokers-required"
  | "environment-username-required"
  | "environment-password-required"
  | "environment-duplicate-name"
  | "environment-not-found"
  | "environment-storage-failed"
  | "environment-secret-store-failed"
  | "environment-secret-missing"
  | "environment-advanced-properties-invalid"
  | "kafka-auth-unsupported"
  | "kafka-operation-failed"
  | "kafka-session-not-found"
  | "kafka-payload-required";

export type MilenaCommandError = {
  code: MilenaCommandErrorCode;
  message: string;
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
    }
  | {
      event: "kafkaConsumerStarted";
      data: {
        sessionId: string;
        groupId: string;
        topics: string[];
      };
    }
  | {
      event: "kafkaRecord";
      data: {
        record: KafkaRecordEvent;
      };
    }
  | {
      event: "kafkaConsumerError";
      data: {
        sessionId: string;
        message: string;
      };
    }
  | {
      event: "kafkaConsumerStopped";
      data: {
        sessionId: string;
        groupId: string;
      };
    };

export type SaveEnvironmentRequest = {
  name: string;
  brokers: string[];
  authMode: EnvironmentAuthMode;
  username?: string | null;
  password?: string | null;
  advancedProperties: string;
};

export type SavedEnvironment = {
  schemaVersion: number;
  name: string;
  brokers: string[];
  authMode: EnvironmentAuthMode;
  username?: string | null;
  advancedProperties: string;
};

export type ListSavedEnvironmentsResponse = {
  environments: SavedEnvironment[];
};

export type DeleteEnvironmentResponse = {
  name: string;
  warning?: string | null;
};

export type EnvironmentAuthMode =
  | "plaintext"
  | "saslSslPlain"
  | "saslSslScramSha512";

export type RuntimeAuthConfig = {
  environment: string;
  brokers: string[];
  properties: Record<string, string>;
};

export type KafkaTopicMetadata = {
  name: string;
  partitionCount: number;
};

export type KafkaTopicList = {
  topics: KafkaTopicMetadata[];
};

export type ListKafkaTopicsRequest = {
  auth: RuntimeAuthConfig;
};

export type PublishKafkaRecordRequest = {
  auth: RuntimeAuthConfig;
  topic: string;
  key?: string | null;
  payload: string;
};

export type PublishKafkaRecordResponse = {
  topic: string;
  partition: number;
  offset: number;
  status: string;
};

export type StartKafkaConsumerSessionRequest = {
  auth: RuntimeAuthConfig;
  topics: string[];
  groupId?: string | null;
  fromBeginning?: boolean;
};

export type StopKafkaConsumerSessionRequest = {
  sessionId: string;
};

export type KafkaConsumerSession = {
  sessionId: string;
  groupId: string;
  topics: string[];
  status: string;
};

export type KafkaConsumerGroupCleanupAttempt = {
  groupId: string;
  attempted: boolean;
  succeeded: boolean;
  error?: string | null;
};

export type StopKafkaConsumerSessionResponse = {
  sessionId: string;
  groupId: string;
  status: string;
  cleanup: KafkaConsumerGroupCleanupAttempt;
};

export type KafkaRecordEvent = {
  sessionId: string;
  topic: string;
  partition: number;
  offset: number;
  key?: string | null;
  payload?: string | null;
  headers?: KafkaRecordHeaders | null;
  receivedAt?: string | null;
};

export type KafkaRecordHeader = {
  key: string;
  value?: string | null;
};

export type KafkaRecordHeaders =
  | KafkaRecordHeader[]
  | Record<string, string | string[] | null>;

export async function loadAppState(): Promise<AppState> {
  return invoke<AppState>("get_app_state");
}

export async function setAppAppearanceTheme(
  preference: AppAppearancePreference,
): Promise<void> {
  if (!isTauri()) {
    return;
  }

  const theme: NativeAppearanceTheme =
    preference === "system" ? null : preference;

  try {
    await setTheme(theme);
  } catch {
    // Non-Tauri test shells can import the API without having IPC available.
  }
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

export async function saveEnvironment(
  request: SaveEnvironmentRequest,
): Promise<SavedEnvironment> {
  return invoke<SavedEnvironment>("save_environment", { request });
}

export async function loadEnvironment(name: string): Promise<SavedEnvironment> {
  return invoke<SavedEnvironment>("load_environment", { name });
}

export async function listEnvironments(): Promise<ListSavedEnvironmentsResponse> {
  return invoke<ListSavedEnvironmentsResponse>("list_environments");
}

export async function deleteEnvironment(
  name: string,
): Promise<DeleteEnvironmentResponse> {
  return invoke<DeleteEnvironmentResponse>("delete_environment", { name });
}

export async function materializeRuntimeAuthConfig(
  name: string,
): Promise<RuntimeAuthConfig> {
  return invoke<RuntimeAuthConfig>("materialize_runtime_auth_config", { name });
}

export async function materializeTemporaryRuntimeAuthConfig(
  request: SaveEnvironmentRequest,
): Promise<RuntimeAuthConfig> {
  return invoke<RuntimeAuthConfig>("materialize_temporary_runtime_auth_config", {
    request,
  });
}

export async function listKafkaTopics(
  request: ListKafkaTopicsRequest,
): Promise<KafkaTopicList> {
  return invoke<KafkaTopicList>("list_kafka_topics", { request });
}

export async function publishKafkaRecord(
  request: PublishKafkaRecordRequest,
): Promise<PublishKafkaRecordResponse> {
  return invoke<PublishKafkaRecordResponse>("publish_kafka_record", { request });
}

export async function startKafkaConsumerSession(
  request: StartKafkaConsumerSessionRequest,
  onEvent: (event: MilenaBoundaryEvent) => void,
): Promise<KafkaConsumerSession> {
  const eventChannel = new Channel<MilenaBoundaryEvent>();
  eventChannel.onmessage = onEvent;

  return invoke<KafkaConsumerSession>("start_kafka_consumer_session", {
    request,
    onEvent: eventChannel,
  });
}

export async function stopKafkaConsumerSession(
  request: StopKafkaConsumerSessionRequest,
): Promise<StopKafkaConsumerSessionResponse> {
  return invoke<StopKafkaConsumerSessionResponse>("stop_kafka_consumer_session", {
    request,
  });
}
