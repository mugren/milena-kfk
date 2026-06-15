import type {
  KafkaRecordEvent,
  KafkaRecordHeader,
  KafkaRecordHeaders,
  MilenaBoundaryEvent,
} from "./tauri";

export type MessageRenderMode = "json" | "raw";

export type MessageRenderPreferenceStore = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
};

export type MessageRenderPreferences = Record<
  string,
  Record<string, MessageRenderMode>
>;

export type RenderKafkaRecordOptions = {
  mode?: MessageRenderMode;
  expanded?: boolean;
  maxPreviewChars?: number;
  maxExpandedChars?: number;
};

export type RenderedKafkaPayload = {
  mode: MessageRenderMode;
  format: "json" | "raw";
  preview: string;
  content: string;
  invalidJson: boolean;
  truncated: boolean;
  marker: string | null;
};

export type RenderedKafkaRecord = {
  identity: string;
  receiveTime: string;
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  expanded: boolean;
  payload: RenderedKafkaPayload;
  headers: KafkaRecordHeader[];
};

export const DEFAULT_MESSAGE_RENDER_MODE: MessageRenderMode = "json";
export const MESSAGE_RENDER_PREFERENCES_KEY =
  "milena.messageRenderPreferences.v1";
export const DEFAULT_PAYLOAD_PREVIEW_CHARS = 180;
export const DEFAULT_PAYLOAD_EXPANDED_CHARS = 8_000;

export function renderKafkaRecord(
  record: KafkaRecordEvent,
  options: RenderKafkaRecordOptions = {},
): RenderedKafkaRecord {
  const mode = options.mode ?? DEFAULT_MESSAGE_RENDER_MODE;
  const expanded = options.expanded ?? false;
  const renderedPayload = renderPayload(record.payload ?? "", {
    mode,
    maxPreviewChars:
      options.maxPreviewChars ?? DEFAULT_PAYLOAD_PREVIEW_CHARS,
    maxExpandedChars:
      options.maxExpandedChars ?? DEFAULT_PAYLOAD_EXPANDED_CHARS,
  });

  return {
    identity: kafkaRecordIdentity(record),
    receiveTime: formatReceiveTime(record.receivedAt),
    topic: record.topic,
    partition: record.partition,
    offset: record.offset,
    key: normalizeKey(record.key),
    expanded,
    payload: renderedPayload,
    headers: normalizeHeaders(record.headers),
  };
}

export function renderPayload(
  payload: string,
  {
    mode,
    maxPreviewChars,
    maxExpandedChars,
  }: {
    mode: MessageRenderMode;
    maxPreviewChars: number;
    maxExpandedChars: number;
  },
): RenderedKafkaPayload {
  if (mode === "raw") {
    return {
      mode,
      format: "raw",
      preview: capText(payload, maxPreviewChars).text,
      content: capText(payload, maxExpandedChars).text,
      invalidJson: false,
      truncated:
        capText(payload, maxPreviewChars).truncated ||
        capText(payload, maxExpandedChars).truncated,
      marker: null,
    };
  }

  const parsed = parseJsonPayload(payload);
  if (!parsed.ok) {
    return {
      mode,
      format: "raw",
      preview: capText(payload, maxPreviewChars).text,
      content: capText(payload, maxExpandedChars).text,
      invalidJson: true,
      truncated:
        capText(payload, maxPreviewChars).truncated ||
        capText(payload, maxExpandedChars).truncated,
      marker: "invalid JSON",
    };
  }

  const previewPayload = JSON.stringify(parsed.value);
  const expandedPayload = JSON.stringify(parsed.value, null, 2);
  const preview = capText(previewPayload, maxPreviewChars);
  const content = capText(expandedPayload, maxExpandedChars);

  return {
    mode,
    format: "json",
    preview: preview.text,
    content: content.text,
    invalidJson: false,
    truncated: preview.truncated || content.truncated,
    marker: null,
  };
}

export function stampKafkaRecordReceivedAt(
  event: MilenaBoundaryEvent,
  receivedAt: Date = new Date(),
): MilenaBoundaryEvent {
  if (event.event !== "kafkaRecord" || event.data.record.receivedAt) {
    return event;
  }

  return {
    ...event,
    data: {
      record: {
        ...event.data.record,
        receivedAt: receivedAt.toISOString(),
      },
    },
  };
}

export function kafkaRecordIdentity(record: KafkaRecordEvent): string {
  return [
    record.sessionId,
    record.topic,
    record.partition,
    record.offset,
  ].join(":");
}

export function readMessageRenderPreferences(
  store?: MessageRenderPreferenceStore,
): MessageRenderPreferences {
  if (!store) {
    return {};
  }

  try {
    const raw = store.getItem(MESSAGE_RENDER_PREFERENCES_KEY);
    if (!raw) {
      return {};
    }

    return parseMessageRenderPreferences(JSON.parse(raw));
  } catch {
    return {};
  }
}

export function setTopicMessageRenderMode(
  preferences: MessageRenderPreferences,
  environment: string,
  topic: string,
  mode: MessageRenderMode,
  store?: MessageRenderPreferenceStore,
): MessageRenderPreferences {
  const nextPreferences = {
    ...preferences,
    [environment]: {
      ...(preferences[environment] ?? {}),
      [topic]: mode,
    },
  };

  writeMessageRenderPreferences(nextPreferences, store);
  return nextPreferences;
}

export function removeMessageRenderPreferencesForEnvironment(
  environment: string,
  store?: MessageRenderPreferenceStore,
): MessageRenderPreferences {
  const preferences = readMessageRenderPreferences(store);
  const { [environment]: _deletedEnvironment, ...nextPreferences } = preferences;

  writeMessageRenderPreferences(nextPreferences, store);
  return nextPreferences;
}

export function topicMessageRenderMode(
  preferences: MessageRenderPreferences,
  environment: string,
  topic: string | null | undefined,
): MessageRenderMode {
  if (!topic) {
    return DEFAULT_MESSAGE_RENDER_MODE;
  }

  return (
    preferences[environment]?.[topic] ?? DEFAULT_MESSAGE_RENDER_MODE
  );
}

export function writeMessageRenderPreferences(
  preferences: MessageRenderPreferences,
  store?: MessageRenderPreferenceStore,
) {
  if (!store) {
    return;
  }

  store.setItem(MESSAGE_RENDER_PREFERENCES_KEY, JSON.stringify(preferences));
}

function parseJsonPayload(
  payload: string,
): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(payload) };
  } catch {
    return { ok: false };
  }
}

function capText(text: string, maxChars: number) {
  if (text.length <= maxChars) {
    return { text, truncated: false };
  }

  return {
    text: text.slice(0, Math.max(0, maxChars)).trimEnd(),
    truncated: true,
  };
}

function normalizeHeaders(
  headers: KafkaRecordHeaders | null | undefined,
): KafkaRecordHeader[] {
  if (!headers) {
    return [];
  }

  if (Array.isArray(headers)) {
    return headers
      .filter((header) => typeof header.key === "string")
      .map((header) => ({
        key: header.key,
        value: header.value ?? null,
      }));
  }

  return Object.entries(headers).flatMap(([key, value]) => {
    if (Array.isArray(value)) {
      return value.map((entry) => ({ key, value: entry }));
    }

    return [{ key, value }];
  });
}

function normalizeKey(key: string | null | undefined): string | null {
  return key && key.length > 0 ? key : null;
}

function formatReceiveTime(receivedAt: string | null | undefined): string {
  if (!receivedAt) {
    return "--:--:--";
  }

  const date = new Date(receivedAt);
  if (Number.isNaN(date.getTime())) {
    return receivedAt;
  }

  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function parseMessageRenderPreferences(
  value: unknown,
): MessageRenderPreferences {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).flatMap(([environment, topicModes]) => {
      if (
        !topicModes ||
        typeof topicModes !== "object" ||
        Array.isArray(topicModes)
      ) {
        return [];
      }

      const parsedModes = Object.fromEntries(
        Object.entries(topicModes).filter((entry): entry is [
          string,
          MessageRenderMode,
        ] => entry[1] === "json" || entry[1] === "raw"),
      );

      return [[environment, parsedModes]];
    }),
  );
}
