import {
  startPanePollingSession,
  type PanePollingOptions,
} from "./polling";
import type {
  KafkaConsumerSession,
  PublishKafkaRecordRequest,
  PublishKafkaRecordResponse,
  RuntimeAuthConfig,
} from "./tauri";
import type { WorkspacePane, WorkspaceState } from "./workspace";

export type PublisherStatus = "idle" | "sending" | "delivered" | "error";

export type ProducerAck = PublishKafkaRecordResponse & {
  key: string | null;
  sentAt: string;
};

export type PublisherPaneState = {
  topic: string | null;
  key: string;
  payload: string;
  status: PublisherStatus;
  error: string | null;
  ack: ProducerAck | null;
};

export type PublisherState = Record<number, PublisherPaneState>;

export type PublisherUpdate = (
  update: (current: PublisherState) => PublisherState,
) => void;

export type PublishRecord = (
  request: PublishKafkaRecordRequest,
) => Promise<PublishKafkaRecordResponse>;

export type CombinedPublishPollOptions = PanePollingOptions & {
  updatePublisherState: PublisherUpdate;
};

export type SendPublisherRecordOptions = {
  paneId: number;
  auth: RuntimeAuthConfig;
  getWorkspace: () => WorkspaceState;
  getPublisherState: () => PublisherState;
  updatePublisherState: PublisherUpdate;
  publishRecord: PublishRecord;
  now?: () => Date;
};

export function createInitialPublisherState(): PublisherState {
  return {};
}

export function openPublisherPane(
  state: PublisherState,
  paneId: number,
  topic: string,
): PublisherState {
  const current = state[paneId];
  if (current?.topic === topic) {
    return {
      ...state,
      [paneId]: {
        ...current,
        error: null,
      },
    };
  }

  return {
    ...state,
    [paneId]: createPublisherPaneState(topic),
  };
}

export function getPublisherPaneState(
  state: PublisherState,
  paneId: number,
  topic: string | null,
): PublisherPaneState {
  return state[paneId] ?? createPublisherPaneState(topic);
}

export function setPublisherPayload(
  state: PublisherState,
  paneId: number,
  payload: string,
  topic: string | null = null,
): PublisherState {
  const current = getPublisherPaneState(state, paneId, topic);
  return {
    ...state,
    [paneId]: {
      ...current,
      payload,
      status: current.status === "error" ? "idle" : current.status,
      error: null,
    },
  };
}

export function setPublisherKey(
  state: PublisherState,
  paneId: number,
  key: string,
  topic: string | null = null,
): PublisherState {
  const current = getPublisherPaneState(state, paneId, topic);
  return {
    ...state,
    [paneId]: {
      ...current,
      key,
      status: current.status === "error" ? "idle" : current.status,
      error: null,
    },
  };
}

export function formatPublisherPayload(
  state: PublisherState,
  paneId: number,
  topic: string | null = null,
): PublisherState {
  const current = getPublisherPaneState(state, paneId, topic);
  const formatted = formatJsonPayload(current.payload);
  if (!formatted.ok) {
    return {
      ...state,
      [paneId]: {
        ...current,
        status: "error",
        error: formatted.error,
      },
    };
  }

  return {
    ...state,
    [paneId]: {
      ...current,
      payload: formatted.payload,
      status: current.status === "error" ? "idle" : current.status,
      error: null,
    },
  };
}

export function validatePublisherPayload(payload: string):
  | { ok: true; payload: string }
  | { ok: false; error: string } {
  if (!payload.trim()) {
    return { ok: false, error: "Payload is required" };
  }

  try {
    JSON.parse(payload);
    return { ok: true, payload };
  } catch {
    return { ok: false, error: "Payload must be valid JSON" };
  }
}

export function formatJsonPayload(payload: string):
  | { ok: true; payload: string }
  | { ok: false; error: string } {
  const validation = validatePublisherPayload(payload);
  if (!validation.ok) {
    return validation;
  }

  return {
    ok: true,
    payload: JSON.stringify(JSON.parse(payload), null, 2),
  };
}

export function canSendPublisherRecord(
  pane: WorkspacePane | undefined,
  publisher: PublisherPaneState,
): boolean {
  return (
    pane?.mode === "poll" &&
    pane.status === "ready" &&
    Boolean(pane.topic) &&
    publisher.status !== "sending" &&
    validatePublisherPayload(publisher.payload).ok
  );
}

export async function openCombinedPublishPollPane({
  updatePublisherState,
  ...pollingOptions
}: CombinedPublishPollOptions): Promise<KafkaConsumerSession | null> {
  updatePublisherState((current) =>
    openPublisherPane(current, pollingOptions.paneId, pollingOptions.topic),
  );

  return startPanePollingSession(pollingOptions);
}

export async function sendPublisherRecord({
  paneId,
  auth,
  getWorkspace,
  getPublisherState,
  updatePublisherState,
  publishRecord,
  now = () => new Date(),
}: SendPublisherRecordOptions): Promise<PublishKafkaRecordResponse | null> {
  const workspace = getWorkspace();
  const pane = workspace.panes.find((candidate) => candidate.id === paneId);
  const publisher = getPublisherPaneState(
    getPublisherState(),
    paneId,
    pane?.topic ?? null,
  );
  const request = buildPublishKafkaRecordRequest(auth, pane, publisher);

  if (!request.ok) {
    updatePublisherState((current) =>
      setPublisherError(current, paneId, publisher, request.error),
    );
    return null;
  }

  updatePublisherState((current) =>
    setPublisherSending(current, paneId, publisher),
  );

  try {
    const ack = await publishRecord(request.request);
    updatePublisherState((current) =>
      setPublisherAck(current, paneId, publisher, ack, now()),
    );
    return ack;
  } catch (cause) {
    updatePublisherState((current) =>
      setPublisherError(
        current,
        paneId,
        publisher,
        cause instanceof Error ? cause.message : "Kafka publish failed",
      ),
    );
    return null;
  }
}

export function buildPublishKafkaRecordRequest(
  auth: RuntimeAuthConfig,
  pane: WorkspacePane | undefined,
  publisher: PublisherPaneState,
):
  | { ok: true; request: PublishKafkaRecordRequest }
  | { ok: false; error: string } {
  if (pane?.mode !== "poll" || pane.status !== "ready" || !pane.topic) {
    return { ok: false, error: "Start polling before publishing" };
  }

  const validation = validatePublisherPayload(publisher.payload);
  if (!validation.ok) {
    return validation;
  }

  return {
    ok: true,
    request: {
      auth,
      topic: pane.topic,
      key: normalizePublisherKey(publisher.key),
      payload: validation.payload,
    },
  };
}

export function producerAckLabel(ack: ProducerAck | null): string {
  if (!ack) {
    return "No producer ack";
  }

  return `${ack.status} / p${ack.partition} / ${ack.offset}`;
}

function createPublisherPaneState(topic: string | null): PublisherPaneState {
  return {
    topic,
    key: "",
    payload: defaultPublisherPayload(topic),
    status: "idle",
    error: null,
    ack: null,
  };
}

function defaultPublisherPayload(topic: string | null): string {
  return JSON.stringify(
    {
      topic,
      event: "preview",
    },
    null,
    2,
  );
}

function normalizePublisherKey(key: string): string | null {
  const trimmed = key.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function setPublisherSending(
  state: PublisherState,
  paneId: number,
  fallback: PublisherPaneState,
): PublisherState {
  const current = state[paneId] ?? fallback;
  return {
    ...state,
    [paneId]: {
      ...current,
      status: "sending",
      error: null,
    },
  };
}

function setPublisherAck(
  state: PublisherState,
  paneId: number,
  fallback: PublisherPaneState,
  ack: PublishKafkaRecordResponse,
  sentAt: Date,
): PublisherState {
  const current = state[paneId] ?? fallback;
  return {
    ...state,
    [paneId]: {
      ...current,
      status: "delivered",
      error: null,
      ack: {
        ...ack,
        key: normalizePublisherKey(current.key),
        sentAt: sentAt.toISOString(),
      },
    },
  };
}

function setPublisherError(
  state: PublisherState,
  paneId: number,
  fallback: PublisherPaneState,
  error: string,
): PublisherState {
  const current = state[paneId] ?? fallback;
  return {
    ...state,
    [paneId]: {
      ...current,
      status: "error",
      error,
    },
  };
}
