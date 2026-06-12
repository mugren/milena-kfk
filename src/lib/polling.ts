import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  RuntimeAuthConfig,
  StartKafkaConsumerSessionRequest,
  StopKafkaConsumerSessionRequest,
  StopKafkaConsumerSessionResponse,
} from "./tauri";
import {
  appendPaneActivity,
  closePane,
  markPaneError,
  markPanePollingStarted,
  markPanePollingStarting,
  stopPane,
  type WorkspaceState,
} from "./workspace";
import { stampKafkaRecordReceivedAt } from "./messages";

export type StartConsumerSession = (
  request: StartKafkaConsumerSessionRequest,
  onEvent: (event: MilenaBoundaryEvent) => void,
) => Promise<KafkaConsumerSession>;

export type StopConsumerSession = (
  request: StopKafkaConsumerSessionRequest,
) => Promise<StopKafkaConsumerSessionResponse>;

export type WorkspaceUpdate = (
  update: (workspace: WorkspaceState) => WorkspaceState,
) => void;

export type PanePollingOptions = {
  paneId: number;
  topic: string;
  auth: RuntimeAuthConfig;
  getWorkspace: () => WorkspaceState;
  updateWorkspace: WorkspaceUpdate;
  startConsumerSession: StartConsumerSession;
  stopConsumerSession?: StopConsumerSession;
  isCurrent?: () => boolean;
  onEvent?: (event: MilenaBoundaryEvent) => void;
  onError?: (error: string) => void;
  onStopError?: (error: string) => void;
  now?: () => Date;
};

export type StopPanePollingOptions = {
  paneId: number;
  getWorkspace: () => WorkspaceState;
  updateWorkspace: WorkspaceUpdate;
  stopConsumerSession: StopConsumerSession;
  onStopError?: (error: string) => void;
};

export function buildLatestOnlyConsumerRequest(
  auth: RuntimeAuthConfig,
  topic: string,
): StartKafkaConsumerSessionRequest {
  return {
    auth,
    topics: [topic],
    fromBeginning: false,
  };
}

export async function startPanePollingSession({
  paneId,
  topic,
  auth,
  getWorkspace,
  updateWorkspace,
  startConsumerSession,
  stopConsumerSession,
  isCurrent = () => true,
  onEvent,
  onError,
  onStopError,
  now = () => new Date(),
}: PanePollingOptions): Promise<KafkaConsumerSession | null> {
  const existingSessionId = getPanePollingSessionId(getWorkspace(), paneId);
  if (existingSessionId && stopConsumerSession) {
    await stopConsumerBestEffort(stopConsumerSession, existingSessionId, onStopError);
  }

  if (!isCurrent()) {
    return null;
  }

  updateWorkspace((current) => markPanePollingStarting(current, paneId, topic));

  try {
    const session = await startConsumerSession(
      buildLatestOnlyConsumerRequest(auth, topic),
      (event) => {
        if (!isCurrent()) {
          return;
        }

        const receivedEvent = stampKafkaRecordReceivedAt(event, now());
        updateWorkspace((current) => {
          const next = appendPaneActivity(current, paneId, receivedEvent);
          if (receivedEvent.event !== "kafkaConsumerError") {
            return next;
          }

          return markPaneError(next, paneId, receivedEvent.data.message);
        });
        onEvent?.(receivedEvent);
      },
    );

    if (!isCurrent()) {
      if (stopConsumerSession) {
        await stopConsumerBestEffort(
          stopConsumerSession,
          session.sessionId,
          onStopError,
        );
      }
      return null;
    }

    updateWorkspace((current) =>
      markPanePollingStarted(current, paneId, session),
    );
    return session;
  } catch (cause) {
    const message =
      cause instanceof Error ? cause.message : "Kafka consumer session failed";
    if (isCurrent()) {
      updateWorkspace((current) => markPaneError(current, paneId, message));
      onError?.(message);
    }
    return null;
  }
}

export async function stopPanePollingSession({
  paneId,
  getWorkspace,
  updateWorkspace,
  stopConsumerSession,
  onStopError,
}: StopPanePollingOptions): Promise<void> {
  const sessionId = getPanePollingSessionId(getWorkspace(), paneId);
  updateWorkspace((current) => stopPane(current, paneId));

  if (!sessionId) {
    return;
  }

  await stopConsumerBestEffort(stopConsumerSession, sessionId, onStopError);
}

export async function closePanePollingSession({
  paneId,
  getWorkspace,
  updateWorkspace,
  stopConsumerSession,
  onStopError,
}: StopPanePollingOptions): Promise<void> {
  const sessionId = getPanePollingSessionId(getWorkspace(), paneId);
  updateWorkspace((current) => closePane(current, paneId));

  if (!sessionId) {
    return;
  }

  await stopConsumerBestEffort(stopConsumerSession, sessionId, onStopError);
}

export function getPanePollingSessionId(
  workspace: WorkspaceState,
  paneId: number,
): string | null {
  const pane = workspace.panes.find((candidate) => candidate.id === paneId);
  if (pane?.mode !== "poll") {
    return null;
  }

  return pane.session?.sessionId ?? null;
}

async function stopConsumerBestEffort(
  stopConsumerSession: StopConsumerSession,
  sessionId: string,
  onStopError?: (error: string) => void,
) {
  try {
    await stopConsumerSession({ sessionId });
  } catch (cause) {
    onStopError?.(
      cause instanceof Error ? cause.message : "Kafka consumer cleanup failed",
    );
  }
}
