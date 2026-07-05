import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  RuntimeAuthConfig,
  StartKafkaConsumerSessionRequest,
  StopKafkaConsumerSessionRequest,
  StopKafkaConsumerSessionResponse,
} from "./tauri";
import {
  appendTabActivity,
  closeTab,
  markTabError,
  markTabPollingStarted,
  markTabPollingStarting,
  stopTab,
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

export type TabPollingOptions = {
  tabId: number;
  topic: string;
  auth: RuntimeAuthConfig;
  consumerGroupId?: string;
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

export type StopTabPollingOptions = {
  tabId: number;
  getWorkspace: () => WorkspaceState;
  updateWorkspace: WorkspaceUpdate;
  stopConsumerSession: StopConsumerSession;
  onStopError?: (error: string) => void;
};

export type PanePollingOptions = Omit<TabPollingOptions, "tabId"> & {
  paneId: number;
};

export type StopPanePollingOptions = Omit<StopTabPollingOptions, "tabId"> & {
  paneId: number;
};

export function buildLatestOnlyConsumerRequest(
  auth: RuntimeAuthConfig,
  topic: string,
  groupId?: string,
): StartKafkaConsumerSessionRequest {
  return {
    auth,
    topics: [topic],
    groupId,
    fromBeginning: false,
  };
}

export function defaultTabConsumerGroup(tabId: number): string {
  return `milena-poll-tab-${tabId}`;
}

export async function startTabPollingSession({
  tabId,
  topic,
  auth,
  consumerGroupId = defaultTabConsumerGroup(tabId),
  getWorkspace,
  updateWorkspace,
  startConsumerSession,
  stopConsumerSession,
  isCurrent = () => true,
  onEvent,
  onError,
  onStopError,
  now = () => new Date(),
}: TabPollingOptions): Promise<KafkaConsumerSession | null> {
  const tab = getWorkspaceTab(getWorkspace(), tabId);
  if (tab?.topic !== topic) {
    return null;
  }

  const existingSessionId = getTabPollingSessionId(getWorkspace(), tabId);
  if (existingSessionId && stopConsumerSession) {
    await stopConsumerBestEffort(stopConsumerSession, existingSessionId, onStopError);
  }

  if (!isCurrent()) {
    return null;
  }

  updateWorkspace((current) =>
    markTabPollingStarting(current, tabId, topic, consumerGroupId),
  );

  try {
    const session = await startConsumerSession(
      buildLatestOnlyConsumerRequest(auth, topic, consumerGroupId),
      (event) => {
        if (
          !isCurrent() ||
          !eventBelongsToTabPollingRun(
            getWorkspace(),
            tabId,
            event,
            consumerGroupId,
          )
        ) {
          return;
        }

        const receivedEvent = stampKafkaRecordReceivedAt(event, now());
        updateWorkspace((current) => {
          const next = appendTabActivity(current, tabId, receivedEvent);
          if (receivedEvent.event !== "kafkaConsumerError") {
            return next;
          }

          return markTabError(next, tabId, receivedEvent.data.message);
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
      markTabPollingStarted(current, tabId, session),
    );
    return session;
  } catch (cause) {
    const message =
      cause instanceof Error ? cause.message : "Kafka consumer session failed";
    if (isCurrent()) {
      updateWorkspace((current) => markTabError(current, tabId, message));
      onError?.(message);
    }
    return null;
  }
}

export async function startPanePollingSession({
  paneId,
  ...options
}: PanePollingOptions): Promise<KafkaConsumerSession | null> {
  return startTabPollingSession({ tabId: paneId, ...options });
}

export async function stopTabPollingSession({
  tabId,
  getWorkspace,
  updateWorkspace,
  stopConsumerSession,
  onStopError,
}: StopTabPollingOptions): Promise<void> {
  const sessionId = getTabPollingSessionId(getWorkspace(), tabId);
  updateWorkspace((current) => stopTab(current, tabId));

  if (!sessionId) {
    return;
  }

  await stopConsumerBestEffort(stopConsumerSession, sessionId, onStopError);
}

export async function stopPanePollingSession({
  paneId,
  ...options
}: StopPanePollingOptions): Promise<void> {
  await stopTabPollingSession({ tabId: paneId, ...options });
}

export async function closeTabPollingSession({
  tabId,
  getWorkspace,
  updateWorkspace,
  stopConsumerSession,
  onStopError,
}: StopTabPollingOptions): Promise<void> {
  const sessionId = getTabPollingSessionId(getWorkspace(), tabId);
  updateWorkspace((current) => {
    const result = closeTab(current, tabId);
    return result.status === "closed" ? result.workspace : current;
  });

  if (!sessionId) {
    return;
  }

  await stopConsumerBestEffort(stopConsumerSession, sessionId, onStopError);
}

export async function closePanePollingSession({
  paneId,
  ...options
}: StopPanePollingOptions): Promise<void> {
  await closeTabPollingSession({ tabId: paneId, ...options });
}

export function getTabPollingSessionId(
  workspace: WorkspaceState,
  tabId: number,
): string | null {
  const tab = workspace.groups
    .flatMap((group) => group.tabs)
    .find((candidate) => candidate.id === tabId);
  if (tab?.mode !== "poll") {
    return null;
  }

  return (
    tab.session?.sessionId ??
    (tab.status === "loading" ? tab.consumerGroup : null)
  );
}

export const getPanePollingSessionId = getTabPollingSessionId;

function getWorkspaceTab(workspace: WorkspaceState, tabId: number) {
  return workspace.groups
    .flatMap((group) => group.tabs)
    .find((candidate) => candidate.id === tabId);
}

function eventBelongsToTabPollingRun(
  workspace: WorkspaceState,
  tabId: number,
  event: MilenaBoundaryEvent,
  expectedSessionId: string,
): boolean {
  const tab = getWorkspaceTab(workspace, tabId);
  if (!tab || tab.mode !== "poll" || tab.status === "idle") {
    return false;
  }

  const eventSessionId = eventPollingSessionId(event);
  if (!eventSessionId) {
    return true;
  }

  return (
    (tab.session?.sessionId ?? tab.consumerGroup ?? expectedSessionId) ===
    eventSessionId
  );
}

function eventPollingSessionId(event: MilenaBoundaryEvent): string | null {
  switch (event.event) {
    case "boundaryOpened":
    case "kafkaConsumerStarted":
    case "kafkaConsumerError":
    case "kafkaConsumerStopped":
      return event.data.sessionId;
    case "boundaryReady":
      return event.data.sessionId;
    case "kafkaRecord":
      return event.data.record.sessionId;
  }
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
