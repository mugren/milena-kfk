import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  TopicSessionPreview,
  TopicSessionPreviewRequest,
} from "./tauri";

export type BoundaryStatus = "idle" | "loading" | "ready" | "error";
export type PaneMode = "idle" | TopicSessionPreviewRequest["mode"];
export type PaneTone = "normal" | "warning" | "error";
export type SplitDirection = "right" | "top" | "bottom";
export type TopicOpenPlacement = "selected" | SplitDirection;
export type TopicOpenStatus = "opened" | "pane-limit" | "missing-target";
export type TopicOpenResult = {
  status: TopicOpenStatus;
  workspace: WorkspaceState;
};
export type WorkspaceLayout = "single" | "two-right" | "two-top" | "quad";

export type WorkspacePane = {
  id: number;
  topic: string | null;
  consumerGroup: string | null;
  mode: PaneMode;
  status: BoundaryStatus;
  session: TopicSessionPreview | KafkaConsumerSession | null;
  activity: MilenaBoundaryEvent[];
  error: string | null;
  tone: PaneTone;
};

export const MAX_PANE_ACTIVITY = 1_000;

export type WorkspaceState = {
  layout: WorkspaceLayout;
  selectedPaneId: number;
  expandedPaneId: number | null;
  selectedTopic: string | null;
  nextPaneId: number;
  panes: WorkspacePane[];
};

export function createInitialWorkspaceState(): WorkspaceState {
  return {
    layout: "single",
    selectedPaneId: 0,
    expandedPaneId: null,
    selectedTopic: null,
    nextPaneId: 1,
    panes: [],
  };
}

export function createEmptyPane(id: number): WorkspacePane {
  return {
    id,
    topic: null,
    consumerGroup: null,
    mode: "idle",
    status: "idle",
    session: null,
    activity: [],
    error: null,
    tone: "normal",
  };
}

export function isPaneEmpty(pane: WorkspacePane): boolean {
  return (
    pane.topic === null &&
    pane.consumerGroup === null &&
    pane.mode === "idle" &&
    pane.status === "idle" &&
    pane.session === null &&
    pane.activity.length === 0 &&
    pane.error === null
  );
}

export function canStartPaneSession(
  pane: WorkspacePane | undefined,
): pane is WorkspacePane & { topic: string } {
  return Boolean(pane?.topic);
}

export function getSelectedPane(
  workspace: WorkspaceState,
): WorkspacePane | undefined {
  return (
    workspace.panes.find((pane) => pane.id === workspace.selectedPaneId) ??
    workspace.panes[0]
  );
}

export function selectPane(
  workspace: WorkspaceState,
  paneId: number,
): WorkspaceState {
  if (!workspace.panes.some((pane) => pane.id === paneId)) {
    return workspace;
  }

  return { ...workspace, selectedPaneId: paneId };
}

export function selectTopicPreview(
  workspace: WorkspaceState,
  topic: string,
): WorkspaceState {
  return { ...workspace, selectedTopic: topic };
}

export function expandPane(
  workspace: WorkspaceState,
  paneId: number,
): WorkspaceState {
  const pane = workspace.panes.find((candidate) => candidate.id === paneId);
  if (!pane) {
    return workspace;
  }

  return {
    ...workspace,
    selectedPaneId: pane.id,
    expandedPaneId: pane.id,
    selectedTopic: pane.topic,
  };
}

export function restoreExpandedPane(workspace: WorkspaceState): WorkspaceState {
  if (workspace.expandedPaneId === null) {
    return workspace;
  }

  return {
    ...workspace,
    expandedPaneId: null,
  };
}

export function assignTopicToPane(
  workspace: WorkspaceState,
  paneId: number,
  topic: string,
): WorkspaceState {
  if (workspace.panes.length === 0) {
    const pane = {
      ...createEmptyPane(workspace.nextPaneId),
      topic,
      consumerGroup: `milena-preview-${workspace.nextPaneId}`,
    };

    return {
      ...workspace,
      selectedPaneId: pane.id,
      selectedTopic: topic,
      nextPaneId: workspace.nextPaneId + 1,
      panes: [pane],
    };
  }

  return updatePane(
    { ...workspace, selectedTopic: topic },
    paneId,
    (pane) => ({
      ...createEmptyPane(pane.id),
      topic,
      consumerGroup: `milena-preview-${pane.id}`,
    }),
  );
}

export function openTopicInWorkspace(
  workspace: WorkspaceState,
  topic: string,
  placement: TopicOpenPlacement,
): TopicOpenResult {
  if (workspace.panes.length === 0) {
    return {
      status: "opened",
      workspace: assignTopicToPane(workspace, workspace.selectedPaneId, topic),
    };
  }

  const targetPane = getSelectedPane(workspace);
  if (!targetPane) {
    return {
      status: "missing-target",
      workspace,
    };
  }

  if (placement === "selected") {
    return {
      status: "opened",
      workspace: assignTopicToPane(workspace, targetPane.id, topic),
    };
  }

  if (workspace.panes.length >= 4) {
    return {
      status: "pane-limit",
      workspace,
    };
  }

  const split = splitPane(workspace, targetPane.id, placement);
  if (split === workspace) {
    return {
      status: "missing-target",
      workspace,
    };
  }

  return {
    status: "opened",
    workspace: assignTopicToPane(split, split.selectedPaneId, topic),
  };
}

export function splitPane(
  workspace: WorkspaceState,
  paneId: number,
  direction: SplitDirection,
): WorkspaceState {
  if (workspace.panes.length >= 4) {
    return workspace;
  }

  const targetIndex = workspace.panes.findIndex((pane) => pane.id === paneId);
  if (targetIndex === -1) {
    return workspace;
  }

  const newPane = createEmptyPane(workspace.nextPaneId);
  const nextPanes = [...workspace.panes];
  const insertIndex = direction === "top" ? targetIndex : targetIndex + 1;
  nextPanes.splice(insertIndex, 0, newPane);

  let nextPaneId = workspace.nextPaneId + 1;
  if (nextPanes.length === 3) {
    nextPanes.push(createEmptyPane(nextPaneId));
    nextPaneId += 1;
  }

  return {
    ...workspace,
    layout: deriveLayout(nextPanes.length, direction),
    selectedPaneId: newPane.id,
    nextPaneId,
    panes: nextPanes,
  };
}

export function closePane(
  workspace: WorkspaceState,
  paneId: number,
): WorkspaceState {
  if (!workspace.panes.some((pane) => pane.id === paneId)) {
    return workspace;
  }

  const remainingPanes = workspace.panes.filter((pane) => pane.id !== paneId);
  const usefulPanes = remainingPanes.filter((pane) => !isPaneEmpty(pane));
  const panes = usefulPanes.length > 0 ? usefulPanes : remainingPanes;
  const selectedPaneId =
    panes.find((pane) => pane.id === workspace.selectedPaneId)?.id ??
    panes[0]?.id ??
    0;
  const selectedPane = panes.find((pane) => pane.id === selectedPaneId);

  return {
    ...workspace,
    layout: normalizeLayout(panes.length),
    selectedPaneId,
    expandedPaneId:
      workspace.expandedPaneId !== null &&
      panes.some((pane) => pane.id === workspace.expandedPaneId)
        ? workspace.expandedPaneId
        : null,
    selectedTopic: selectedPane?.topic ?? null,
    panes,
  };
}

export function stopPane(
  workspace: WorkspaceState,
  paneId: number,
): WorkspaceState {
  return updatePane(workspace, paneId, (pane) => ({
    ...pane,
    mode: "idle",
    status: "idle",
    session: null,
    error: null,
    activity: [],
    tone: "normal",
  }));
}

export function markPaneLoading(
  workspace: WorkspaceState,
  paneId: number,
  mode: TopicSessionPreviewRequest["mode"],
): WorkspaceState {
  const pane = workspace.panes.find((candidate) => candidate.id === paneId);
  if (!canStartPaneSession(pane)) {
    return workspace;
  }

  return updatePane(selectPane(workspace, paneId), paneId, (currentPane) => ({
    ...currentPane,
    mode,
    status: "loading",
    error: null,
    activity: [],
    tone: "normal",
  }));
}

export function appendPaneActivity(
  workspace: WorkspaceState,
  paneId: number,
  event: MilenaBoundaryEvent,
  limit = MAX_PANE_ACTIVITY,
): WorkspaceState {
  return updatePane(workspace, paneId, (pane) => {
    if (!shouldAppendPaneEvent(pane, event)) {
      return pane;
    }

    return {
      ...pane,
      activity: [event, ...pane.activity].slice(0, limit),
    };
  });
}

export function markPaneReady(
  workspace: WorkspaceState,
  paneId: number,
  session: TopicSessionPreview,
): WorkspaceState {
  return updatePane(workspace, paneId, (pane) => ({
    ...pane,
    session,
    status: "ready",
    mode: session.mode,
  }));
}

export function markPanePollingStarting(
  workspace: WorkspaceState,
  paneId: number,
  topic: string,
): WorkspaceState {
  if (!workspace.panes.some((pane) => pane.id === paneId)) {
    const id = paneId > 0 ? paneId : workspace.nextPaneId;
    const pane: WorkspacePane = {
      ...createEmptyPane(id),
      topic,
      mode: "poll",
      status: "loading",
    };

    return {
      ...workspace,
      layout: deriveLayout(workspace.panes.length + 1, "right"),
      selectedPaneId: id,
      selectedTopic: topic,
      nextPaneId: Math.max(workspace.nextPaneId, id + 1),
      panes: [...workspace.panes, pane],
    };
  }

  return updatePane(
    selectPane(selectTopicPreview(workspace, topic), paneId),
    paneId,
    (pane) => ({
      ...pane,
      topic,
      consumerGroup: null,
      mode: "poll",
      status: "loading",
      session: null,
      activity: [],
      error: null,
      tone: "normal",
    }),
  );
}

export function markPanePollingStarted(
  workspace: WorkspaceState,
  paneId: number,
  session: KafkaConsumerSession,
): WorkspaceState {
  return updatePane(workspace, paneId, (pane) => ({
    ...pane,
    topic: session.topics[0] ?? pane.topic,
    consumerGroup: session.groupId,
    mode: "poll",
    status: "ready",
    session,
    error: null,
    tone: "normal",
  }));
}

export function markPaneError(
  workspace: WorkspaceState,
  paneId: number,
  error: string,
): WorkspaceState {
  return updatePane(workspace, paneId, {
    error,
    status: "error",
    tone: "error",
  });
}

function updatePane(
  workspace: WorkspaceState,
  paneId: number,
  update: Partial<WorkspacePane> | ((pane: WorkspacePane) => WorkspacePane),
): WorkspaceState {
  let changed = false;
  const panes = workspace.panes.map((pane) => {
    if (pane.id !== paneId) {
      return pane;
    }

    changed = true;
    return typeof update === "function" ? update(pane) : { ...pane, ...update };
  });

  return changed ? { ...workspace, panes } : workspace;
}

function normalizeLayout(paneCount: number): WorkspaceLayout {
  if (paneCount <= 1) {
    return "single";
  }

  if (paneCount === 2) {
    return "two-right";
  }

  return "quad";
}

function shouldAppendPaneEvent(
  pane: WorkspacePane,
  event: MilenaBoundaryEvent,
): boolean {
  if (pane.status === "idle") {
    return false;
  }

  const eventSessionId = paneEventSessionId(event);
  if (pane.session?.sessionId && eventSessionId) {
    return pane.session.sessionId === eventSessionId;
  }

  return true;
}

function paneEventSessionId(event: MilenaBoundaryEvent): string | null {
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

function deriveLayout(
  paneCount: number,
  direction: SplitDirection,
): WorkspaceLayout {
  if (paneCount === 1) {
    return "single";
  }

  if (paneCount === 2) {
    return direction === "right" ? "two-right" : "two-top";
  }

  return "quad";
}
