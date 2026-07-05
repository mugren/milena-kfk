import {
  applyWorkspaceGroupLayout,
  MAX_GROUPS,
  MAX_TABS_PER_GROUP,
} from "./workspace";
import type { WorkspaceState } from "./workspace";

export const DOCKVIEW_TOPIC_PANEL_COMPONENT = "topicPanel";

const GROUP_ID_PREFIX = "group-";
const PANEL_ID_PREFIX = "tab-";

// Keep this module pure and idempotent. React StrictMode can double-invoke
// render/setup paths; Dockview API mutations belong in the thin renderer layer.
export type DockviewWorkspaceGroup = {
  id: string;
  activePanelId: string;
  panelIds: string[];
};

export type DockviewWorkspacePanel = {
  id: string;
  tabId: number;
  title: string;
  component: string;
};

export type DockviewWorkspaceMapping = {
  groups: DockviewWorkspaceGroup[];
  panels: DockviewWorkspacePanel[];
  activeGroupId: string | null;
};

export type DockviewWorkspaceSnapshot = {
  groups: DockviewWorkspaceGroup[];
  activeGroupId?: string | null;
  activePanelId?: string | null;
};

export type DockviewSnapshotApiLike = {
  groups: {
    id: string;
    panels: { id: string }[];
    activePanel?: { id: string } | null;
  }[];
  activeGroup?: { id: string } | null;
  activePanel?: { id: string } | null;
};

export type DockviewReconcileResult = {
  status: "applied" | "rejected";
  workspace: WorkspaceState;
  prunePanelIds: string[];
  rejectedGroupIds: string[];
};

export type DockviewDropPosition =
  | "left"
  | "right"
  | "top"
  | "bottom"
  | "above"
  | "below"
  | "center";
export type DockviewDropReason =
  | "allowed"
  | "unsupported-position"
  | "unknown-panel"
  | "unknown-group"
  | "tab-limit"
  | "group-limit";

export type DockviewDropInput = {
  position: DockviewDropPosition;
  sourcePanelId?: string | null;
  sourceGroupId?: string | null;
  targetGroupId?: string | null;
};

export type DockviewWillDropEventLike = {
  position: DockviewDropPosition;
  panel?: { id: string } | null;
  group?: { id: string } | null;
  getData?: () =>
    | {
        groupId?: string | null;
        panelId?: string | null;
      }
    | undefined;
  preventDefault: () => void;
};

export type DockviewDropDecision =
  | { allow: true; reason: "allowed" }
  | { allow: false; reason: Exclude<DockviewDropReason, "allowed"> };

export type DockviewWorkspaceOptions = {
  disableFloatingGroups: true;
  getTabContextMenuItems: () => [];
  getTabGroupChipContextMenuItems: () => [];
};

export function toDockviewGroupId(groupId: number): string {
  return `${GROUP_ID_PREFIX}${groupId}`;
}

export function toDockviewPanelId(tabId: number): string {
  return `${PANEL_ID_PREFIX}${tabId}`;
}

export function fromDockviewGroupId(groupId: string): number | null {
  return fromDockviewId(groupId, GROUP_ID_PREFIX);
}

export function fromDockviewPanelId(panelId: string): number | null {
  return fromDockviewId(panelId, PANEL_ID_PREFIX);
}

export function mapWorkspaceToDockview(
  workspace: WorkspaceState,
  component = DOCKVIEW_TOPIC_PANEL_COMPONENT,
): DockviewWorkspaceMapping {
  return {
    groups: workspace.groups.map((group) => ({
      id: toDockviewGroupId(group.id),
      activePanelId: toDockviewPanelId(group.activeTabId),
      panelIds: group.tabs.map((tab) => toDockviewPanelId(tab.id)),
    })),
    panels: workspace.groups.flatMap((group) =>
      group.tabs.map((tab) => ({
        id: toDockviewPanelId(tab.id),
        tabId: tab.id,
        title: tab.title,
        component,
      })),
    ),
    activeGroupId: workspace.groups.some(
      (group) => group.id === workspace.focusedGroupId,
    )
      ? toDockviewGroupId(workspace.focusedGroupId)
      : null,
  };
}

export function snapshotDockviewApi(
  api: DockviewSnapshotApiLike,
): DockviewWorkspaceSnapshot {
  return {
    groups: api.groups.map((group) => ({
      id: group.id,
      activePanelId: group.activePanel?.id ?? group.panels[0]?.id ?? "",
      panelIds: group.panels.map((panel) => panel.id),
    })),
    activeGroupId: api.activeGroup?.id ?? null,
    activePanelId: api.activePanel?.id ?? null,
  };
}

export function reconcileDockviewSnapshot(
  workspace: WorkspaceState,
  snapshot: DockviewWorkspaceSnapshot,
): DockviewReconcileResult {
  const canonical = mapWorkspaceToDockview(workspace);
  const canonicalGroupIds = new Set(canonical.groups.map((group) => group.id));
  const canonicalPanelIds = new Set(canonical.panels.map((panel) => panel.id));
  const snapshotPanelIds = snapshot.groups.flatMap((group) => group.panelIds);
  const prunePanelIds = unique(
    snapshotPanelIds.filter((panelId) => !canonicalPanelIds.has(panelId)),
  );
  const rejectedGroupIds = unique(
    snapshot.groups
      .filter((group) => group.panelIds.every((panelId) => !canonicalPanelIds.has(panelId)))
      .map((group) => group.id),
  );

  if (
    prunePanelIds.length > 0 ||
    rejectedGroupIds.length > 0 ||
    !sameStringSet(snapshotPanelIds, [...canonicalPanelIds])
  ) {
    return {
      status: "rejected",
      workspace,
      prunePanelIds,
      rejectedGroupIds,
    };
  }

  const activePanelId = resolveActivePanelId(snapshot, canonicalPanelIds);
  const activeTabId =
    activePanelId !== null ? fromDockviewPanelId(activePanelId) : null;
  const resolvedGroups = resolveSnapshotGroups(workspace, snapshot.groups);
  const activeGroupId = resolveSnapshotActiveGroupId(
    snapshot.activeGroupId ?? null,
    resolvedGroups,
    canonicalGroupIds,
    activeTabId,
  );

  const layout = applyWorkspaceGroupLayout(
    workspace,
    resolvedGroups.map((group) => ({
      id: group.workspaceGroupId,
      activeTabId: fromDockviewPanelId(group.activePanelId),
      tabIds: group.tabIds,
    })),
    activeGroupId,
    activeTabId,
  );

  if (layout.status === "rejected") {
    return {
      status: "rejected",
      workspace,
      prunePanelIds: [],
      rejectedGroupIds: [],
    };
  }

  return {
    status: "applied",
    workspace: layout.workspace,
    prunePanelIds: [],
    rejectedGroupIds: [],
  };
}

export function getDockviewDropDecision(
  workspace: WorkspaceState,
  input: DockviewDropInput,
): DockviewDropDecision {
  const position = normalizeDropPosition(input.position);

  if (position === "left" || position === "top") {
    return { allow: false, reason: "unsupported-position" };
  }

  const sourceTabId = input.sourcePanelId
    ? fromDockviewPanelId(input.sourcePanelId)
    : null;
  const sourceGroupId = input.sourceGroupId
    ? fromDockviewGroupId(input.sourceGroupId)
    : null;
  const sourceGroup =
    sourceTabId !== null
      ? workspace.groups.find((group) =>
          group.tabs.some((tab) => tab.id === sourceTabId),
        )
      : workspace.groups.find((group) => group.id === sourceGroupId);
  if (!sourceGroup) {
    return { allow: false, reason: "unknown-panel" };
  }

  const targetGroupId = input.targetGroupId
    ? fromDockviewGroupId(input.targetGroupId)
    : null;
  const targetGroup = workspace.groups.find((group) => group.id === targetGroupId);
  if (!targetGroup) {
    return { allow: false, reason: "unknown-group" };
  }

  if (position === "center") {
    if (
      targetGroup.id !== sourceGroup.id &&
      targetGroup.tabs.length >= MAX_TABS_PER_GROUP
    ) {
      return { allow: false, reason: "tab-limit" };
    }

    return { allow: true, reason: "allowed" };
  }

  if (workspace.groups.length >= MAX_GROUPS) {
    return { allow: false, reason: "group-limit" };
  }

  return { allow: true, reason: "allowed" };
}

export function handleDockviewWillDrop(
  workspace: WorkspaceState,
  event: DockviewWillDropEventLike,
): DockviewDropDecision {
  const transfer = event.getData?.();
  const decision = getDockviewDropDecision(workspace, {
    position: event.position,
    sourcePanelId: transfer?.panelId ?? null,
    sourceGroupId: transfer?.groupId ?? null,
    targetGroupId: event.group?.id ?? null,
  });

  if (!decision.allow) {
    event.preventDefault();
  }

  return decision;
}

export function createDockviewWorkspaceOptions(): DockviewWorkspaceOptions {
  return {
    disableFloatingGroups: true,
    getTabContextMenuItems: () => [],
    getTabGroupChipContextMenuItems: () => [],
  };
}

function resolveActivePanelId(
  snapshot: DockviewWorkspaceSnapshot,
  canonicalPanelIds: Set<string>,
): string | null {
  if (snapshot.activePanelId && canonicalPanelIds.has(snapshot.activePanelId)) {
    return snapshot.activePanelId;
  }

  const activeGroup = snapshot.groups.find(
    (group) => group.id === snapshot.activeGroupId,
  );
  if (
    activeGroup?.activePanelId &&
    canonicalPanelIds.has(activeGroup.activePanelId)
  ) {
    return activeGroup.activePanelId;
  }

  return null;
}

type ResolvedDockviewSnapshotGroup = DockviewWorkspaceGroup & {
  workspaceGroupId: number | null;
  tabIds: number[];
};

function resolveSnapshotGroups(
  workspace: WorkspaceState,
  groups: DockviewWorkspaceGroup[],
): ResolvedDockviewSnapshotGroup[] {
  const usedGroupIds = new Set<number>();

  return groups.map((group) => {
    const tabIds = group.panelIds
      .map(fromDockviewPanelId)
      .filter((tabId): tabId is number => tabId !== null);
    const explicitGroupId = fromDockviewGroupId(group.id);
    const currentGroup =
      explicitGroupId !== null &&
      !usedGroupIds.has(explicitGroupId) &&
      workspace.groups.some((candidate) => candidate.id === explicitGroupId)
        ? workspace.groups.find((candidate) => candidate.id === explicitGroupId)
        : bestWorkspaceGroupMatch(workspace, tabIds, usedGroupIds);

    if (currentGroup) {
      usedGroupIds.add(currentGroup.id);
    }

    return {
      ...group,
      workspaceGroupId: currentGroup?.id ?? null,
      tabIds,
    };
  });
}

function bestWorkspaceGroupMatch(
  workspace: WorkspaceState,
  tabIds: number[],
  usedGroupIds: Set<number>,
): WorkspaceState["groups"][number] | undefined {
  const candidates = workspace.groups.filter(
    (group) => !usedGroupIds.has(group.id),
  );
  const exactMatch = candidates.find((group) =>
    sameNumberSet(
      group.tabs.map((tab) => tab.id),
      tabIds,
    ),
  );
  if (exactMatch) {
    return exactMatch;
  }

  let bestMatch:
    | { group: WorkspaceState["groups"][number]; overlap: number }
    | null = null;
  const tabIdSet = new Set(tabIds);
  for (const group of candidates) {
    const overlap = group.tabs.filter((tab) => tabIdSet.has(tab.id)).length;
    if (overlap > 0 && (!bestMatch || overlap > bestMatch.overlap)) {
      bestMatch = { group, overlap };
    }
  }

  return bestMatch?.group;
}

function resolveSnapshotActiveGroupId(
  activeGroupId: string | null,
  groups: ResolvedDockviewSnapshotGroup[],
  canonicalGroupIds: Set<string>,
  activeTabId: number | null,
): number | null {
  if (activeTabId !== null) {
    const activeTabGroup = groups.find((group) =>
      group.tabIds.includes(activeTabId),
    );
    if (activeTabGroup?.workspaceGroupId) {
      return activeTabGroup.workspaceGroupId;
    }
  }

  if (activeGroupId && canonicalGroupIds.has(activeGroupId)) {
    return fromDockviewGroupId(activeGroupId);
  }

  return (
    groups.find((group) => group.id === activeGroupId)?.workspaceGroupId ?? null
  );
}

function sameStringSet(first: string[], second: string[]): boolean {
  if (first.length !== second.length) {
    return false;
  }

  const values = new Set(first);
  return second.every((value) => values.has(value));
}

function sameNumberSet(first: number[], second: number[]): boolean {
  if (first.length !== second.length) {
    return false;
  }

  const values = new Set(first);
  return second.every((value) => values.has(value));
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

function normalizeDropPosition(
  position: DockviewDropPosition,
): "left" | "right" | "top" | "bottom" | "center" {
  if (position === "above") {
    return "top";
  }
  if (position === "below") {
    return "bottom";
  }
  return position;
}

function fromDockviewId(id: string, prefix: string): number | null {
  if (!id.startsWith(prefix)) {
    return null;
  }

  const numeric = Number(id.slice(prefix.length));
  return Number.isInteger(numeric) && numeric > 0 ? numeric : null;
}
