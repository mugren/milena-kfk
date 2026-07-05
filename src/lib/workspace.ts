import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  TopicSessionPreview,
  TopicSessionPreviewRequest,
} from "./tauri";

export type BoundaryStatus = "idle" | "loading" | "ready" | "error";
export type TabMode = "idle" | TopicSessionPreviewRequest["mode"];
export type TabTone = "normal" | "warning" | "error";
export type TabMoveDirection = "right" | "bottom";
export type SplitDirection = TabMoveDirection | "top";
export type TopicOpenPlacement = "selected" | SplitDirection;
export type TopicOpenStatus =
  | "opened"
  | "group-limit"
  | "tab-limit"
  | "pane-limit"
  | "missing-target";
export type WorkspaceActionStatus =
  | "moved"
  | "closed"
  | "group-limit"
  | "tab-limit"
  | "missing-target";
export type TopicOpenResult = {
  status: TopicOpenStatus;
  workspace: WorkspaceState;
};
export type WorkspaceActionResult = {
  status: WorkspaceActionStatus;
  workspace: WorkspaceState;
};
export type WorkspaceLayout = "single" | "two-right" | "two-top" | "quad";

export type WorkspaceTab = {
  id: number;
  topic: string | null;
  title: string;
  consumerGroup: string | null;
  mode: TabMode;
  status: BoundaryStatus;
  session: TopicSessionPreview | KafkaConsumerSession | null;
  activity: MilenaBoundaryEvent[];
  error: string | null;
  tone: TabTone;
};

export type WorkspaceGroup = {
  id: number;
  activeTabId: number;
  tabs: WorkspaceTab[];
};

export type WorkspaceState = {
  groups: WorkspaceGroup[];
  focusedGroupId: number;
  expandedGroupId: number | null;
  selectedTopic: string | null;
  nextGroupId: number;
  nextTabId: number;
  layout: WorkspaceLayout;
  selectedPaneId: number;
  expandedPaneId: number | null;
  nextPaneId: number;
  panes: WorkspaceTab[];
};

export type PaneMode = TabMode;
export type PaneTone = TabTone;
export type WorkspacePane = Omit<WorkspaceTab, "title"> &
  Partial<Pick<WorkspaceTab, "title">>;

export const MAX_GROUPS = 4;
export const MAX_TABS_PER_GROUP = 8;
export const MAX_PANE_ACTIVITY = 1_000;

export function createInitialWorkspaceState(): WorkspaceState {
  return withCompatibility({
    groups: [],
    focusedGroupId: 0,
    expandedGroupId: null,
    selectedTopic: null,
    nextGroupId: 1,
    nextTabId: 1,
  });
}

export function createEmptyTab(id: number): WorkspaceTab {
  return {
    id,
    topic: null,
    title: "",
    consumerGroup: null,
    mode: "idle",
    status: "idle",
    session: null,
    activity: [],
    error: null,
    tone: "normal",
  };
}

export const createEmptyPane = createEmptyTab;

export function isTabEmpty(tab: WorkspacePane): boolean {
  return (
    tab.topic === null &&
    tab.consumerGroup === null &&
    tab.mode === "idle" &&
    tab.status === "idle" &&
    tab.session === null &&
    tab.activity.length === 0 &&
    tab.error === null
  );
}

export const isPaneEmpty = isTabEmpty;

export function canStartTabSession(
  tab: WorkspacePane | undefined,
): tab is WorkspacePane & { topic: string } {
  return Boolean(tab?.topic);
}

export const canStartPaneSession = canStartTabSession;

export function getFocusedGroup(
  workspace: WorkspaceState,
): WorkspaceGroup | undefined {
  return (
    workspace.groups.find((group) => group.id === workspace.focusedGroupId) ??
    workspace.groups[0]
  );
}

export function getActiveTab(
  workspace: WorkspaceState,
): WorkspaceTab | undefined {
  const group = getFocusedGroup(workspace);
  return (
    group?.tabs.find((tab) => tab.id === group.activeTabId) ?? group?.tabs[0]
  );
}

export const getSelectedPane = getActiveTab;

export function selectGroup(
  workspace: WorkspaceState,
  groupId: number,
): WorkspaceState {
  if (!workspace.groups.some((group) => group.id === groupId)) {
    return workspace;
  }

  return withCompatibility({ ...workspace, focusedGroupId: groupId });
}

export function selectTab(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceState {
  const location = findTabLocation(workspace, tabId);
  if (!location) {
    return workspace;
  }

  const groups = workspace.groups.map((group) =>
    group.id === location.group.id ? { ...group, activeTabId: tabId } : group,
  );

  return withCompatibility({
    ...workspace,
    groups,
    focusedGroupId: location.group.id,
  });
}

export const selectPane = selectTab;

export function selectTopicPreview(
  workspace: WorkspaceState,
  topic: string,
): WorkspaceState {
  return withCompatibility({ ...workspace, selectedTopic: topic });
}

export function expandGroup(
  workspace: WorkspaceState,
  groupId: number,
): WorkspaceState {
  if (!workspace.groups.some((group) => group.id === groupId)) {
    return workspace;
  }

  return withCompatibility({
    ...workspace,
    focusedGroupId: groupId,
    expandedGroupId: groupId,
    selectedTopic: getGroupActiveTab(workspace.groups, groupId)?.topic ?? null,
  });
}

export function expandTab(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceState {
  const selected = selectTab(workspace, tabId);
  if (selected === workspace) {
    return workspace;
  }

  return withCompatibility({
    ...selected,
    expandedGroupId: selected.focusedGroupId,
    selectedTopic: getActiveTab(selected)?.topic ?? null,
  });
}

export const expandPane = expandTab;

export function restoreExpandedGroup(workspace: WorkspaceState): WorkspaceState {
  if (workspace.expandedGroupId === null) {
    return workspace;
  }

  return withCompatibility({
    ...workspace,
    expandedGroupId: null,
  });
}

export const restoreExpandedPane = restoreExpandedGroup;

export function openTopicInWorkspace(
  workspace: WorkspaceState,
  topic: string,
  placement: TopicOpenPlacement,
): TopicOpenResult {
  if (placement === "selected") {
    return openTopicInFocusedGroup(workspace, topic);
  }

  const opened = openTopicInFocusedGroup(workspace, topic);
  if (opened.status !== "opened") {
    return opened;
  }

  const moved = moveTabToAdjacentGroup(
    opened.workspace,
    opened.workspace.selectedPaneId,
    normalizeMoveDirection(placement),
  );

  if (moved.status === "moved") {
    return { status: "opened", workspace: moved.workspace };
  }

  if (
    moved.status === "group-limit" ||
    moved.status === "tab-limit" ||
    moved.status === "missing-target"
  ) {
    return { status: moved.status, workspace };
  }

  return { status: "missing-target", workspace };
}

export function openTopicInFocusedGroup(
  workspace: WorkspaceState,
  topic: string,
): TopicOpenResult {
  if (workspace.groups.length === 0) {
    const groupId = workspace.nextGroupId;
    const tab = createTopicTab(workspace, topic);

    return {
      status: "opened",
      workspace: withCompatibility({
        ...workspace,
        groups: [{ id: groupId, activeTabId: tab.id, tabs: [tab] }],
        focusedGroupId: groupId,
        selectedTopic: topic,
        nextGroupId: groupId + 1,
        nextTabId: tab.id + 1,
      }),
    };
  }

  const focusedGroup = getFocusedGroup(workspace);
  if (!focusedGroup) {
    return { status: "missing-target", workspace };
  }

  if (focusedGroup.tabs.length >= MAX_TABS_PER_GROUP) {
    return { status: "tab-limit", workspace };
  }

  const tab = createTopicTab(workspace, topic);
  const groups = workspace.groups.map((group) =>
    group.id === focusedGroup.id
      ? { ...group, activeTabId: tab.id, tabs: [...group.tabs, tab] }
      : group,
  );

  return {
    status: "opened",
    workspace: withCompatibility({
      ...workspace,
      groups,
      focusedGroupId: focusedGroup.id,
      selectedTopic: topic,
      nextTabId: tab.id + 1,
    }),
  };
}

export function assignTopicToTab(
  workspace: WorkspaceState,
  tabId: number,
  topic: string,
): WorkspaceState {
  if (workspace.groups.length === 0) {
    return openTopicInFocusedGroup(workspace, topic).workspace;
  }

  const location = findTabLocation(workspace, tabId);
  if (!location) {
    return workspace;
  }

  const title = nextTopicTitle(workspace, topic, tabId);
  return updateTab(
    selectTab(selectTopicPreview(workspace, topic), tabId),
    tabId,
    (tab) => ({
      ...createEmptyTab(tab.id),
      topic,
      title,
      consumerGroup: `milena-preview-${tab.id}`,
    }),
  );
}

export const assignTopicToPane = assignTopicToTab;

export function moveActiveTabToGroup(
  workspace: WorkspaceState,
  direction: TabMoveDirection,
): WorkspaceActionResult {
  const activeTab = getActiveTab(workspace);
  if (!activeTab) {
    return { status: "missing-target", workspace };
  }

  return moveTabToAdjacentGroup(workspace, activeTab.id, direction);
}

export function moveTabToAdjacentGroup(
  workspace: WorkspaceState,
  tabId: number,
  direction: TabMoveDirection,
): WorkspaceActionResult {
  const location = findTabLocation(workspace, tabId);
  if (!location) {
    return { status: "missing-target", workspace };
  }

  const targetIndex = location.groupIndex + 1;
  const existingTarget = workspace.groups[targetIndex];
  if (existingTarget && existingTarget.tabs.length >= MAX_TABS_PER_GROUP) {
    return { status: "tab-limit", workspace };
  }

  if (!existingTarget && workspace.groups.length >= MAX_GROUPS) {
    return { status: "group-limit", workspace };
  }

  const movingTab = location.tab;
  const sourceTabs = location.group.tabs.filter((tab) => tab.id !== tabId);
  const sourceActiveTabId = nearestActiveTabId(
    location.group.tabs,
    location.tabIndex,
  );
  const nextGroupId = existingTarget
    ? workspace.nextGroupId
    : workspace.nextGroupId + 1;
  const targetGroup =
    existingTarget ??
    ({
      id: workspace.nextGroupId,
      activeTabId: movingTab.id,
      tabs: [],
    } satisfies WorkspaceGroup);

  const groups = workspace.groups.flatMap((group, index) => {
    if (index === location.groupIndex) {
      return sourceTabs.length > 0
        ? [{ ...group, tabs: sourceTabs, activeTabId: sourceActiveTabId }]
        : [];
    }

    if (group.id === targetGroup.id) {
      return [
        {
          ...group,
          tabs: [...group.tabs, movingTab],
          activeTabId: movingTab.id,
        },
      ];
    }

    return [group];
  });

  if (!existingTarget) {
    const insertIndex =
      sourceTabs.length > 0 ? location.groupIndex + 1 : location.groupIndex;
    groups.splice(insertIndex, 0, {
      ...targetGroup,
      tabs: [movingTab],
      activeTabId: movingTab.id,
    });
  }

  return {
    status: "moved",
    workspace: withCompatibility({
      ...workspace,
      groups,
      focusedGroupId: targetGroup.id,
      selectedTopic: movingTab.topic,
      nextGroupId,
      expandedGroupId:
        workspace.expandedGroupId !== null &&
        groups.some((group) => group.id === workspace.expandedGroupId)
          ? workspace.expandedGroupId
          : null,
    }),
  };
}

export function splitPane(
  workspace: WorkspaceState,
  paneId: number,
  direction: SplitDirection,
): WorkspaceState {
  const result = moveTabToAdjacentGroup(
    workspace,
    paneId,
    normalizeMoveDirection(direction),
  );

  return result.status === "moved" ? result.workspace : workspace;
}

export function closeTab(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceActionResult {
  const location = findTabLocation(workspace, tabId);
  if (!location) {
    return { status: "missing-target", workspace };
  }

  const sourceTabs = location.group.tabs.filter((tab) => tab.id !== tabId);
  const activeTabId = nearestActiveTabId(location.group.tabs, location.tabIndex);
  const groups = workspace.groups.flatMap((group) => {
    if (group.id !== location.group.id) {
      return [group];
    }

    return sourceTabs.length > 0
      ? [{ ...group, tabs: sourceTabs, activeTabId }]
      : [];
  });
  const focusedGroupId =
    groups.find((group) => group.id === workspace.focusedGroupId)?.id ??
    groups[Math.max(0, location.groupIndex - 1)]?.id ??
    groups[0]?.id ??
    0;
  const focusedGroup = groups.find((group) => group.id === focusedGroupId);
  const activeTab = focusedGroup?.tabs.find(
    (tab) => tab.id === focusedGroup.activeTabId,
  );

  return {
    status: "closed",
    workspace: withCompatibility({
      ...workspace,
      groups,
      focusedGroupId,
      expandedGroupId:
        workspace.expandedGroupId !== null &&
        groups.some((group) => group.id === workspace.expandedGroupId)
          ? workspace.expandedGroupId
          : null,
      selectedTopic: activeTab?.topic ?? null,
    }),
  };
}

export function closePane(
  workspace: WorkspaceState,
  paneId: number,
): WorkspaceState {
  const result = closeTab(workspace, paneId);
  return result.status === "closed" ? result.workspace : workspace;
}

export type WorkspaceGroupLayoutInput = {
  id?: number | null;
  activeTabId?: number | null;
  tabIds: number[];
};

export type WorkspaceGroupLayoutResult = {
  status: "applied" | "rejected";
  workspace: WorkspaceState;
};

export function applyWorkspaceGroupLayout(
  workspace: WorkspaceState,
  groupInputs: WorkspaceGroupLayoutInput[],
  focusedGroupId?: number | null,
  activeTabId?: number | null,
): WorkspaceGroupLayoutResult {
  const currentTabs = workspace.groups.flatMap((group) => group.tabs);
  const tabsById = new Map(currentTabs.map((tab) => [tab.id, tab]));
  const seenTabIds = new Set<number>();
  const requestedTabIds = groupInputs.flatMap((group) => group.tabIds);

  if (requestedTabIds.length !== currentTabs.length) {
    return { status: "rejected", workspace };
  }

  for (const tabId of requestedTabIds) {
    if (seenTabIds.has(tabId) || !tabsById.has(tabId)) {
      return { status: "rejected", workspace };
    }
    seenTabIds.add(tabId);
  }

  const currentGroupIds = new Set(workspace.groups.map((group) => group.id));
  const usedGroupIds = new Set<number>();
  let nextGroupId = workspace.nextGroupId;
  const groups: WorkspaceGroup[] = [];

  for (const input of groupInputs) {
    const tabs = input.tabIds
      .map((tabId) => tabsById.get(tabId))
      .filter((tab): tab is WorkspaceTab => tab !== undefined);
    if (tabs.length === 0) {
      continue;
    }

    const requestedGroupId =
      input.id && currentGroupIds.has(input.id) && !usedGroupIds.has(input.id)
        ? input.id
        : null;
    const groupId = requestedGroupId ?? nextGroupId;
    if (requestedGroupId === null) {
      nextGroupId += 1;
    }
    usedGroupIds.add(groupId);

    const preferredActiveTabId =
      input.activeTabId && tabs.some((tab) => tab.id === input.activeTabId)
        ? input.activeTabId
        : activeTabId && tabs.some((tab) => tab.id === activeTabId)
          ? activeTabId
          : tabs[0].id;

    groups.push({
      id: groupId,
      activeTabId: preferredActiveTabId,
      tabs,
    });
  }

  const focusedGroup =
    (focusedGroupId
      ? groups.find((group) => group.id === focusedGroupId)
      : undefined) ??
    (activeTabId
      ? groups.find((group) => group.tabs.some((tab) => tab.id === activeTabId))
      : undefined) ??
    groups[0];
  const focusedTab =
    focusedGroup?.tabs.find((tab) => tab.id === focusedGroup.activeTabId) ??
    focusedGroup?.tabs[0];
  const nextExpandedGroupId =
    workspace.expandedGroupId !== null &&
    groups.some((group) => group.id === workspace.expandedGroupId)
      ? workspace.expandedGroupId
      : null;
  const nextSelectedTopic = focusedTab?.topic ?? null;

  if (
    workspaceGroupLayoutEquals(workspace.groups, groups) &&
    workspace.focusedGroupId === (focusedGroup?.id ?? 0) &&
    workspace.expandedGroupId === nextExpandedGroupId &&
    workspace.selectedTopic === nextSelectedTopic &&
    workspace.nextGroupId === Math.max(nextGroupId, workspace.nextGroupId)
  ) {
    return { status: "applied", workspace };
  }

  return {
    status: "applied",
    workspace: withCompatibility({
      ...workspace,
      groups,
      focusedGroupId: focusedGroup?.id ?? 0,
      expandedGroupId: nextExpandedGroupId,
      selectedTopic: nextSelectedTopic,
      nextGroupId: Math.max(nextGroupId, workspace.nextGroupId),
    }),
  };
}

export function stopTab(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceState {
  return updateTab(workspace, tabId, (tab) => ({
    ...tab,
    mode: "idle",
    status: "idle",
    session: null,
    error: null,
    tone: "normal",
  }));
}

export const stopPane = stopTab;

export function markTabLoading(
  workspace: WorkspaceState,
  tabId: number,
  mode: TopicSessionPreviewRequest["mode"],
): WorkspaceState {
  const tab = findTab(workspace, tabId);
  if (!canStartTabSession(tab)) {
    return workspace;
  }

  return updateTab(selectTab(workspace, tabId), tabId, (currentTab) => ({
    ...currentTab,
    mode,
    status: "loading",
    error: null,
    activity: [],
    tone: "normal",
  }));
}

export const markPaneLoading = markTabLoading;

export function appendTabActivity(
  workspace: WorkspaceState,
  tabId: number,
  event: MilenaBoundaryEvent,
  limit = MAX_PANE_ACTIVITY,
): WorkspaceState {
  return updateTab(workspace, tabId, (tab) => {
    if (!shouldAppendTabEvent(tab, event)) {
      return tab;
    }

    return {
      ...tab,
      activity: [event, ...tab.activity].slice(0, limit),
    };
  });
}

export const appendPaneActivity = appendTabActivity;

export function clearTabActivity(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceState {
  return updateTab(workspace, tabId, (tab) => ({
    ...tab,
    activity: [],
  }));
}

export const clearPaneActivity = clearTabActivity;

export function markTabReady(
  workspace: WorkspaceState,
  tabId: number,
  session: TopicSessionPreview,
): WorkspaceState {
  return updateTab(workspace, tabId, (tab) => ({
    ...tab,
    session,
    status: "ready",
    mode: session.mode,
  }));
}

export const markPaneReady = markTabReady;

export function markTabPollingStarting(
  workspace: WorkspaceState,
  tabId: number,
  topic: string,
  consumerGroup: string | null = null,
): WorkspaceState {
  if (!findTab(workspace, tabId)) {
    if (workspace.groups.length === 0) {
      const tab: WorkspaceTab = {
        ...createEmptyTab(tabId > 0 ? tabId : workspace.nextTabId),
        topic,
        title: nextTopicTitle(workspace, topic),
        consumerGroup,
        mode: "poll",
        status: "loading",
      };
      const groupId = workspace.nextGroupId;

      return withCompatibility({
        ...workspace,
        groups: [{ id: groupId, activeTabId: tab.id, tabs: [tab] }],
        focusedGroupId: groupId,
        selectedTopic: topic,
        nextGroupId: groupId + 1,
        nextTabId: Math.max(workspace.nextTabId, tab.id + 1),
      });
    }

    const focusedGroup = getFocusedGroup(workspace);
    if (!focusedGroup || focusedGroup.tabs.length >= MAX_TABS_PER_GROUP) {
      return workspace;
    }

    const tab: WorkspaceTab = {
      ...createEmptyTab(tabId > 0 ? tabId : workspace.nextTabId),
      topic,
      title: nextTopicTitle(workspace, topic),
      consumerGroup,
      mode: "poll",
      status: "loading",
    };
    const groups = workspace.groups.map((group) =>
      group.id === focusedGroup.id
        ? { ...group, activeTabId: tab.id, tabs: [...group.tabs, tab] }
        : group,
    );

    return withCompatibility({
      ...workspace,
      groups,
      focusedGroupId: focusedGroup.id,
      selectedTopic: topic,
      nextTabId: Math.max(workspace.nextTabId, tab.id + 1),
    });
  }

  return updateTab(
    selectTab(selectTopicPreview(workspace, topic), tabId),
    tabId,
    (tab) => ({
      ...tab,
      topic,
      title: nextTopicTitle(workspace, topic, tabId),
      consumerGroup,
      mode: "poll",
      status: "loading",
      session: null,
      activity: [],
      error: null,
      tone: "normal",
    }),
  );
}

export const markPanePollingStarting = markTabPollingStarting;

export function markTabPollingStarted(
  workspace: WorkspaceState,
  tabId: number,
  session: KafkaConsumerSession,
): WorkspaceState {
  return updateTab(workspace, tabId, (tab) => ({
    ...tab,
    topic: session.topics[0] ?? tab.topic,
    consumerGroup: session.groupId,
    mode: "poll",
    status: "ready",
    session,
    error: null,
    tone: "normal",
  }));
}

export const markPanePollingStarted = markTabPollingStarted;

export function markTabError(
  workspace: WorkspaceState,
  tabId: number,
  error: string,
): WorkspaceState {
  return updateTab(workspace, tabId, {
    error,
    status: "error",
    tone: "error",
  });
}

export const markPaneError = markTabError;

function createTopicTab(
  workspace: WorkspaceState,
  topic: string,
): WorkspaceTab {
  const id = workspace.nextTabId;
  return {
    ...createEmptyTab(id),
    topic,
    title: nextTopicTitle(workspace, topic),
    consumerGroup: `milena-preview-${id}`,
  };
}

function updateTab(
  workspace: WorkspaceState,
  tabId: number,
  update: Partial<WorkspaceTab> | ((tab: WorkspaceTab) => WorkspaceTab),
): WorkspaceState {
  let changed = false;
  const groups = workspace.groups.map((group) => {
    const tabs = group.tabs.map((tab) => {
      if (tab.id !== tabId) {
        return tab;
      }

      changed = true;
      return typeof update === "function" ? update(tab) : { ...tab, ...update };
    });

    return changed && group.tabs.some((tab) => tab.id === tabId)
      ? { ...group, tabs }
      : group;
  });

  return changed ? withCompatibility({ ...workspace, groups }) : workspace;
}

function findTab(
  workspace: WorkspaceState,
  tabId: number,
): WorkspaceTab | undefined {
  return findTabLocation(workspace, tabId)?.tab;
}

function findTabLocation(
  workspace: WorkspaceState,
  tabId: number,
):
  | {
      group: WorkspaceGroup;
      groupIndex: number;
      tab: WorkspaceTab;
      tabIndex: number;
    }
  | undefined {
  for (const [groupIndex, group] of workspace.groups.entries()) {
    const tabIndex = group.tabs.findIndex((tab) => tab.id === tabId);
    if (tabIndex !== -1) {
      return {
        group,
        groupIndex,
        tab: group.tabs[tabIndex],
        tabIndex,
      };
    }
  }

  return undefined;
}

function workspaceGroupLayoutEquals(
  first: WorkspaceGroup[],
  second: WorkspaceGroup[],
): boolean {
  return (
    first.length === second.length &&
    first.every((group, index) => {
      const other = second[index];
      return (
        other !== undefined &&
        group.id === other.id &&
        group.activeTabId === other.activeTabId &&
        group.tabs.length === other.tabs.length &&
        group.tabs.every((tab, tabIndex) => tab.id === other.tabs[tabIndex]?.id)
      );
    })
  );
}

function getGroupActiveTab(
  groups: WorkspaceGroup[],
  groupId: number,
): WorkspaceTab | undefined {
  const group = groups.find((candidate) => candidate.id === groupId);
  return group?.tabs.find((tab) => tab.id === group.activeTabId);
}

function nearestActiveTabId(tabs: WorkspaceTab[], removedIndex: number): number {
  return tabs[removedIndex - 1]?.id ?? tabs[removedIndex + 1]?.id ?? 0;
}

function nextTopicTitle(
  workspace: WorkspaceState,
  topic: string,
  replacingTabId?: number,
): string {
  const usedSuffixes = new Set<number>();
  for (const tab of workspace.groups.flatMap((group) => group.tabs)) {
    if (tab.id === replacingTabId || tab.topic !== topic) {
      continue;
    }

    usedSuffixes.add(titleSuffix(tab.title, topic));
  }

  let suffix = 1;
  while (usedSuffixes.has(suffix)) {
    suffix += 1;
  }

  return suffix === 1 ? topic : `${topic} (${suffix})`;
}

function titleSuffix(title: string, topic: string): number {
  if (title === topic) {
    return 1;
  }

  const escapedTopic = topic.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = title.match(new RegExp(`^${escapedTopic} \\((\\d+)\\)$`));
  return match ? Number(match[1]) : 1;
}

function withCompatibility(
  workspace: Omit<
    WorkspaceState,
    "layout" | "selectedPaneId" | "expandedPaneId" | "nextPaneId" | "panes"
  > &
    Partial<
      Pick<
        WorkspaceState,
        "layout" | "selectedPaneId" | "expandedPaneId" | "nextPaneId" | "panes"
      >
    >,
): WorkspaceState {
  const focusedGroup =
    workspace.groups.find((group) => group.id === workspace.focusedGroupId) ??
    workspace.groups[0];
  const activeTab =
    focusedGroup?.tabs.find((tab) => tab.id === focusedGroup.activeTabId) ??
    focusedGroup?.tabs[0];
  const expandedGroup = workspace.expandedGroupId
    ? workspace.groups.find((group) => group.id === workspace.expandedGroupId)
    : undefined;

  return {
    ...workspace,
    layout: normalizeLayout(workspace.groups.length),
    selectedPaneId: activeTab?.id ?? 0,
    expandedPaneId:
      expandedGroup?.activeTabId ??
      (workspace.expandedGroupId === null ? null : activeTab?.id ?? null),
    nextPaneId: workspace.nextTabId,
    panes: workspace.groups.flatMap((group) => group.tabs),
  };
}

function normalizeLayout(groupCount: number): WorkspaceLayout {
  if (groupCount <= 1) {
    return "single";
  }

  if (groupCount === 2) {
    return "two-right";
  }

  return "quad";
}

function normalizeMoveDirection(direction: SplitDirection): TabMoveDirection {
  return direction === "bottom" ? "bottom" : "right";
}

function shouldAppendTabEvent(
  tab: WorkspaceTab,
  event: MilenaBoundaryEvent,
): boolean {
  if (tab.status === "idle") {
    return false;
  }

  const eventSessionId = tabEventSessionId(event);
  if (tab.session?.sessionId && eventSessionId) {
    return tab.session.sessionId === eventSessionId;
  }

  return true;
}

function tabEventSessionId(event: MilenaBoundaryEvent): string | null {
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
