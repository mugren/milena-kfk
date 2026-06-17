import {
  DockviewDefaultTab,
  DockviewReact,
  type DockviewApi,
  type DockviewReadyEvent,
  type IDockviewHeaderActionsProps,
  type IDockviewPanelHeaderProps,
  type IDockviewPanelProps,
  themeLight,
} from "dockview-react";
import React, {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
} from "react";
import "dockview-react/dist/styles/dockview.css";
import "./dockview-workspace.css";

type MoveDirection = "right" | "bottom";

interface PrototypeTab {
  id: string;
  topic: string;
  title: string;
  session: "idle" | "polling" | "paused";
  draft: string;
  messages: string[];
  openedAt: string;
  movedCount: number;
}

interface PrototypeGroup {
  id: string;
  tabIds: string[];
  activeTabId?: string;
  focused: boolean;
  maximized: boolean;
}

interface PrototypeWorkspace {
  groups: PrototypeGroup[];
  tabs: Record<string, PrototypeTab>;
  focusedGroupId?: string;
  selectedTopic: string;
  warning?: string;
  events: string[];
}

interface PrototypeContextValue {
  workspace: PrototypeWorkspace;
  appendMessage: (tabId: string) => void;
  closeTab: (tabId: string) => void;
  moveActiveTab: (direction: MoveDirection) => void;
  openTopic: (topic: string) => void;
  selectTopic: (topic: string) => void;
  syncFromDockview: () => void;
  toggleGroupMaximize: (groupId: string) => void;
  toggleSession: (tabId: string) => void;
  updateDraft: (tabId: string, draft: string) => void;
}

const MAX_GROUPS = 4;
const MAX_TABS_PER_GROUP = 8;
const TOPICS = [
  "orders.created",
  "orders.validated",
  "orders.enriched",
  "payments.authorized",
  "payments.failed",
  "inventory.reserved",
  "shipments.created",
  "audit.kafka.dead-letter",
];

let globalLastOpen: { topic: string; at: number } | null = null;

const PrototypeContext = createContext<PrototypeContextValue | null>(null);

function usePrototypeContext(): PrototypeContextValue {
  const value = useContext(PrototypeContext);
  if (!value) {
    throw new Error("Dockview workspace prototype context is missing");
  }
  return value;
}

function createInitialWorkspace(): PrototypeWorkspace {
  return {
    groups: [],
    tabs: {},
    selectedTopic: TOPICS[0],
    events: ["Prototype ready: open a topic to create the first group."],
  };
}

function addEvent(events: string[], event: string): string[] {
  return [event, ...events].slice(0, 12);
}

function nowLabel(): string {
  return new Date().toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function createTab(id: string, topic: string, title: string): PrototypeTab {
  return {
    id,
    topic,
    title,
    session: "polling",
    draft: JSON.stringify({ topic, tabId: id, prototype: true }, null, 2),
    messages: [
      `{ "topic": "${topic}", "offset": 1024, "tab": "${id}" }`,
      `{ "topic": "${topic}", "offset": 1025, "tab": "${id}" }`,
    ],
    openedAt: nowLabel(),
    movedCount: 0,
  };
}

function nextTitleForTopic(topic: string, tabs: Record<string, PrototypeTab>) {
  const usedTitles = new Set(Object.values(tabs).map((tab) => tab.title));
  if (!usedTitles.has(topic)) {
    return topic;
  }

  let suffix = 1;
  while (usedTitles.has(`${topic} (${suffix})`)) {
    suffix += 1;
  }
  return `${topic} (${suffix})`;
}

function findGroup(
  groups: PrototypeGroup[],
  groupId: string | undefined,
): PrototypeGroup | undefined {
  return groups.find((group) => group.id === groupId);
}

function countOpenTabs(
  api: DockviewApi | null,
  fallbackGroup?: PrototypeGroup,
): number {
  if (!api || !fallbackGroup) {
    return fallbackGroup?.tabIds.length ?? 0;
  }
  const group = api.groups.find((candidate) => candidate.id === fallbackGroup.id);
  return group?.panels.length ?? fallbackGroup.tabIds.length;
}

function overlap(
  firstStart: number,
  firstEnd: number,
  secondStart: number,
  secondEnd: number,
): number {
  return Math.max(0, Math.min(firstEnd, secondEnd) - Math.max(firstStart, secondStart));
}

function findAdjacentGroup(
  api: DockviewApi,
  sourceGroupId: string,
  direction: MoveDirection,
) {
  const source = api.groups.find((group) => group.id === sourceGroupId);
  if (!source) {
    return undefined;
  }

  const sourceRect = source.element.getBoundingClientRect();
  const candidates = api.groups
    .filter((group) => group.id !== sourceGroupId)
    .map((group) => ({
      group,
      rect: group.element.getBoundingClientRect(),
    }))
    .filter(({ rect }) => {
      if (direction === "right") {
        return (
          rect.left >= sourceRect.right - 6 &&
          overlap(sourceRect.top, sourceRect.bottom, rect.top, rect.bottom) > 12
        );
      }
      return (
        rect.top >= sourceRect.bottom - 6 &&
        overlap(sourceRect.left, sourceRect.right, rect.left, rect.right) > 12
      );
    })
    .sort((a, b) => {
      if (direction === "right") {
        return a.rect.left - b.rect.left;
      }
      return a.rect.top - b.rect.top;
    });

  return candidates[0]?.group;
}

function snapshotGroups(api: DockviewApi): PrototypeGroup[] {
  return api.groups.map((group) => ({
    id: group.id,
    tabIds: group.panels.map((panel) => panel.id),
    activeTabId: group.activePanel?.id,
    focused: api.activeGroup?.id === group.id,
    maximized: group.api.isMaximized(),
  }));
}

function DockviewTab(props: IDockviewPanelHeaderProps) {
  const { closeTab, workspace } = usePrototypeContext();
  const tab = workspace.tabs[props.api.id];

  return (
    <DockviewDefaultTab
      {...props}
      closeActionOverride={() => closeTab(props.api.id)}
      title={tab ? `${tab.topic} · ${tab.id}` : props.api.title}
    />
  );
}

function HeaderActions(props: IDockviewHeaderActionsProps) {
  const { moveActiveTab, toggleGroupMaximize, workspace } = usePrototypeContext();
  const group = findGroup(workspace.groups, props.group.id);
  const activeTab = props.activePanel ? workspace.tabs[props.activePanel.id] : undefined;
  const disableMove =
    !activeTab ||
    (workspace.groups.length === 1 && (group?.tabIds.length ?? 0) === 1);

  return (
    <div className="dockview-prototype-header-actions">
      <span className="dockview-prototype-header-context">
        {group?.tabIds.length ?? props.panels.length} tabs
      </span>
      <button
        type="button"
        disabled={disableMove}
        onClick={() => moveActiveTab("right")}
        title="Move active tab right"
      >
        Move right
      </button>
      <button
        type="button"
        disabled={disableMove}
        onClick={() => moveActiveTab("bottom")}
        title="Move active tab bottom"
      >
        Move bottom
      </button>
      <button
        type="button"
        onClick={() => toggleGroupMaximize(props.group.id)}
        title="Maximize or restore group"
      >
        {props.group.api.isMaximized() ? "Restore" : "Maximize"}
      </button>
    </div>
  );
}

function TopicPanel(props: IDockviewPanelProps) {
  const { appendMessage, toggleSession, updateDraft, workspace } =
    usePrototypeContext();
  const tab = workspace.tabs[props.api.id];

  if (!tab) {
    return (
      <div className="dockview-prototype-panel dockview-prototype-panel-empty">
        Tab state is missing for {props.api.id}
      </div>
    );
  }

  return (
    <div className="dockview-prototype-panel">
      <div className="dockview-prototype-panel-header">
        <div>
          <strong>{tab.title}</strong>
          <span>{tab.topic}</span>
        </div>
        <div className={`dockview-prototype-status ${tab.session}`}>
          {tab.session}
        </div>
      </div>

      <div className="dockview-prototype-panel-grid">
        <section>
          <div className="dockview-prototype-section-title">Mock Consumer</div>
          <div className="dockview-prototype-stream">
            {tab.messages.map((message, index) => (
              <div className="dockview-prototype-message" key={`${tab.id}-${index}`}>
                <span>partition 0 · offset {1024 + index}</span>
                <code>{message}</code>
              </div>
            ))}
          </div>
          <div className="dockview-prototype-toolbar">
            <button type="button" onClick={() => appendMessage(tab.id)}>
              Append mock
            </button>
            <button type="button" onClick={() => toggleSession(tab.id)}>
              Toggle session
            </button>
          </div>
        </section>

        <section>
          <div className="dockview-prototype-section-title">Per-Tab Draft</div>
          <textarea
            spellCheck={false}
            value={tab.draft}
            onChange={(event) => updateDraft(tab.id, event.currentTarget.value)}
          />
        </section>
      </div>
    </div>
  );
}

const DOCKVIEW_COMPONENTS: Record<
  string,
  React.FunctionComponent<IDockviewPanelProps>
> = {
  topicPanel: TopicPanel,
};

function EmptyWorkspace() {
  return (
    <div className="dockview-prototype-empty">
      <strong>Open a topic to start.</strong>
      <span>No placeholder group is created until an explicit open action.</span>
    </div>
  );
}

function LeftRail() {
  const { openTopic, selectTopic, workspace } = usePrototypeContext();

  return (
    <aside className="dockview-prototype-rail left">
      <div className="dockview-prototype-rail-header">
        <span>Environment</span>
        <strong>local-dev</strong>
      </div>
      <div className="dockview-prototype-section-title">Topics</div>
      <div className="dockview-prototype-topic-list">
        {TOPICS.map((topic) => (
          <div
            className={
              topic === workspace.selectedTopic
                ? "dockview-prototype-topic active"
                : "dockview-prototype-topic"
            }
            key={topic}
          >
            <button
              className="dockview-prototype-topic-name"
              type="button"
              onClick={() => selectTopic(topic)}
              title="Preview topic only"
            >
              <strong>{topic}</strong>
              <span>preview-only selection</span>
            </button>
            <button type="button" onClick={() => openTopic(topic)}>
              Open
            </button>
          </div>
        ))}
      </div>
    </aside>
  );
}

function RightRail() {
  const { workspace } = usePrototypeContext();
  const activeGroup = findGroup(workspace.groups, workspace.focusedGroupId);
  const activeTab = activeGroup?.activeTabId
    ? workspace.tabs[activeGroup.activeTabId]
    : undefined;
  const groupCount = workspace.groups.length;
  const tabCount = Object.keys(workspace.tabs).length;

  return (
    <aside className="dockview-prototype-rail right">
      <div className="dockview-prototype-rail-header">
        <span>Workspace</span>
        <strong>
          {groupCount} groups / {tabCount} tabs
        </strong>
      </div>

      {workspace.warning ? (
        <div className="dockview-prototype-warning">{workspace.warning}</div>
      ) : null}

      <section className="dockview-prototype-inspector">
        <div className="dockview-prototype-section-title">Active Tab</div>
        {activeTab ? (
          <dl>
            <dt>Title</dt>
            <dd>{activeTab.title}</dd>
            <dt>Topic</dt>
            <dd>{activeTab.topic}</dd>
            <dt>Tab id</dt>
            <dd>{activeTab.id}</dd>
            <dt>Group</dt>
            <dd>
              {workspace.focusedGroupId} · {activeGroup?.tabIds.length ?? 0} tabs
            </dd>
            <dt>Session</dt>
            <dd>{activeTab.session}</dd>
            <dt>Moves</dt>
            <dd>{activeTab.movedCount}</dd>
          </dl>
        ) : (
          <p>No active tab.</p>
        )}
      </section>

      <section>
        <div className="dockview-prototype-section-title">Canonical State</div>
        <pre>{JSON.stringify(workspace.groups, null, 2)}</pre>
      </section>

      <section>
        <div className="dockview-prototype-section-title">Activity</div>
        <div className="dockview-prototype-activity">
          {workspace.events.map((event, index) => (
            <div key={`${event}-${index}`}>{event}</div>
          ))}
        </div>
      </section>
    </aside>
  );
}

export default function DockviewWorkspacePrototype() {
  const apiRef = useRef<DockviewApi | null>(null);
  const tabIdRef = useRef(0);
  const [workspace, setWorkspace] = useState<PrototypeWorkspace>(
    createInitialWorkspace,
  );
  const workspaceRef = useRef(workspace);
  workspaceRef.current = workspace;

  const syncFromDockview = useCallback(() => {
    const api = apiRef.current;
    if (!api) {
      return;
    }

    const current = workspaceRef.current;
    const orphanPanels = api.groups
      .flatMap((group) => group.panels)
      .filter((panel) => !current.tabs[panel.id]);
    orphanPanels.forEach((panel) => panel.api.close());

    const nextWorkspace = {
      ...current,
      groups: snapshotGroups(api),
      focusedGroupId: api.activeGroup?.id,
      selectedTopic:
        api.activePanel && current.tabs[api.activePanel.id]
          ? current.tabs[api.activePanel.id].topic
          : current.selectedTopic,
      events:
        orphanPanels.length > 0
          ? addEvent(
              current.events,
              `Pruned ${orphanPanels.length} Dockview orphan panel`,
            )
          : current.events,
    };
    workspaceRef.current = nextWorkspace;
    setWorkspace(nextWorkspace);
  }, []);

  const selectTopic = useCallback((topic: string) => {
    setWorkspace((current) => ({
      ...current,
      selectedTopic: topic,
      warning: undefined,
    }));
  }, []);

  const warn = useCallback((message: string) => {
    setWorkspace((current) => ({
      ...current,
      warning: message,
      events: addEvent(current.events, message),
    }));
  }, []);

  const openTopic = useCallback(
    (topic: string) => {
      const api = apiRef.current;
      if (!api) {
        return;
      }

      const now = Date.now();
      const lastOpen = globalLastOpen;
      if (lastOpen?.topic === topic && now - lastOpen.at < 250) {
        return;
      }
      globalLastOpen = { topic, at: now };

      const current = workspaceRef.current;
      const focusedGroup =
        findGroup(current.groups, current.focusedGroupId) ?? current.groups[0];
      if (focusedGroup && countOpenTabs(api, focusedGroup) >= MAX_TABS_PER_GROUP) {
        setWorkspace({
          ...current,
          warning: "Max tabs opened in group",
          events: addEvent(current.events, "Max tabs opened in group"),
        });
        return;
      }

      const id = `tab-${++tabIdRef.current}`;
      const title = nextTitleForTopic(topic, current.tabs);
      const tab = createTab(id, topic, title);

      const options =
        focusedGroup && api.getGroup(focusedGroup.id)
          ? {
              id,
              title,
              component: "topicPanel",
              position: { referenceGroup: focusedGroup.id },
            }
          : { id, title, component: "topicPanel" };

      const nextWorkspace = {
        ...current,
        tabs: { ...current.tabs, [id]: tab },
        selectedTopic: topic,
        warning: undefined,
        events: addEvent(current.events, `Opened ${title} in focused group`),
      };
      workspaceRef.current = nextWorkspace;
      setWorkspace(nextWorkspace);

      api.addPanel(options);
      queueMicrotask(syncFromDockview);
      window.setTimeout(syncFromDockview, 0);
      window.setTimeout(syncFromDockview, 75);
      window.setTimeout(syncFromDockview, 250);
    },
    [syncFromDockview],
  );

  const closeTab = useCallback(
    (tabId: string) => {
      const api = apiRef.current;
      const tab = workspace.tabs[tabId];
      api?.getPanel(tabId)?.api.close();

      setWorkspace((current) => {
        const { [tabId]: closed, ...remainingTabs } = current.tabs;
        return {
          ...current,
          tabs: remainingTabs,
          warning: undefined,
          events: addEvent(
            current.events,
            `Closed ${tab?.title ?? closed?.title ?? tabId}; session stopped`,
          ),
        };
      });

      queueMicrotask(syncFromDockview);
    },
    [syncFromDockview, workspace.tabs],
  );

  const moveActiveTab = useCallback(
    (direction: MoveDirection) => {
      const api = apiRef.current;
      const panel = api?.activePanel;
      if (!api || !panel) {
        return;
      }

      const sourceGroup = panel.group;
      const sourceState = findGroup(workspace.groups, sourceGroup.id);
      if (workspace.groups.length === 1 && (sourceState?.tabIds.length ?? 0) === 1) {
        warn("Move disabled until another tab or group exists");
        return;
      }

      const adjacent = findAdjacentGroup(api, sourceGroup.id, direction);
      if (adjacent) {
        if (adjacent.panels.length >= MAX_TABS_PER_GROUP) {
          warn("Max tabs opened in group");
          return;
        }
        panel.api.moveTo({ group: adjacent, position: "center" });
      } else {
        if (api.groups.length >= MAX_GROUPS) {
          warn("Max groups opened");
          return;
        }
        panel.api.moveTo({
          group: sourceGroup,
          position: direction === "right" ? "right" : "bottom",
        });
      }

      setWorkspace((current) => {
        const tab = current.tabs[panel.id];
        return {
          ...current,
          tabs: tab
            ? {
                ...current.tabs,
                [panel.id]: { ...tab, movedCount: tab.movedCount + 1 },
              }
            : current.tabs,
          warning: undefined,
          events: addEvent(current.events, `Moved ${panel.title ?? panel.id} ${direction}`),
        };
      });

      queueMicrotask(syncFromDockview);
    },
    [syncFromDockview, warn, workspace.groups],
  );

  const toggleGroupMaximize = useCallback(
    (groupId: string) => {
      const api = apiRef.current;
      const group = api?.groups.find((candidate) => candidate.id === groupId);
      if (!group) {
        return;
      }

      if (group.api.isMaximized()) {
        group.api.exitMaximized();
      } else {
        group.api.maximize();
      }
      queueMicrotask(syncFromDockview);
    },
    [syncFromDockview],
  );

  const updateDraft = useCallback((tabId: string, draft: string) => {
    setWorkspace((current) => {
      const tab = current.tabs[tabId];
      if (!tab) {
        return current;
      }
      return {
        ...current,
        tabs: { ...current.tabs, [tabId]: { ...tab, draft } },
      };
    });
  }, []);

  const appendMessage = useCallback((tabId: string) => {
    setWorkspace((current) => {
      const tab = current.tabs[tabId];
      if (!tab) {
        return current;
      }
      const nextOffset = 1024 + tab.messages.length;
      return {
        ...current,
        tabs: {
          ...current.tabs,
          [tabId]: {
            ...tab,
            messages: [
              ...tab.messages,
              `{ "topic": "${tab.topic}", "offset": ${nextOffset}, "tab": "${tabId}" }`,
            ].slice(-1000),
          },
        },
      };
    });
  }, []);

  const toggleSession = useCallback((tabId: string) => {
    setWorkspace((current) => {
      const tab = current.tabs[tabId];
      if (!tab) {
        return current;
      }
      const nextSession = tab.session === "polling" ? "paused" : "polling";
      return {
        ...current,
        tabs: {
          ...current.tabs,
          [tabId]: { ...tab, session: nextSession },
        },
        events: addEvent(current.events, `${tab.title} session ${nextSession}`),
      };
    });
  }, []);

  const onReady = useCallback(
    (event: DockviewReadyEvent) => {
      apiRef.current = event.api;
      event.api.clear();
      setWorkspace(createInitialWorkspace());
      event.api.onDidActiveGroupChange(syncFromDockview);
      event.api.onDidActivePanelChange(syncFromDockview);
      event.api.onDidAddPanel(syncFromDockview);
      event.api.onDidRemovePanel(syncFromDockview);
      event.api.onDidMovePanel(syncFromDockview);
      event.api.onDidAddGroup(syncFromDockview);
      event.api.onDidRemoveGroup(syncFromDockview);
      event.api.onDidMaximizedGroupChange(syncFromDockview);
    },
    [syncFromDockview],
  );

  const contextValue = useMemo<PrototypeContextValue>(
    () => ({
      workspace,
      appendMessage,
      closeTab,
      moveActiveTab,
      openTopic,
      selectTopic,
      syncFromDockview,
      toggleGroupMaximize,
      toggleSession,
      updateDraft,
    }),
    [
      appendMessage,
      closeTab,
      moveActiveTab,
      openTopic,
      selectTopic,
      syncFromDockview,
      toggleGroupMaximize,
      toggleSession,
      updateDraft,
      workspace,
    ],
  );

  return (
    <PrototypeContext.Provider value={contextValue}>
      <div className="dockview-prototype-shell">
        <LeftRail />
        <main className="dockview-prototype-main">
          <div className="dockview-prototype-topbar">
            <div>
              <span>Throwaway Prototype</span>
              <strong>Dockview tabbed workspace</strong>
            </div>
            <div>
              Max {MAX_GROUPS} groups · Max {MAX_TABS_PER_GROUP} tabs/group ·
              Dockview owns visual layout
            </div>
          </div>
          <div className="dockview-prototype-workspace dockview-theme-light">
            {workspace.groups.length === 0 ? <EmptyWorkspace /> : null}
            <DockviewReact
              className={workspace.groups.length === 0 ? "is-empty" : undefined}
              components={DOCKVIEW_COMPONENTS}
              defaultTabComponent={DockviewTab}
              disableFloatingGroups
              getTabContextMenuItems={() => []}
              getTabGroupChipContextMenuItems={() => []}
              noPanelsOverlay="watermark"
              onDidDrop={syncFromDockview}
              onReady={onReady}
              onWillDrop={(event) => {
                if (event.position === "left" || event.position === "top") {
                  event.preventDefault();
                  warn("Only right and bottom split targets are supported");
                  return;
                }
                if (
                  event.position === "center" &&
                  (event.group?.panels.length ?? 0) >= MAX_TABS_PER_GROUP
                ) {
                  event.preventDefault();
                  warn("Max tabs opened in group");
                  return;
                }
                if (
                  (event.position === "right" || event.position === "bottom") &&
                  (apiRef.current?.groups.length ?? 0) >= MAX_GROUPS
                ) {
                  event.preventDefault();
                  warn("Max groups opened");
                }
              }}
              rightHeaderActionsComponent={HeaderActions}
              singleTabMode="default"
              tabGroupAccent="off"
              theme={themeLight}
            />
          </div>
        </main>
        <RightRail />
      </div>
    </PrototypeContext.Provider>
  );
}
