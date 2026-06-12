import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import {
  ChevronDown,
  ChevronUp,
  Maximize2,
  Minimize2,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
  Pin,
  PinOff,
  Play,
  SplitSquareHorizontal,
  SplitSquareVertical,
  Square,
  X,
} from "lucide-react";
import {
  AppState,
  RuntimeAuthConfig,
  listKafkaTopics,
  loadAppState,
  publishKafkaRecord,
  startKafkaConsumerSession,
  stopKafkaConsumerSession,
  type MilenaBoundaryEvent,
} from "./lib/tauri";
import {
  appendBoundaryEventActivity,
  appendGlobalError,
  clearGlobalActivity,
  type GlobalActivityEntry,
  type GlobalActivitySource,
} from "./lib/activity";
import {
  closePanePollingSession,
  getPanePollingSessionId,
  startPanePollingSession,
  stopPanePollingSession,
} from "./lib/polling";
import {
  readMessageRenderPreferences,
  renderKafkaRecord,
  setTopicMessageRenderMode,
  topicMessageRenderMode,
  type MessageRenderMode,
  type MessageRenderPreferences,
  type MessageRenderPreferenceStore,
  type RenderedKafkaRecord,
} from "./lib/messages";
import {
  canSendPublisherRecord,
  createInitialPublisherState,
  formatPublisherPayload,
  getPublisherPaneState,
  producerAckLabel,
  sendPublisherRecord,
  setPublisherKey,
  setPublisherPayload,
  validatePublisherPayload,
  type PublisherPaneState,
  type PublisherState,
} from "./lib/publisher";
import {
  createInitialTopicRailState,
  environmentKey,
  markTopicLoadFailed,
  markTopicLoadStarted,
  markTopicLoadSucceeded,
  openTopicEnvironment,
  setTopicSearch,
  toggleTopicPin,
  visibleTopicRows,
  type TopicPinStore,
} from "./lib/topics";
import {
  canStartPaneSession,
  createInitialWorkspaceState,
  expandPane as expandWorkspacePaneState,
  getSelectedPane,
  isPaneEmpty,
  openTopicInWorkspace,
  restoreExpandedPane,
  selectPane as selectWorkspacePane,
  selectTopicPreview,
  splitPane as splitWorkspacePane,
  type SplitDirection,
  type TopicOpenPlacement,
  type WorkspacePane,
  type WorkspaceState,
} from "./lib/workspace";

type TopicMenuState = {
  topic: string;
  x: number;
  y: number;
} | null;

type TopicPreviewModel = {
  name: string;
  partitionCount: number | null;
  renderMode: MessageRenderMode;
};

type VisibleColumns = {
  topics: boolean;
  inspector: boolean;
};

const APP_SHELL_NAME = "Milena - Kafka Reader";

const activeRuntimeAuth: RuntimeAuthConfig = {
  environment: "local-dev",
  brokers: ["localhost:19092"],
  properties: {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "milena_plain",
    "sasl.password": "milena-plain-secret",
    "ssl.ca.location": "docker/kafka/generated/ssl/ca.crt",
    "ssl.endpoint.identification.algorithm": "https",
  },
};

function App() {
  const [appState, setAppState] = useState<AppState | null>(null);
  const [workspace, setWorkspace] = useState(createInitialWorkspaceState);
  const [topicRail, setTopicRail] = useState(() =>
    createInitialTopicRailState(getTopicPinStore()),
  );
  const [messageRenderPreferences, setMessageRenderPreferences] = useState(() =>
    readMessageRenderPreferences(getMessageRenderPreferenceStore()),
  );
  const [publisherState, setPublisherState] = useState(
    createInitialPublisherState,
  );
  const [activity, setActivity] = useState<GlobalActivityEntry[]>([]);
  const [topicMenu, setTopicMenu] = useState<TopicMenuState>(null);
  const [visibleColumns, setVisibleColumns] = useState<VisibleColumns>({
    topics: true,
    inspector: true,
  });
  const requestedTopicLoads = useRef(new Set<string>());
  const workspaceRef = useRef(workspace);
  const pollingRuns = useRef(new Map<number, number>());

  useEffect(() => {
    document.title = APP_SHELL_NAME;
  }, []);

  useEffect(() => {
    loadAppState()
      .then(setAppState)
      .catch((cause: unknown) => {
        const message =
          cause instanceof Error ? cause.message : "Tauri runtime unavailable";
        recordGlobalError("app", "Tauri runtime unavailable", message);
      });
  }, []);

  useEffect(() => {
    workspaceRef.current = workspace;
  }, [workspace]);

  useEffect(() => {
    if (!topicMenu) {
      return;
    }

    function closeTopicMenu() {
      setTopicMenu(null);
    }

    window.addEventListener("click", closeTopicMenu);
    window.addEventListener("keydown", closeTopicMenu);
    return () => {
      window.removeEventListener("click", closeTopicMenu);
      window.removeEventListener("keydown", closeTopicMenu);
    };
  }, [topicMenu]);

  const activePane = useMemo(
    () => getSelectedPane(workspace),
    [workspace],
  );
  const expandedPane = useMemo(
    () =>
      workspace.expandedPaneId === null
        ? null
        : workspace.panes.find((pane) => pane.id === workspace.expandedPaneId) ??
          null,
    [workspace.expandedPaneId, workspace.panes],
  );
  const visiblePanes = expandedPane ? [expandedPane] : workspace.panes;
  const visibleLayout = expandedPane ? "single" : workspace.layout;

  const activeTopic = activePane?.topic ?? null;
  const previewTopic = workspace.selectedTopic ?? activeTopic;
  const activeEnvironmentKey = environmentKey(activeRuntimeAuth);
  const topicRows = useMemo(() => visibleTopicRows(topicRail), [topicRail]);
  const topicPreview = useMemo<TopicPreviewModel | null>(() => {
    if (!workspace.selectedTopic || workspace.selectedTopic === activePane?.topic) {
      return null;
    }

    const metadata = topicRail.topics.find(
      (topic) => topic.name === workspace.selectedTopic,
    );

    return {
      name: workspace.selectedTopic,
      partitionCount: metadata?.partitionCount ?? null,
      renderMode: topicMessageRenderMode(
        messageRenderPreferences,
        activeEnvironmentKey,
        workspace.selectedTopic,
      ),
    };
  }, [
    activeEnvironmentKey,
    activePane?.topic,
    messageRenderPreferences,
    topicRail.topics,
    workspace.selectedTopic,
  ]);

  function selectTopic(topic: string) {
    updateWorkspace((current) => selectTopicPreview(current, topic));
  }

  async function loadTopicList(force: boolean) {
    const key = environmentKey(activeRuntimeAuth);
    if (!force && requestedTopicLoads.current.has(key)) {
      setTopicRail((current) => openTopicEnvironment(current, key));
      return;
    }

    requestedTopicLoads.current.add(key);
    setTopicRail((current) => markTopicLoadStarted(current, key));

    try {
      const topicList = await listKafkaTopics({ auth: activeRuntimeAuth });
      setTopicRail((current) =>
        markTopicLoadSucceeded(current, key, topicList.topics),
      );
    } catch (cause) {
      const message =
        cause instanceof Error ? cause.message : "Topic list refresh failed";
      setTopicRail((current) => markTopicLoadFailed(current, key, message));
      recordGlobalError("topics", "Topic list refresh failed", message);
    }
  }

  function refreshTopicList() {
    void loadTopicList(true);
  }

  function pinTopic(topic: string) {
    setTopicRail((current) =>
      toggleTopicPin(current, activeEnvironmentKey, topic, getTopicPinStore()),
    );
  }

  function setMessageRenderMode(topic: string, mode: MessageRenderMode) {
    setMessageRenderPreferences((current) =>
      setTopicMessageRenderMode(
        current,
        activeEnvironmentKey,
        topic,
        mode,
        getMessageRenderPreferenceStore(),
      ),
    );
  }

  function startPolling(paneId: number, topic: string) {
    const run = nextPollingRun(paneId);
    setTopicMenu(null);
    return startPanePollingSession({
      paneId,
      topic,
      auth: activeRuntimeAuth,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      startConsumerSession: startKafkaConsumerSession,
      stopConsumerSession: stopKafkaConsumerSession,
      isCurrent: () => pollingRuns.current.get(paneId) === run,
      onEvent: (event) => {
        setActivity((current) =>
          appendBoundaryEventActivity(current, event, paneId),
        );
      },
      onError: (message) =>
        recordGlobalError("consumer", "Consumer session failed", message, paneId),
      onStopError: (message) =>
        recordGlobalError("consumer", "Consumer cleanup failed", message, paneId),
    });
  }

  function splitPane(paneId: number, direction: SplitDirection) {
    updateWorkspace((current) => splitWorkspacePane(current, paneId, direction));
  }

  function expandPane(paneId: number) {
    updateWorkspace((current) => expandWorkspacePaneState(current, paneId));
  }

  function restorePane() {
    updateWorkspace(restoreExpandedPane);
  }

  function openTopic(topic: string, placement: TopicOpenPlacement) {
    setTopicMenu(null);
    const current = workspaceRef.current;
    const replacedPane = placement === "selected" ? getSelectedPane(current) : null;
    const replacedSessionId = replacedPane
      ? getPanePollingSessionId(current, replacedPane.id)
      : null;

    if (replacedPane) {
      nextPollingRun(replacedPane.id);
    }

    const result = openTopicInWorkspace(current, topic, placement);
    workspaceRef.current = result.workspace;
    setWorkspace(result.workspace);

    if (result.status === "pane-limit") {
      recordGlobalError(
        "topics",
        "Pane limit reached",
        "Milena supports up to four open panes.",
      );
    }

    if (replacedSessionId && replacedPane) {
      void stopKafkaConsumerSession({ sessionId: replacedSessionId }).catch(
        (cause: unknown) => {
          const message =
            cause instanceof Error ? cause.message : "Kafka consumer cleanup failed";
          recordGlobalError(
            "consumer",
            "Consumer cleanup failed",
            message,
            replacedPane.id,
          );
        },
      );
    }
  }

  function stopPane(paneId: number) {
    nextPollingRun(paneId);
    void stopPanePollingSession({
      paneId,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      stopConsumerSession: stopKafkaConsumerSession,
      onStopError: (message) =>
        recordGlobalError("consumer", "Consumer cleanup failed", message, paneId),
    });
  }

  function closeWorkspacePane(paneId: number) {
    nextPollingRun(paneId);
    void closePanePollingSession({
      paneId,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      stopConsumerSession: stopKafkaConsumerSession,
      onStopError: (message) =>
        recordGlobalError("consumer", "Consumer cleanup failed", message, paneId),
    });
  }

  function updateWorkspace(update: (current: WorkspaceState) => WorkspaceState) {
    setWorkspace((current) => {
      const next = update(current);
      workspaceRef.current = next;
      return next;
    });
  }

  function updatePublisherState(
    update: (current: PublisherState) => PublisherState,
  ) {
    setPublisherState(update);
  }

  function changePublisherPayload(
    paneId: number,
    topic: string | null,
    payload: string,
  ) {
    setPublisherState((current) =>
      setPublisherPayload(current, paneId, payload, topic),
    );
  }

  function changePublisherKey(
    paneId: number,
    topic: string | null,
    key: string,
  ) {
    setPublisherState((current) => setPublisherKey(current, paneId, key, topic));
  }

  function formatPanePublisherPayload(paneId: number, topic: string | null) {
    setPublisherState((current) =>
      formatPublisherPayload(current, paneId, topic),
    );
  }

  function sendPanePublisherRecord(paneId: number) {
    void sendPublisherRecord({
      paneId,
      auth: activeRuntimeAuth,
      getWorkspace: () => workspaceRef.current,
      getPublisherState: () => publisherState,
      updatePublisherState,
      publishRecord: publishKafkaRecord,
      onError: (message) =>
        recordGlobalError("producer", "Publish failed", message, paneId),
    });
  }

  function nextPollingRun(paneId: number) {
    const next = (pollingRuns.current.get(paneId) ?? 0) + 1;
    pollingRuns.current.set(paneId, next);
    return next;
  }

  function openTopicMenu(event: MouseEvent<HTMLElement>, topic: string) {
    event.preventDefault();
    event.stopPropagation();
    setTopicMenu({ topic, x: event.clientX, y: event.clientY });
  }

  function recordGlobalError(
    source: GlobalActivitySource,
    message: string,
    detail: string,
    paneId: number | null = null,
  ) {
    setActivity((current) =>
      appendGlobalError(current, source, message, { detail, paneId }),
    );
  }

  function clearActivityLog() {
    setActivity(clearGlobalActivity());
  }

  function setColumnVisibility(column: keyof VisibleColumns, visible: boolean) {
    setVisibleColumns((current) => ({
      ...current,
      [column]: visible,
    }));
  }

  return (
    <main
      className={[
        "app-shell",
        visibleColumns.topics ? "" : "topics-hidden",
        visibleColumns.inspector ? "" : "inspector-hidden",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {visibleColumns.topics ? (
        <aside className="rail rail-left" aria-label="Kafka topics">
          <header className="rail-header">
            <div className="rail-title">
              <strong>{APP_SHELL_NAME}</strong>
              <small>{activeRuntimeAuth.environment}</small>
            </div>
            <div className="rail-header-actions">
              <span className="status-pill">local</span>
              <button
                className="rail-toggle-button"
                type="button"
                aria-label="Hide topic sidebar"
                title="Hide topic sidebar"
                onClick={() => setColumnVisibility("topics", false)}
              >
                <PanelLeftClose aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            </div>
          </header>
          <section className="environment-summary">
            <span>Cluster</span>
            <button
              className="refresh-button"
              type="button"
              disabled={topicRail.status === "loading"}
              onClick={refreshTopicList}
            >
              Refresh
            </button>
            <strong>{activeRuntimeAuth.brokers.join(", ")}</strong>
            <small>
              {topicRailStatus(topicRail.status, topicRail.topics.length)}
            </small>
          </section>
          <div className="topic-search">
            <input
              aria-label="Search topics"
              placeholder="Search topics"
              type="search"
              value={topicRail.searchQuery}
              onChange={(event) => {
                const value = event.currentTarget.value;
                setTopicRail((current) =>
                  setTopicSearch(current, value),
                );
              }}
            />
          </div>
          {topicRail.error ? (
            <div className="error-strip compact">{topicRail.error}</div>
          ) : null}
          <nav className="topic-list">
            {topicRows.length === 0 ? (
              <p className="topic-empty">
                {topicRail.status === "loading" ? "Loading topics" : "No topics"}
              </p>
            ) : (
              topicRows.map((topic) => {
                const selected = topic.name === previewTopic;
                const pinAction = topic.pinned ? "Unpin" : "Pin";
                const renderMode = topicMessageRenderMode(
                  messageRenderPreferences,
                  activeEnvironmentKey,
                  topic.name,
                );

                return (
                  <div
                    className={[
                      "topic",
                      selected ? "active" : "",
                      topic.pinned ? "pinned" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    data-pinned={topic.pinned}
                    data-selected={selected}
                    key={topic.name}
                    onContextMenu={(event) => openTopicMenu(event, topic.name)}
                  >
                    <button
                      className="topic-select"
                      type="button"
                      aria-current={selected ? "true" : undefined}
                      onClick={() => selectTopic(topic.name)}
                      title={topic.name}
                    >
                      <span className="topic-marker" />
                      <span className="topic-copy">
                        <strong>{topic.name}</strong>
                        <small>
                          {topic.partitionCount} partitions / {renderMode}
                        </small>
                      </span>
                    </button>
                    <button
                      className="topic-open"
                      type="button"
                      aria-haspopup="menu"
                      aria-label={`Open actions for ${topic.name}`}
                      onClick={(event) => openTopicMenu(event, topic.name)}
                    >
                      Open
                    </button>
                    <button
                      className="topic-pin"
                      type="button"
                      aria-label={`${pinAction} ${topic.name}`}
                      aria-pressed={topic.pinned}
                      onClick={() => pinTopic(topic.name)}
                      title={`${pinAction} ${topic.name}`}
                    >
                      {topic.pinned ? (
                        <PinOff aria-hidden="true" size={14} strokeWidth={1.9} />
                      ) : (
                        <Pin aria-hidden="true" size={14} strokeWidth={1.9} />
                      )}
                    </button>
                  </div>
                );
              })
            )}
          </nav>
        </aside>
      ) : null}

      <section className="workspace" aria-label="Milena workspace">
        {!visibleColumns.topics || !visibleColumns.inspector ? (
          <div className="workspace-rail-controls" aria-label="Hidden sidebars">
            {!visibleColumns.topics ? (
              <button
                className="rail-toggle-button workspace-rail-toggle is-left"
                type="button"
                aria-label="Show topic sidebar"
                title="Show topic sidebar"
                onClick={() => setColumnVisibility("topics", true)}
              >
                <PanelLeftOpen aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            ) : null}
            {!visibleColumns.inspector ? (
              <button
                className="rail-toggle-button workspace-rail-toggle is-right"
                type="button"
                aria-label="Show inspector column"
                title="Show inspector column"
                onClick={() => setColumnVisibility("inspector", true)}
              >
                <PanelRightOpen aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            ) : null}
          </div>
        ) : null}
        <section
          className={[
            "pane-grid",
            visiblePanes.length === 0 ? "is-empty" : "",
            expandedPane ? "is-expanded" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          data-layout={visibleLayout}
        >
          {visiblePanes.length === 0 ? (
            <p className="workspace-empty">
              {previewTopic
                ? "Topic selected for preview."
                : "Select a topic to preview it."}
            </p>
          ) : visiblePanes.map((pane) => (
            <Pane
              key={pane.id}
              pane={pane}
              active={pane.id === workspace.selectedPaneId}
              canSplit={!expandedPane && workspace.panes.length < 4}
              isExpanded={pane.id === workspace.expandedPaneId}
              onActivate={() => {
                updateWorkspace((current) => ({
                  ...selectWorkspacePane(current, pane.id),
                  selectedTopic: pane.topic,
                }));
              }}
              onPoll={() => pane.topic && startPolling(pane.id, pane.topic)}
              publisher={getPublisherPaneState(
                publisherState,
                pane.id,
                pane.topic,
              )}
              onPayloadChange={(payload) =>
                changePublisherPayload(pane.id, pane.topic, payload)
              }
              onKeyChange={(key) => changePublisherKey(pane.id, pane.topic, key)}
              onFormatPayload={() =>
                formatPanePublisherPayload(pane.id, pane.topic)
              }
              onSend={() => sendPanePublisherRecord(pane.id)}
              environmentKey={activeEnvironmentKey}
              renderPreferences={messageRenderPreferences}
              onRenderModeChange={setMessageRenderMode}
              onSplit={(direction) => splitPane(pane.id, direction)}
              onExpand={() => expandPane(pane.id)}
              onRestore={restorePane}
              onStop={() => stopPane(pane.id)}
              onClose={() => closeWorkspacePane(pane.id)}
            />
          ))}
        </section>
      </section>

      {visibleColumns.inspector ? (
        <RightRail
          activePane={activePane}
          activity={activity}
          layout={workspace.layout}
          paneCount={workspace.panes.length}
          topicPreview={topicPreview}
          onClearActivity={clearActivityLog}
          onHideInspector={() => setColumnVisibility("inspector", false)}
        />
      ) : null}
      {topicMenu ? (
        <TopicContextMenu
          menu={topicMenu}
          panes={workspace.panes}
          selectedPaneId={workspace.selectedPaneId}
          onOpen={(placement) => openTopic(topicMenu.topic, placement)}
        />
      ) : null}
    </main>
  );
}

export function RightRail({
  activePane,
  activity,
  layout,
  paneCount,
  topicPreview,
  onClearActivity,
  onHideInspector = () => undefined,
}: {
  activePane: WorkspacePane | undefined;
  activity: GlobalActivityEntry[];
  layout: WorkspaceState["layout"];
  paneCount: number;
  topicPreview: TopicPreviewModel | null;
  onClearActivity: () => void;
  onHideInspector?: () => void;
}) {
  const statusClass = topicPreview ? "preview" : activePane?.status ?? "idle";
  const contextPill = topicPreview ? "preview" : activePane ? "selected" : "idle";
  const inspectorTitle = topicPreview
    ? "Topic preview"
    : activePane
      ? "Selected pane"
      : "No selection";
  const inspectorContext = topicPreview
    ? topicPreview.name
    : activePane
      ? `Pane ${activePane.id}`
      : formatLayout(layout, paneCount);

  return (
    <aside className="rail rail-right" aria-label="Activity log">
      <header className="rail-header">
        <div className="rail-title">
          <span className="eyebrow">Inspector</span>
          <strong>{inspectorTitle}</strong>
          <small>{inspectorContext}</small>
        </div>
        <div className="rail-header-actions">
          <span className={`status-pill ${statusClass}`}>{contextPill}</span>
          <button
            className="rail-toggle-button"
            type="button"
            aria-label="Hide inspector column"
            title="Hide inspector column"
            onClick={onHideInspector}
          >
            <PanelRightClose aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
        </div>
      </header>
      {topicPreview ? (
        <section className="topic-preview">
          <div>
            <span>Topic</span>
            <strong>{topicPreview.name}</strong>
          </div>
          <div>
            <span>Partitions</span>
            <strong>
              {topicPreview.partitionCount === null
                ? "unknown"
                : `${topicPreview.partitionCount} partitions`}
            </strong>
          </div>
          <div>
            <span>Render</span>
            <strong>{topicPreview.renderMode}</strong>
          </div>
          <div className="topic-actions">
            <span>Open actions</span>
            <strong>
              <span>Poll</span>
              <span>Publish</span>
            </strong>
          </div>
        </section>
      ) : (
        <section className="pane-state">
          {activePane ? (
            <>
              <div>
                <span>Pane</span>
                <strong>Pane {activePane.id}</strong>
              </div>
              <div>
                <span>Status</span>
                <strong>{activePane.status}</strong>
              </div>
            </>
          ) : null}
          <div>
            <span>Topic</span>
            <strong>{activePane?.topic ?? "none"}</strong>
          </div>
          <div>
            <span>Group</span>
            <strong>{activePane?.consumerGroup ?? "none"}</strong>
          </div>
          <div>
            <span>Layout</span>
            <strong>{formatLayout(layout, paneCount)}</strong>
          </div>
        </section>
      )}
      <section className="activity-log">
        <header>
          <span className="eyebrow">Activity & errors</span>
          <button type="button" onClick={onClearActivity}>
            Clear
          </button>
        </header>
        {activity.length === 0 ? (
          <p>Inactive</p>
        ) : (
          activity.map((event) => (
            <p
              className={event.severity === "error" ? "activity-error" : ""}
              key={event.id}
            >
              <span>{event.time}</span>
              <strong>{event.paneId ? `P${event.paneId}` : event.source}</strong>
              <span>
                {event.message}
                {event.detail ? <small>{event.detail}</small> : null}
              </span>
            </p>
          ))
        )}
      </section>
    </aside>
  );
}

type PaneProps = {
  pane: WorkspacePane;
  active: boolean;
  canSplit: boolean;
  isExpanded: boolean;
  onActivate: () => void;
  onPoll: () => void;
  publisher: PublisherPaneState;
  onPayloadChange: (payload: string) => void;
  onKeyChange: (key: string) => void;
  onFormatPayload: () => void;
  onSend: () => void;
  environmentKey: string;
  renderPreferences: MessageRenderPreferences;
  onRenderModeChange: (topic: string, mode: MessageRenderMode) => void;
  onSplit: (direction: SplitDirection) => void;
  onExpand: () => void;
  onRestore: () => void;
  onStop: () => void;
  onClose: () => void;
};

export function Pane({
  pane,
  active,
  canSplit,
  isExpanded,
  onActivate,
  onPoll,
  publisher,
  onPayloadChange,
  onKeyChange,
  onFormatPayload,
  onSend,
  environmentKey,
  renderPreferences,
  onRenderModeChange,
  onSplit,
  onExpand,
  onRestore,
  onStop,
  onClose,
}: PaneProps) {
  const empty = isPaneEmpty(pane);
  const canStart = canStartPaneSession(pane);
  const renderMode = topicMessageRenderMode(
    renderPreferences,
    environmentKey,
    pane.topic,
  );
  const [expandedRows, setExpandedRows] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [publisherExpanded, setPublisherExpanded] = useState(false);
  useEffect(() => {
    setPublisherExpanded(false);
  }, [pane.id, pane.topic]);
  const recordEvents = pane.activity.filter(isKafkaRecordEvent);
  const payloadValidation = validatePublisherPayload(publisher.payload);
  const canSend = canSendPublisherRecord(pane, publisher);
  const publisherReadiness = publisherReadinessLabel(
    pane,
    publisher,
    payloadValidation.ok ? null : payloadValidation.error,
  );
  const sessionButton = paneSessionButtonState(pane);
  const compactStatus = compactPaneStatusLabel(pane);

  function toggleRow(identity: string) {
    setExpandedRows((current) => {
      const next = new Set(current);
      if (next.has(identity)) {
        next.delete(identity);
      } else {
        next.add(identity);
      }
      return next;
    });
  }

  return (
    <article
      className={[
        "pane",
        active ? "active" : "",
        empty ? "is-empty" : "",
        pane.tone === "error" ? "has-error" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      aria-label={`Pane ${pane.id}`}
      aria-current={active ? "true" : undefined}
      onClick={onActivate}
    >
      <header className="pane-header">
        <div className="pane-identity">
          <span className="eyebrow">Pane {pane.id} / {pane.mode}</span>
          <div className="pane-topic-row">
            <h2>{pane.topic ?? "Empty pane"}</h2>
            <div className="pane-session-actions">
              <button
                className="pane-session-button"
                type="button"
                aria-label={sessionButton.label}
                onClick={stopEvent(
                  sessionButton.action === "stop" ? onStop : onPoll,
                )}
                disabled={!canStart}
              >
                {sessionButton.action === "stop" ? (
                  <Square aria-hidden="true" size={11} fill="currentColor" />
                ) : (
                  <Play aria-hidden="true" size={12} fill="currentColor" />
                )}
                <span>{sessionButton.label}</span>
              </button>
            </div>
          </div>
          <small>{pane.consumerGroup ?? "No consumer group"}</small>
        </div>
        <div className="pane-actions">
          <button
            className="pane-icon-button pane-expand-button"
            type="button"
            title={
              isExpanded ? `Restore pane ${pane.id}` : `Maximize pane ${pane.id}`
            }
            aria-label={
              isExpanded ? `Restore pane ${pane.id}` : `Maximize pane ${pane.id}`
            }
            onClick={stopEvent(isExpanded ? onRestore : onExpand)}
          >
            {isExpanded ? (
              <Minimize2 aria-hidden="true" size={15} strokeWidth={1.9} />
            ) : (
              <Maximize2 aria-hidden="true" size={15} strokeWidth={1.9} />
            )}
          </button>
          <button
            className="pane-icon-button pane-close-button"
            type="button"
            title={`Close pane ${pane.id}`}
            aria-label={`Close pane ${pane.id}`}
            onClick={stopEvent(onClose)}
          >
            <X aria-hidden="true" size={15} strokeWidth={2.1} />
          </button>
          <button
            className="pane-icon-button pane-split-button"
            type="button"
            aria-label={`Split pane ${pane.id} right`}
            title={`Split pane ${pane.id} right`}
            onClick={stopEvent(() => onSplit("right"))}
            disabled={!canSplit}
          >
            <SplitSquareVertical aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
          <button
            className="pane-icon-button pane-split-button"
            type="button"
            aria-label={`Split pane ${pane.id} top`}
            title={`Split pane ${pane.id} top`}
            onClick={stopEvent(() => onSplit("top"))}
            disabled={!canSplit}
          >
            <SplitSquareHorizontal aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
        </div>
      </header>

      {pane.error ? (
        <div className="error-strip pane-error">{pane.error}</div>
      ) : null}

      <div className="session-grid">
        <div className="stat-cell">
          <span>Status</span>
          <strong>{pane.status}</strong>
        </div>
        <div className="stat-cell">
          <span>Session</span>
          <strong>{pane.session?.sessionId ?? "none"}</strong>
        </div>
        <div className="stat-cell">
          <span>Mode</span>
          <strong>{pane.mode}</strong>
        </div>
      </div>

      <div
        className={`pane-workbench ${
          publisherExpanded ? "has-expanded-publisher" : "has-collapsed-publisher"
        }`}
      >
        <section className="consumer" aria-label={`Consumer for pane ${pane.id}`}>
          <header>
            <span className="eyebrow">Consumer</span>
            <div className="consumer-controls">
              {pane.topic ? (
                <label
                  className="render-mode-control"
                  onClick={(event) => event.stopPropagation()}
                >
                  <span>Render</span>
                  <select
                    aria-label={`Render mode for ${pane.topic}`}
                    value={renderMode}
                    onChange={(event) =>
                      onRenderModeChange(
                        pane.topic ?? "",
                        event.currentTarget.value as MessageRenderMode,
                      )
                    }
                  >
                    <option value="json">JSON</option>
                    <option value="raw">Raw</option>
                  </select>
                </label>
              ) : null}
              <span className={`status-pill ${pane.status}`}>{compactStatus}</span>
            </div>
          </header>
          <div className="message-stream">
            {recordEvents.length === 0 ? (
              <p className="empty-state">Inactive</p>
            ) : (
              recordEvents.map((event) => {
                const collapsed = renderKafkaRecord(event.data.record, {
                  mode: renderMode,
                });
                const rendered = renderKafkaRecord(event.data.record, {
                  mode: renderMode,
                  expanded: expandedRows.has(collapsed.identity),
                });

                return (
                  <MessageStreamRow
                    key={rendered.identity}
                    message={rendered}
                    onToggle={() => toggleRow(rendered.identity)}
                  />
                );
              })
            )}
          </div>
        </section>

        <section
          className={`publisher ${publisherExpanded ? "expanded" : "collapsed"}`}
          aria-label={`Publisher for pane ${pane.id}`}
          onClick={(event) => event.stopPropagation()}
        >
          <header>
            <div className="publisher-heading">
              <span className="eyebrow">Publisher</span>
              <small>
                {publisherExpanded ? "Draft editor" : publisher.error ?? publisherReadiness}
              </small>
            </div>
            <button
              className="secondary compact"
              type="button"
              aria-controls={`publisher-panel-${pane.id}`}
              aria-expanded={publisherExpanded}
              aria-label={`${publisherExpanded ? "Collapse" : "Expand"} publisher for pane ${pane.id}`}
              onClick={stopEvent(() =>
                setPublisherExpanded((expanded) => !expanded),
              )}
            >
              <span>{publisherExpanded ? "Collapse" : "Expand"}</span>
              {publisherExpanded ? (
                <ChevronDown aria-hidden="true" size={14} strokeWidth={2} />
              ) : (
                <ChevronUp aria-hidden="true" size={14} strokeWidth={2} />
              )}
            </button>
          </header>
          {publisherExpanded ? (
            <div className="publisher-panel" id={`publisher-panel-${pane.id}`}>
              <div className="publisher-tools">
                <button
                  className="secondary compact"
                  type="button"
                  onClick={stopEvent(onFormatPayload)}
                >
                  Format JSON
                </button>
              </div>
              <div className="publisher-fields">
                <label>
                  <span>Key</span>
                  <input
                    aria-label={`Kafka key for pane ${pane.id}`}
                    placeholder="Optional key"
                    type="text"
                    value={publisher.key}
                    onChange={(event) => onKeyChange(event.currentTarget.value)}
                  />
                </label>
                <label>
                  <span>Payload</span>
                  <textarea
                    aria-label={`JSON payload for pane ${pane.id}`}
                    spellCheck={false}
                    value={publisher.payload}
                    onChange={(event) => onPayloadChange(event.currentTarget.value)}
                  />
                </label>
              </div>
              <section className={`producer-status ${publisher.status}`}>
                <div>
                  <span className="eyebrow">Producer</span>
                  <strong>{producerAckLabel(publisher.ack)}</strong>
                </div>
                <small>{publisher.error ?? publisherReadiness}</small>
              </section>
              <div className="publisher-footer">
                <span>{publisher.status === "sending" ? "sending" : "acks=all"}</span>
                <button
                  className="primary"
                  type="button"
                  onClick={stopEvent(onSend)}
                  disabled={!canSend}
                >
                  Send
                </button>
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </article>
  );
}

function MessageStreamRow({
  message,
  onToggle,
}: {
  message: RenderedKafkaRecord;
  onToggle: () => void;
}) {
  return (
    <article
      className={[
        "message-row",
        message.expanded ? "expanded" : "",
        message.payload.invalidJson ? "invalid-json" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <button
        className={`message-row-summary ${message.key ? "has-key" : "no-key"}`}
        type="button"
        aria-expanded={message.expanded}
        onClick={stopEvent(onToggle)}
      >
        <span className="message-toggle" aria-hidden="true">
          {message.expanded ? "-" : "+"}
        </span>
        <span className="message-meta">{message.receiveTime}</span>
        <span className="topic-label">{message.topic}</span>
        <span className="message-meta">
          p{message.partition} / {message.offset}
        </span>
        {message.key ? (
          <span className="message-key">key {message.key}</span>
        ) : null}
        <span className="message-preview">
          {message.payload.marker ? (
            <span className="message-marker">{message.payload.marker}</span>
          ) : null}
          <code>{message.payload.preview}</code>
          {message.payload.truncated ? (
            <span className="message-marker">truncated</span>
          ) : null}
        </span>
      </button>

      {message.expanded ? (
        <div className="message-expanded">
          <pre>{message.payload.content}</pre>
          {message.payload.truncated ? (
            <span className="message-marker">payload truncated</span>
          ) : null}
          <section className="message-headers" aria-label="Kafka headers">
            <span className="eyebrow">Headers</span>
            {message.headers.length === 0 ? (
              <p>No headers</p>
            ) : (
              <dl>
                {message.headers.map((header, index) => (
                  <div key={`${header.key}-${index}`}>
                    <dt>{header.key}</dt>
                    <dd>{formatHeaderValue(header.value)}</dd>
                  </div>
                ))}
              </dl>
            )}
          </section>
        </div>
      ) : null}
    </article>
  );
}

function TopicContextMenu({
  menu,
  panes,
  selectedPaneId,
  onOpen,
}: {
  menu: NonNullable<TopicMenuState>;
  panes: WorkspacePane[];
  selectedPaneId: number;
  onOpen: (placement: TopicOpenPlacement) => void;
}) {
  const selectedPane = panes.find((pane) => pane.id === selectedPaneId);

  return (
    <div
      className="topic-menu"
      role="menu"
      style={{ left: menu.x, top: menu.y }}
      onClick={(event) => event.stopPropagation()}
    >
      <header>
        <span className="eyebrow">Topic actions</span>
        <strong>{menu.topic}</strong>
      </header>
      {panes.length === 0 ? (
        <div className="topic-menu-row" role="none">
          <span>First pane</span>
          <button type="button" role="menuitem" onClick={() => onOpen("selected")}>
            Open
          </button>
        </div>
      ) : (
        <>
          <div className="topic-menu-row" role="none">
            <span>
              {selectedPane ? `Pane ${selectedPane.id}` : "Selected pane"}
            </span>
            <button
              type="button"
              role="menuitem"
              onClick={() => onOpen("selected")}
            >
              Open selected
            </button>
          </div>
          <div className="topic-menu-row" role="none">
            <span>Split</span>
            <div className="topic-menu-actions">
              <button
                type="button"
                role="menuitem"
                onClick={() => onOpen("right")}
              >
                Split right
              </button>
              <button
                type="button"
                role="menuitem"
                onClick={() => onOpen("top")}
              >
                Split top
              </button>
              <button
                type="button"
                role="menuitem"
                onClick={() => onOpen("bottom")}
              >
                Split bottom
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function formatLayout(layout: string, paneCount: number) {
  if (paneCount === 0) {
    return "no panes";
  }

  if (layout === "quad") {
    return "2 x 2";
  }

  if (layout === "two-top") {
    return "2 panes / top";
  }

  if (layout === "two-right") {
    return "2 panes / right";
  }

  return "1 pane";
}

function topicRailStatus(status: string, topicCount: number) {
  if (status === "loading") {
    return "Listing topics";
  }

  if (status === "error") {
    return "Topic list unavailable";
  }

  return `${topicCount} topics / manual refresh`;
}

function isKafkaRecordEvent(
  event: MilenaBoundaryEvent,
): event is Extract<MilenaBoundaryEvent, { event: "kafkaRecord" }> {
  return event.event === "kafkaRecord";
}

function formatHeaderValue(value: string | null | undefined): string {
  return value ?? "(null)";
}

function publisherReadinessLabel(
  pane: WorkspacePane,
  publisher: PublisherPaneState,
  validationError: string | null,
) {
  if (publisher.error) {
    return publisher.error;
  }

  if (validationError) {
    return validationError;
  }

  if (!pane.topic) {
    return "Assign a topic before publishing";
  }

  if (pane.mode !== "poll" || pane.status !== "ready") {
    return "Start polling before publishing";
  }

  if (publisher.ack) {
    return `ack at ${new Date(publisher.ack.sentAt).toLocaleTimeString()}`;
  }

  return "Ready to publish";
}

function paneSessionButtonState(pane: WorkspacePane): {
  action: "poll" | "stop";
  label: "Poll" | "Stop";
} {
  if (pane.mode === "poll" && (pane.status === "loading" || pane.status === "ready")) {
    return { action: "stop", label: "Stop" };
  }

  return { action: "poll", label: "Poll" };
}

function compactPaneStatusLabel(pane: WorkspacePane): string {
  if (pane.mode === "poll" && pane.status === "loading") {
    return "Starting";
  }

  if (pane.mode === "poll" && pane.status === "ready") {
    return "Polling";
  }

  return pane.status;
}

function getTopicPinStore(): TopicPinStore | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

function getMessageRenderPreferenceStore():
  | MessageRenderPreferenceStore
  | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

function stopEvent(action: () => void) {
  return (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    action();
  };
}

export default App;
