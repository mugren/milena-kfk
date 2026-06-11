import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import {
  AppState,
  MilenaBoundaryEvent,
  RuntimeAuthConfig,
  TopicSessionPreviewRequest,
  listKafkaTopics,
  loadAppState,
  previewTopicSession,
  startKafkaConsumerSession,
  stopKafkaConsumerSession,
} from "./lib/tauri";
import {
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
  appendPaneActivity,
  assignTopicToPane,
  canStartPaneSession,
  createInitialWorkspaceState,
  getSelectedPane,
  isPaneEmpty,
  markPaneError,
  markPaneLoading,
  markPaneReady,
  selectPane as selectWorkspacePane,
  splitPane as splitWorkspacePane,
  type SplitDirection,
  type WorkspacePane,
  type WorkspaceState,
} from "./lib/workspace";

type ActivityEntry = MilenaBoundaryEvent & {
  paneId: number;
  time: string;
};

type TopicMenuState = {
  topic: string;
  x: number;
  y: number;
} | null;

const activeRuntimeAuth: RuntimeAuthConfig = {
  environment: "local-dev",
  brokers: ["localhost:9092"],
  properties: {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "local",
    "sasl.password": "local",
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
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const [appError, setAppError] = useState<string | null>(null);
  const [topicMenu, setTopicMenu] = useState<TopicMenuState>(null);
  const requestedTopicLoads = useRef(new Set<string>());
  const workspaceRef = useRef(workspace);
  const pollingRuns = useRef(new Map<number, number>());

  useEffect(() => {
    loadAppState()
      .then(setAppState)
      .catch((cause: unknown) => {
        const message =
          cause instanceof Error ? cause.message : "Tauri runtime unavailable";
        setAppError(message);
      });
    void loadTopicList(false);
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

  const activeTopic = activePane?.topic ?? null;
  const activeEnvironmentKey = environmentKey(activeRuntimeAuth);
  const topicRows = useMemo(() => visibleTopicRows(topicRail), [topicRail]);

  const selectedTopicMeta = useMemo(() => {
    const selected = topicRail.topics.find(
      (topic) => topic.name === activeTopic,
    );
    return selected
      ? `${selected.partitionCount} partitions / JSON`
      : "No topic assigned / no active Kafka session";
  }, [activeTopic, topicRail.topics]);

  function selectTopic(topic: string) {
    updateWorkspace((current) =>
      assignTopicToPane(current, current.selectedPaneId, topic),
    );
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

  async function openBoundary(paneId: number, mode: TopicSessionPreviewRequest["mode"]) {
    const pane = workspace.panes.find((candidate) => candidate.id === paneId);
    if (!canStartPaneSession(pane)) {
      return;
    }

    updateWorkspace((current) => markPaneLoading(current, paneId, mode));

    try {
      const nextSession = await previewTopicSession(
        { topic: pane.topic, mode },
        (event) => {
          updateWorkspace((current) => appendPaneActivity(current, paneId, event));
          setActivity((current) =>
            [
              { ...event, paneId, time: new Date().toLocaleTimeString() },
              ...current,
            ].slice(0, 10),
          );
        },
      );
      updateWorkspace((current) => markPaneReady(current, paneId, nextSession));
    } catch (cause) {
      const message =
        cause instanceof Error ? cause.message : "Command boundary failed";
      updateWorkspace((current) => markPaneError(current, paneId, message));
    }
  }

  function startPolling(paneId: number, topic: string) {
    const run = nextPollingRun(paneId);
    setTopicMenu(null);
    void startPanePollingSession({
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
          [
            { ...event, paneId, time: new Date().toLocaleTimeString() },
            ...current,
          ].slice(0, 10),
        );
      },
      onStopError: setAppError,
    });
  }

  function splitPane(paneId: number, direction: SplitDirection) {
    updateWorkspace((current) => splitWorkspacePane(current, paneId, direction));
  }

  function stopPane(paneId: number) {
    nextPollingRun(paneId);
    void stopPanePollingSession({
      paneId,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      stopConsumerSession: stopKafkaConsumerSession,
      onStopError: setAppError,
    });
  }

  function updateWorkspace(update: (current: WorkspaceState) => WorkspaceState) {
    setWorkspace((current) => {
      const next = update(current);
      workspaceRef.current = next;
      return next;
    });
  }

  function nextPollingRun(paneId: number) {
    const next = (pollingRuns.current.get(paneId) ?? 0) + 1;
    pollingRuns.current.set(paneId, next);
    return next;
  }

  function openTopicMenu(event: MouseEvent<HTMLDivElement>, topic: string) {
    event.preventDefault();
    setTopicMenu({ topic, x: event.clientX, y: event.clientY });
  }

  return (
    <main className="app-shell">
      <aside className="rail rail-left" aria-label="Kafka topics">
        <header className="rail-header">
          <div>
            <span className="eyebrow">Milena</span>
            <strong>{appState?.platform ?? "macOS dev"}</strong>
          </div>
          <span className="status-pill">Local</span>
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
            onChange={(event) =>
              setTopicRail((current) =>
                setTopicSearch(current, event.currentTarget.value),
              )
            }
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
            topicRows.map((topic) => (
              <div
                className={[
                  "topic",
                  topic.name === activeTopic ? "active" : "",
                  topic.pinned ? "pinned" : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
                key={topic.name}
                onContextMenu={(event) => openTopicMenu(event, topic.name)}
              >
                <button
                  className="topic-select"
                  type="button"
                  onClick={() => selectTopic(topic.name)}
                >
                  <span className="topic-marker" />
                  <span>
                    <strong>{topic.name}</strong>
                    <small>{topic.partitionCount} partitions</small>
                  </span>
                  <span className="topic-mode">
                    {topicMessageRenderMode(
                      messageRenderPreferences,
                      activeEnvironmentKey,
                      topic.name,
                    ).toUpperCase()}
                  </span>
                </button>
                <button
                  className="topic-pin"
                  type="button"
                  aria-label={`${topic.pinned ? "Unpin" : "Pin"} ${topic.name}`}
                  aria-pressed={topic.pinned}
                  onClick={() => pinTopic(topic.name)}
                >
                  <span />
                </button>
              </div>
            ))
          )}
        </nav>
      </aside>

      <section className="workspace" aria-label="Milena workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Workspace</span>
            <h1>{activeTopic ?? "Empty pane"}</h1>
            <p>{selectedTopicMeta}</p>
          </div>
          <div className="toolbar">
            <button
              type="button"
              disabled={!canStartPaneSession(activePane)}
              onClick={() =>
                activePane?.topic && startPolling(activePane.id, activePane.topic)
              }
            >
              Poll
            </button>
            <button
              className="primary"
              type="button"
              disabled={!canStartPaneSession(activePane)}
              onClick={() => activePane && openBoundary(activePane.id, "publish")}
            >
              Send
            </button>
          </div>
        </header>

        <section className="pane-grid" data-layout={workspace.layout}>
          {workspace.panes.map((pane) => (
            <Pane
              key={pane.id}
              pane={pane}
              active={pane.id === workspace.selectedPaneId}
              canSplit={workspace.panes.length < 4}
              onActivate={() => {
                updateWorkspace((current) =>
                  selectWorkspacePane(current, pane.id),
                );
              }}
              onPoll={() => pane.topic && startPolling(pane.id, pane.topic)}
              onPublish={() => openBoundary(pane.id, "publish")}
              environmentKey={activeEnvironmentKey}
              renderPreferences={messageRenderPreferences}
              onRenderModeChange={setMessageRenderMode}
              onSplit={(direction) => splitPane(pane.id, direction)}
              onStop={() => stopPane(pane.id)}
            />
          ))}
        </section>
      </section>

      <aside className="rail rail-right" aria-label="Activity log">
        <header className="rail-header">
          <div>
            <span className="eyebrow">Pane state</span>
            <strong>{activePane ? `Pane ${activePane.id}` : "No pane"}</strong>
          </div>
          <span className={`status-pill ${activePane?.status ?? "idle"}`}>
            {activePane?.status ?? "idle"}
          </span>
        </header>
        <section className="pane-state">
          <div>
            <span>Topic</span>
            <strong>{activePane?.topic ?? "none"}</strong>
          </div>
          <div>
            <span>Group</span>
            <strong>{activePane?.consumerGroup ?? "none"}</strong>
          </div>
          <div>
            <span>Split</span>
            <strong>{formatLayout(workspace.layout)}</strong>
          </div>
        </section>
        {appError ? <div className="error-strip compact">{appError}</div> : null}
        <section className="split-rules">
          <header>
            <span className="eyebrow">Capabilities</span>
          </header>
          <ul>
            {(appState?.capabilities ?? ["command-boundary"]).map((capability) => (
              <li key={capability}>{capability}</li>
            ))}
          </ul>
        </section>
        <section className="activity-log">
          <header>
            <span className="eyebrow">Events</span>
            <button type="button" onClick={() => setActivity([])}>
              Clear
            </button>
          </header>
          {activity.length === 0 ? (
            <p>Inactive</p>
          ) : (
            activity.map((event, index) => (
              <p key={`${event.event}-log-${index}`}>
                <span>{event.time}</span>
                <strong>P{event.paneId}</strong>
                {event.event}
              </p>
            ))
          )}
        </section>
      </aside>
      {topicMenu ? (
        <TopicContextMenu
          menu={topicMenu}
          panes={workspace.panes}
          selectedPaneId={workspace.selectedPaneId}
          onPoll={(paneId) => startPolling(paneId, topicMenu.topic)}
        />
      ) : null}
    </main>
  );
}

type PaneProps = {
  pane: WorkspacePane;
  active: boolean;
  canSplit: boolean;
  onActivate: () => void;
  onPoll: () => void;
  onPublish: () => void;
  environmentKey: string;
  renderPreferences: MessageRenderPreferences;
  onRenderModeChange: (topic: string, mode: MessageRenderMode) => void;
  onSplit: (direction: SplitDirection) => void;
  onStop: () => void;
};

function Pane({
  pane,
  active,
  canSplit,
  onActivate,
  onPoll,
  onPublish,
  environmentKey,
  renderPreferences,
  onRenderModeChange,
  onSplit,
  onStop,
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
  const recordEvents = pane.activity.filter(isKafkaRecordEvent);

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
        <div>
          <span className="eyebrow">Pane {pane.id} / {pane.mode}</span>
          <h2>{pane.topic ?? "Empty pane"}</h2>
          <small>{pane.consumerGroup ?? "No consumer group"}</small>
        </div>
        <div className="pane-actions">
          <button type="button" onClick={stopEvent(onPoll)} disabled={!canStart}>
            Poll
          </button>
          <button
            type="button"
            aria-label={`Split pane ${pane.id} right`}
            onClick={stopEvent(() => onSplit("right"))}
            disabled={!canSplit}
          >
            Split right
          </button>
          <button
            type="button"
            aria-label={`Split pane ${pane.id} top`}
            onClick={stopEvent(() => onSplit("top"))}
            disabled={!canSplit}
          >
            Split top
          </button>
          <button type="button" onClick={stopEvent(onStop)}>
            Stop
          </button>
        </div>
      </header>

      {pane.error ? <div className="error-strip">{pane.error}</div> : null}

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

      <div className="pane-workbench">
        <section className="publisher">
          <header>
            <span className="eyebrow">Publisher</span>
            <button type="button" onClick={stopEvent(() => undefined)}>
              Format
            </button>
          </header>
          <pre>{`{
  "topic": ${pane.topic ? `"${pane.topic}"` : "null"},
  "key": "preview"
}`}</pre>
          <div className="publisher-footer">
            <span>acks=all</span>
            <button
              className="primary"
              type="button"
              onClick={stopEvent(onPublish)}
              disabled={!canStart}
            >
              Send
            </button>
          </div>
        </section>

        <section className="consumer">
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
              <span className={`status-pill ${pane.status}`}>{pane.status}</span>
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
        className="message-row-summary"
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
  onPoll,
}: {
  menu: NonNullable<TopicMenuState>;
  panes: WorkspacePane[];
  selectedPaneId: number;
  onPoll: (paneId: number) => void;
}) {
  return (
    <div
      className="topic-menu"
      role="menu"
      style={{ left: menu.x, top: menu.y }}
      onClick={(event) => event.stopPropagation()}
    >
      <header>
        <span className="eyebrow">Poll topic</span>
        <strong>{menu.topic}</strong>
      </header>
      {panes.map((pane) => (
        <button
          key={pane.id}
          type="button"
          role="menuitem"
          onClick={() => onPoll(pane.id)}
        >
          {pane.id === selectedPaneId ? "Selected pane" : `Pane ${pane.id}`}
        </button>
      ))}
    </div>
  );
}

function formatLayout(layout: string) {
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
