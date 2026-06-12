import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
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
  openCombinedPublishPollPane,
  openPublisherPane,
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
  assignTopicToPane,
  canStartPaneSession,
  createInitialWorkspaceState,
  getSelectedPane,
  isPaneEmpty,
  selectPane as selectWorkspacePane,
  splitPane as splitWorkspacePane,
  type SplitDirection,
  type WorkspacePane,
  type WorkspaceState,
} from "./lib/workspace";

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
  const [publisherState, setPublisherState] = useState(
    createInitialPublisherState,
  );
  const [activity, setActivity] = useState<GlobalActivityEntry[]>([]);
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
        recordGlobalError("app", "Tauri runtime unavailable", message);
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

  function openPublishPoll(paneId: number, topic: string) {
    const run = nextPollingRun(paneId);
    setTopicMenu(null);
    void openCombinedPublishPollPane({
      paneId,
      topic,
      auth: activeRuntimeAuth,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      updatePublisherState,
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

  function openTopicMenu(event: MouseEvent<HTMLDivElement>, topic: string) {
    event.preventDefault();
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
              onClick={() =>
                activePane?.topic && openPublishPoll(activePane.id, activePane.topic)
              }
            >
              Publish
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
              publisher={getPublisherPaneState(
                publisherState,
                pane.id,
                pane.topic,
              )}
              onOpenPublisher={() =>
                pane.topic && openPublishPoll(pane.id, pane.topic)
              }
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
            <span className="eyebrow">Activity & errors</span>
            <button type="button" onClick={clearActivityLog}>
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
      {topicMenu ? (
        <TopicContextMenu
          menu={topicMenu}
          panes={workspace.panes}
          selectedPaneId={workspace.selectedPaneId}
          onPoll={(paneId) => startPolling(paneId, topicMenu.topic)}
          onPublishPoll={(paneId) => openPublishPoll(paneId, topicMenu.topic)}
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
  publisher: PublisherPaneState;
  onOpenPublisher: () => void;
  onPayloadChange: (payload: string) => void;
  onKeyChange: (key: string) => void;
  onFormatPayload: () => void;
  onSend: () => void;
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
  publisher,
  onOpenPublisher,
  onPayloadChange,
  onKeyChange,
  onFormatPayload,
  onSend,
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
  const payloadValidation = validatePublisherPayload(publisher.payload);
  const canSend = canSendPublisherRecord(pane, publisher);
  const publisherReadiness = publisherReadinessLabel(
    pane,
    publisher,
    payloadValidation.ok ? null : payloadValidation.error,
  );

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

      <div className="pane-workbench">
        <section className="publisher" onClick={(event) => event.stopPropagation()}>
          <header>
            <span className="eyebrow">Publisher</span>
            <button
              className="secondary compact"
              type="button"
              onClick={stopEvent(onFormatPayload)}
            >
              Format JSON
            </button>
          </header>
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
            {pane.mode === "poll" ? null : (
              <button
                type="button"
                onClick={stopEvent(onOpenPublisher)}
                disabled={!pane.topic}
              >
                Start poll
              </button>
            )}
            <button
              className="primary"
              type="button"
              onClick={stopEvent(onSend)}
              disabled={!canSend}
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
  onPublishPoll,
}: {
  menu: NonNullable<TopicMenuState>;
  panes: WorkspacePane[];
  selectedPaneId: number;
  onPoll: (paneId: number) => void;
  onPublishPoll: (paneId: number) => void;
}) {
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
      {panes.map((pane) => (
        <div className="topic-menu-row" key={pane.id} role="none">
          <span>{pane.id === selectedPaneId ? "Selected" : `Pane ${pane.id}`}</span>
          <button type="button" role="menuitem" onClick={() => onPoll(pane.id)}>
            Poll
          </button>
          <button
            className="primary"
            type="button"
            role="menuitem"
            onClick={() => onPublishPoll(pane.id)}
          >
            Publish
          </button>
        </div>
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
