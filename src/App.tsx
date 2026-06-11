import { useEffect, useMemo, useState } from "react";
import {
  AppState,
  MilenaBoundaryEvent,
  TopicSessionPreview,
  TopicSessionPreviewRequest,
  loadAppState,
  previewTopicSession,
} from "./lib/tauri";

type BoundaryStatus = "idle" | "loading" | "ready" | "error";
type PaneMode = "idle" | TopicSessionPreviewRequest["mode"];
type PaneTone = "normal" | "warning" | "error";

type Topic = {
  name: string;
  partitions: number;
  lag: number;
  pinned?: boolean;
};

type WorkspacePane = {
  id: number;
  topic: string;
  consumerGroup: string;
  mode: PaneMode;
  status: BoundaryStatus;
  session: TopicSessionPreview | null;
  activity: MilenaBoundaryEvent[];
  error: string | null;
  tone: PaneTone;
};

type ActivityEntry = MilenaBoundaryEvent & {
  paneId: number;
  time: string;
};

const topics: Topic[] = [
  { name: "orders.created", partitions: 12, lag: 0, pinned: true },
  { name: "payments.authorized", partitions: 8, lag: 4 },
  { name: "inventory.adjusted", partitions: 6, lag: 0 },
  { name: "shipments.dispatched", partitions: 4, lag: 17 },
];

function App() {
  const [appState, setAppState] = useState<AppState | null>(null);
  const [selectedTopic, setSelectedTopic] = useState(topics[0].name);
  const [activePaneId, setActivePaneId] = useState(1);
  const [panes, setPanes] = useState<WorkspacePane[]>([
    createPane(1, topics[0].name),
  ]);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const [appError, setAppError] = useState<string | null>(null);

  useEffect(() => {
    loadAppState()
      .then(setAppState)
      .catch((cause: unknown) => {
        const message =
          cause instanceof Error ? cause.message : "Tauri runtime unavailable";
        setAppError(message);
      });
  }, []);

  const activePane = useMemo(
    () => panes.find((pane) => pane.id === activePaneId) ?? panes[0],
    [activePaneId, panes],
  );

  const selectedTopicMeta = useMemo(() => {
    const selected = topics.find((topic) => topic.name === selectedTopic);
    return selected
      ? `${selected.partitions} partitions / lag ${selected.lag} / JSON`
      : "JSON / no active Kafka session";
  }, [selectedTopic]);

  function updatePane(
    paneId: number,
    update: Partial<WorkspacePane> | ((pane: WorkspacePane) => WorkspacePane),
  ) {
    setPanes((current) =>
      current.map((pane) => {
        if (pane.id !== paneId) {
          return pane;
        }

        return typeof update === "function" ? update(pane) : { ...pane, ...update };
      }),
    );
  }

  function selectTopic(topic: string) {
    setSelectedTopic(topic);
    updatePane(activePaneId, (pane) =>
      pane.status === "idle" && pane.activity.length === 0
        ? { ...pane, topic }
        : pane,
    );
  }

  async function openBoundary(paneId: number, mode: TopicSessionPreviewRequest["mode"]) {
    const pane = panes.find((candidate) => candidate.id === paneId);
    if (!pane) {
      return;
    }

    setActivePaneId(paneId);
    updatePane(paneId, {
      mode,
      status: "loading",
      error: null,
      activity: [],
      tone: "normal",
    });

    try {
      const nextSession = await previewTopicSession(
        { topic: pane.topic, mode },
        (event) => {
          updatePane(paneId, (currentPane) => ({
            ...currentPane,
            activity: [event, ...currentPane.activity].slice(0, 6),
          }));
          setActivity((current) =>
            [
              { ...event, paneId, time: new Date().toLocaleTimeString() },
              ...current,
            ].slice(0, 10),
          );
        },
      );
      updatePane(paneId, {
        session: nextSession,
        status: "ready",
        mode: nextSession.mode,
      });
    } catch (cause) {
      const message =
        cause instanceof Error ? cause.message : "Command boundary failed";
      updatePane(paneId, { error: message, status: "error", tone: "error" });
    }
  }

  function splitPane() {
    if (panes.length >= 4) {
      return;
    }

    const nextId = Math.max(...panes.map((pane) => pane.id)) + 1;
    setPanes((current) => [...current, createPane(nextId, selectedTopic)]);
    setActivePaneId(nextId);
  }

  function stopPane(paneId: number) {
    updatePane(paneId, (pane) => ({
      ...pane,
      mode: "idle",
      status: "idle",
      session: null,
      error: null,
      activity: [],
      tone: "normal",
    }));
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
          <strong>localhost:9092</strong>
          <small>{appState?.version ?? "Rust command pending"}</small>
        </section>
        <nav className="topic-list">
          {topics.map((topic) => (
            <button
              className={[
                "topic",
                topic.name === selectedTopic ? "active" : "",
                topic.pinned ? "pinned" : "",
              ]
                .filter(Boolean)
                .join(" ")}
              key={topic.name}
              type="button"
              onClick={() => selectTopic(topic.name)}
            >
              <span className="topic-marker" />
              <span>
                <strong>{topic.name}</strong>
                <small>{topic.partitions} partitions / lag {topic.lag}</small>
              </span>
              <span className="topic-mode">{topic.pinned ? "PIN" : "JSON"}</span>
            </button>
          ))}
        </nav>
      </aside>

      <section className="workspace" aria-label="Milena workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Workspace</span>
            <h1>{selectedTopic}</h1>
            <p>{selectedTopicMeta}</p>
          </div>
          <div className="toolbar">
            <button
              type="button"
              onClick={() => activePane && openBoundary(activePane.id, "poll")}
            >
              Poll
            </button>
            <button
              className="primary"
              type="button"
              onClick={() => activePane && openBoundary(activePane.id, "publish")}
            >
              Send
            </button>
          </div>
        </header>

        <section className="pane-grid" data-count={panes.length}>
          {panes.map((pane) => (
            <Pane
              key={pane.id}
              pane={pane}
              active={pane.id === activePaneId}
              canSplit={panes.length < 4}
              onActivate={() => {
                setActivePaneId(pane.id);
                setSelectedTopic(pane.topic);
              }}
              onPoll={() => openBoundary(pane.id, "poll")}
              onPublish={() => openBoundary(pane.id, "publish")}
              onSplit={splitPane}
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
            <strong>{activePane?.topic ?? selectedTopic}</strong>
          </div>
          <div>
            <span>Group</span>
            <strong>{activePane?.consumerGroup ?? "none"}</strong>
          </div>
          <div>
            <span>Split</span>
            <strong>{panes.length === 4 ? "2 x 2" : `${panes.length} pane`}</strong>
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
    </main>
  );
}

function createPane(id: number, topic: string): WorkspacePane {
  return {
    id,
    topic,
    consumerGroup: `milena-preview-${id}`,
    mode: "idle",
    status: "idle",
    session: null,
    activity: [],
    error: null,
    tone: "normal",
  };
}

type PaneProps = {
  pane: WorkspacePane;
  active: boolean;
  canSplit: boolean;
  onActivate: () => void;
  onPoll: () => void;
  onPublish: () => void;
  onSplit: () => void;
  onStop: () => void;
};

function Pane({
  pane,
  active,
  canSplit,
  onActivate,
  onPoll,
  onPublish,
  onSplit,
  onStop,
}: PaneProps) {
  return (
    <article
      className={[
        "pane",
        active ? "active" : "",
        pane.tone === "error" ? "has-error" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      onClick={onActivate}
    >
      <header className="pane-header">
        <div>
          <span className="eyebrow">Pane {pane.id} / {pane.mode}</span>
          <h2>{pane.topic}</h2>
          <small>{pane.consumerGroup}</small>
        </div>
        <div className="pane-actions">
          <button type="button" onClick={stopEvent(onPoll)}>
            Poll
          </button>
          <button type="button" onClick={stopEvent(onSplit)} disabled={!canSplit}>
            Split
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
  "topic": "${pane.topic}",
  "key": "preview"
}`}</pre>
          <div className="publisher-footer">
            <span>acks=all</span>
            <button className="primary" type="button" onClick={stopEvent(onPublish)}>
              Send
            </button>
          </div>
        </section>

        <section className="consumer">
          <header>
            <span className="eyebrow">Consumer</span>
            <span className={`status-pill ${pane.status}`}>{pane.status}</span>
          </header>
          <div className="message-stream">
            {pane.activity.length === 0 ? (
              <p className="empty-state">Inactive</p>
            ) : (
              pane.activity.map((event, index) => (
                <article className="message-row" key={`${event.event}-${index}`}>
                  <span className="message-meta">p0 / offset {index}</span>
                  <span className="topic-label">{pane.topic}</span>
                  <code>{JSON.stringify(event.data)}</code>
                </article>
              ))
            )}
          </div>
        </section>
      </div>
    </article>
  );
}

function stopEvent(action: () => void) {
  return (event: React.MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    action();
  };
}

export default App;
