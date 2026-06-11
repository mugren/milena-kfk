import { useEffect, useMemo, useState } from "react";
import {
  AppState,
  MilenaBoundaryEvent,
  TopicSessionPreview,
  loadAppState,
  previewTopicSession,
} from "./lib/tauri";

type BoundaryStatus = "idle" | "loading" | "ready" | "error";

const initialTopics = [
  "orders.created",
  "payments.authorized",
  "inventory.adjusted",
];

function App() {
  const [appState, setAppState] = useState<AppState | null>(null);
  const [selectedTopic, setSelectedTopic] = useState(initialTopics[0]);
  const [status, setStatus] = useState<BoundaryStatus>("idle");
  const [session, setSession] = useState<TopicSessionPreview | null>(null);
  const [activity, setActivity] = useState<MilenaBoundaryEvent[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadAppState()
      .then(setAppState)
      .catch((cause: unknown) => {
        const message =
          cause instanceof Error ? cause.message : "Tauri runtime unavailable";
        setError(message);
      });
  }, []);

  const selectedTopicMeta = useMemo(
    () => `${selectedTopic} / JSON / no active Kafka session`,
    [selectedTopic],
  );

  async function openBoundary(mode: "poll" | "publish") {
    setStatus("loading");
    setError(null);
    setActivity([]);

    try {
      const nextSession = await previewTopicSession(
        { topic: selectedTopic, mode },
        (event) => setActivity((current) => [event, ...current].slice(0, 8)),
      );
      setSession(nextSession);
      setStatus("ready");
    } catch (cause) {
      const message =
        cause instanceof Error ? cause.message : "Command boundary failed";
      setError(message);
      setStatus("error");
    }
  }

  return (
    <main className="app-shell">
      <aside className="rail rail-left" aria-label="Kafka topics">
        <header className="rail-header">
          <span className="eyebrow">Milena</span>
          <strong>{appState?.platform ?? "macOS dev"}</strong>
        </header>
        <section className="environment-summary">
          <span className="status-pill">Local scaffold</span>
          <span>{appState?.version ?? "waiting for Rust command"}</span>
        </section>
        <nav className="topic-list">
          {initialTopics.map((topic) => (
            <button
              className={topic === selectedTopic ? "topic active" : "topic"}
              key={topic}
              type="button"
              onClick={() => setSelectedTopic(topic)}
            >
              <span className="topic-marker" />
              <span>
                <strong>{topic}</strong>
                <small>Preview topic</small>
              </span>
              <span className="topic-mode">JSON</span>
            </button>
          ))}
        </nav>
      </aside>

      <section className="workspace" aria-label="Milena workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Command boundary</span>
            <h1>{selectedTopic}</h1>
            <p>{selectedTopicMeta}</p>
          </div>
          <div className="toolbar">
            <button type="button" onClick={() => openBoundary("poll")}>
              Poll
            </button>
            <button
              className="primary"
              type="button"
              onClick={() => openBoundary("publish")}
            >
              Publish
            </button>
          </div>
        </header>

        <section className="pane">
          <header className="pane-header">
            <div>
              <span className="eyebrow">Pane 1</span>
              <h2>{session ? session.mode : "Empty"}</h2>
            </div>
            <span className={`status-pill ${status}`}>{status}</span>
          </header>

          {error ? <div className="error-strip">{error}</div> : null}

          <div className="session-grid">
            <div className="stat-cell">
              <span>Session</span>
              <strong>{session?.sessionId ?? "none"}</strong>
            </div>
            <div className="stat-cell">
              <span>Topic</span>
              <strong>{session?.topic ?? selectedTopic}</strong>
            </div>
            <div className="stat-cell">
              <span>Backend</span>
              <strong>Rust command</strong>
            </div>
          </div>

          <div className="message-stream">
            {activity.length === 0 ? (
              <p className="empty-state">No activity yet.</p>
            ) : (
              activity.map((event, index) => (
                <article className="message-row" key={`${event.event}-${index}`}>
                  <span className="topic-label">{event.event}</span>
                  <code>{JSON.stringify(event.data)}</code>
                </article>
              ))
            )}
          </div>
        </section>
      </section>

      <aside className="rail rail-right" aria-label="Activity log">
        <header className="rail-header">
          <span className="eyebrow">Boundary</span>
          <strong>Capabilities</strong>
        </header>
        <ul className="capability-list">
          {(appState?.capabilities ?? ["command-boundary"]).map((capability) => (
            <li key={capability}>{capability}</li>
          ))}
        </ul>
        <section className="activity-log">
          <header>
            <span className="eyebrow">Events</span>
            <button type="button" onClick={() => setActivity([])}>
              Clear
            </button>
          </header>
          {activity.map((event, index) => (
            <p key={`${event.event}-log-${index}`}>{event.event}</p>
          ))}
        </section>
      </aside>
    </main>
  );
}

export default App;
