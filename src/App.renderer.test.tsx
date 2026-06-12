/**
 * @vitest-environment jsdom
 */
import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type {
  AppState,
  KafkaConsumerSession,
  KafkaRecordEvent,
  KafkaTopicMetadata,
  MilenaBoundaryEvent,
  PublishKafkaRecordResponse,
  StopKafkaConsumerSessionResponse,
} from "./lib/tauri";

const tauri = vi.hoisted(() => ({
  loadAppState: vi.fn(),
  listKafkaTopics: vi.fn(),
  startKafkaConsumerSession: vi.fn(),
  stopKafkaConsumerSession: vi.fn(),
  publishKafkaRecord: vi.fn(),
}));

vi.mock("./lib/tauri", async () => {
  const actual = await vi.importActual<typeof import("./lib/tauri")>(
    "./lib/tauri",
  );

  return {
    ...actual,
    loadAppState: tauri.loadAppState,
    listKafkaTopics: tauri.listKafkaTopics,
    startKafkaConsumerSession: tauri.startKafkaConsumerSession,
    stopKafkaConsumerSession: tauri.stopKafkaConsumerSession,
    publishKafkaRecord: tauri.publishKafkaRecord,
  };
});

const appState: AppState = {
  appName: "Milena",
  version: "0.1.0",
  platform: "macos-dev",
  capabilities: ["command-boundary", "event-channel", "macos-dev-build"],
};

const topics: KafkaTopicMetadata[] = [
  { name: "orders.created", partitionCount: 12 },
  { name: "payments.authorized", partitionCount: 8 },
  { name: "inventory.adjusted", partitionCount: 6 },
];

const topicPinStorageKey = "milena.topicPins.v1";
const messagePreferencesStorageKey = "milena.messageRenderPreferences.v1";

let emittedEvents: Record<string, (event: MilenaBoundaryEvent) => void>;
let nextSession: number;

beforeEach(() => {
  emittedEvents = {};
  nextSession = 0;
  installLocalStorage();
  vi.resetAllMocks();

  tauri.loadAppState.mockResolvedValue(appState);
  tauri.listKafkaTopics.mockResolvedValue({ topics });
  tauri.startKafkaConsumerSession.mockImplementation(async (request, onEvent) => {
    nextSession += 1;
    const session = consumerSession(
      `session-${nextSession}`,
      `group-${nextSession}`,
      request.topics[0],
    );
    emittedEvents[session.sessionId] = onEvent;
    return session;
  });
  tauri.stopKafkaConsumerSession.mockImplementation(async ({ sessionId }) =>
    stoppedSession(sessionId, sessionId.replace("session", "group")),
  );
  tauri.publishKafkaRecord.mockResolvedValue(producerAck());
});

afterEach(() => {
  cleanup();
  window.localStorage.clear();
});

describe("App renderer flow harness", () => {
  it("renders the shell, loads topics, persists pins, selects topics, and recovers through manual refresh", async () => {
    tauri.listKafkaTopics
      .mockRejectedValueOnce(new Error("SASL auth failed"))
      .mockResolvedValueOnce({ topics });

    const { user } = renderApp();

    expect(
      screen.getByRole("heading", { level: 1, name: "Empty pane" }),
    ).toBeVisible();
    expect(screen.getByLabelText("Pane 1")).toHaveTextContent("Inactive");

    expect((await screen.findAllByText("SASL auth failed"))[0]).toBeVisible();
    expect(screen.getByText("Topic list refresh failed")).toBeVisible();
    expect(queryTopicSelect("orders.created")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.queryByText("Topic list refresh failed")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledTimes(2));
    await loadedTopics();

    await user.type(screen.getByLabelText("Search topics"), "pay");
    expect(
      topicSelect("payments.authorized"),
    ).toBeVisible();
    expect(
      queryTopicSelect("orders.created"),
    ).not.toBeInTheDocument();

    await user.clear(screen.getByLabelText("Search topics"));
    await user.click(screen.getByRole("button", { name: "Pin orders.created" }));
    expect(screen.getByRole("button", { name: "Unpin orders.created" }))
      .toHaveAttribute("aria-pressed", "true");
    expect(window.localStorage.getItem(topicPinStorageKey)).toContain(
      "orders.created",
    );

    await user.click(
      topicSelect("orders.created"),
    );
    expect(
      screen.getByRole("heading", { level: 1, name: "orders.created" }),
    ).toBeVisible();
    expect(screen.getByText("12 partitions / JSON")).toBeVisible();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "orders.created",
    );
    expect(tauri.loadAppState).toHaveBeenCalledOnce();
  });

  it("routes topic selection into the active pane and enforces the four-pane split limit", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await user.click(
      topicSelect("orders.created"),
    );
    await user.click(
      within(pane("1")).getByRole("button", { name: "Split pane 1 right" }),
    );

    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Pane 2");
    expect(pane("2")).toHaveTextContent("Empty pane");

    await user.click(topicSelect("payments.authorized"));
    expect(pane("2")).toHaveTextContent("payments.authorized");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "payments.authorized",
    );
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "2 panes / right",
    );

    await user.click(pane("1"));
    await user.click(
      within(pane("1")).getByRole("button", { name: "Split pane 1 top" }),
    );

    expect(screen.getAllByLabelText(/Pane \d/)).toHaveLength(4);
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("2 x 2");
    for (const button of screen.getAllByRole("button", { name: /Split pane/ })) {
      expect(button).toBeDisabled();
    }
  });

  it("starts latest-only polling, renders delivered events locally, records global activity, and stops cleanly", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await user.click(
      topicSelect("orders.created"),
    );

    await user.click(
      workspaceToolbarButton("Poll"),
    );

    await waitFor(() => expect(tauri.startKafkaConsumerSession).toHaveBeenCalledOnce());
    expect(tauri.startKafkaConsumerSession).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: ["orders.created"],
        fromBeginning: false,
      }),
      expect.any(Function),
    );
    await screen.findByText("session-1");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("group-1");

    emit(
      "session-1",
      kafkaRecord("session-1", {
        offset: 42,
        payload: "{\"offset\":42}",
      }),
    );
    expect(await screen.findByText('{"offset":42}')).toBeVisible();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "orders.created p0 / 42",
    );

    await user.click(within(pane("1")).getByRole("button", { name: "Stop" }));

    await waitFor(() =>
      expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
        sessionId: "session-1",
      }),
    );
    expect(pane("1")).toHaveTextContent("idle");
    expect(pane("1")).toHaveTextContent("Inactive");
  });

  it("isolates multi-pane consumer events and errors", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await user.click(
      topicSelect("orders.created"),
    );
    await user.click(
      within(pane("1")).getByRole("button", { name: "Split pane 1 right" }),
    );
    await user.click(topicSelect("payments.authorized"));

    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");
    await user.click(within(pane("2")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-2");

    emit("session-2", kafkaRecord("session-2", {
      topic: "payments.authorized",
      offset: 7,
      payload: "{\"payment\":true}",
    }));
    emit("session-1", consumerError("session-1", "broker heartbeat failed"));

    expect(pane("1")).toHaveTextContent("broker heartbeat failed");
    expect(pane("1")).not.toHaveTextContent("{\"payment\":true}");
    expect(pane("2")).toHaveTextContent("{\"payment\":true}");
    expect(pane("2")).not.toHaveTextContent("broker heartbeat failed");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("P1");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("P2");
  });

  it("runs the combined publish flow, gates sends, trims optional keys, formats JSON, and displays producer failures", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await user.click(
      topicSelect("orders.created"),
    );

    const send = within(pane("1")).getByRole("button", { name: "Send" });
    expect(send).toBeDisabled();

    await user.click(
      within(screen.getByLabelText("Milena workspace")).getByRole("button", {
        name: "Publish",
      }),
    );
    await screen.findByText("Ready to publish");

    const key = screen.getByLabelText("Kafka key for pane 1");
    const payload = screen.getByLabelText("JSON payload for pane 1");
    fireEvent.change(key, { target: { value: " order-1 " } });
    fireEvent.change(payload, { target: { value: "{\"id\":1,\"ok\":true}" } });

    await user.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(tauri.publishKafkaRecord).toHaveBeenCalledOnce());
    expect(tauri.publishKafkaRecord).toHaveBeenLastCalledWith(
      expect.objectContaining({
        topic: "orders.created",
        key: "order-1",
        payload: "{\"id\":1,\"ok\":true}",
      }),
    );
    expect(await screen.findByText("delivered / p2 / 42")).toBeVisible();

    fireEvent.change(key, { target: { value: "   " } });
    await user.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(tauri.publishKafkaRecord).toHaveBeenCalledTimes(2));
    expect(tauri.publishKafkaRecord).toHaveBeenLastCalledWith(
      expect.objectContaining({ key: null }),
    );

    fireEvent.change(payload, { target: { value: "{\"id\":" } });
    expect(screen.getByText("Payload must be valid JSON")).toBeVisible();
    expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Format JSON" }));
    expect(screen.getAllByText("Payload must be valid JSON").length).toBeGreaterThan(0);

    fireEvent.change(payload, { target: { value: "{\"id\":2}" } });
    await user.click(screen.getByRole("button", { name: "Format JSON" }));
    expect(payload).toHaveValue(`{
  "id": 2
}`);

    tauri.publishKafkaRecord.mockRejectedValueOnce(
      new Error("broker rejected produce"),
    );
    await user.click(screen.getByRole("button", { name: "Send" }));
    expect((await screen.findAllByText("broker rejected produce"))[0])
      .toBeVisible();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "Publish failed",
    );
  });

  it("persists render mode, expands rows with headers, and falls back for invalid JSON payloads", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await user.click(
      topicSelect("orders.created"),
    );
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");

    emit("session-1", kafkaRecord("session-1", {
      key: "order-42",
      offset: 42,
      payload: "{\"id\":",
      headers: [{ key: "trace-id", value: "abc" }],
    }));

    expect(await screen.findByText("invalid JSON")).toBeVisible();
    expect(screen.getByText("{\"id\":")).toBeVisible();

    await user.selectOptions(
      screen.getByLabelText("Render mode for orders.created"),
      "raw",
    );
    expect(window.localStorage.getItem(messagePreferencesStorageKey)).toContain(
      "raw",
    );
    expect(screen.queryByText("invalid JSON")).not.toBeInTheDocument();

    await user.click(messageRowButton("orders.created"));
    expect(messageRowButton("orders.created")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByLabelText("Kafka headers")).toHaveTextContent("trace-id");
    expect(screen.getByLabelText("Kafka headers")).toHaveTextContent("abc");
    expect(screen.getByText("key order-42")).toBeVisible();
  });

  it("opens and closes the topic context menu and routes actions to a targeted pane", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await user.click(
      topicSelect("orders.created"),
    );
    await user.click(
      within(pane("1")).getByRole("button", { name: "Split pane 1 right" }),
    );
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Pane 2");

    await user.pointer([
      {
        target: topicSelect("payments.authorized"),
      },
      "[MouseRight]",
    ]);
    const menu = await screen.findByRole("menu");
    expect(menu).toHaveTextContent("payments.authorized");
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();

    await user.pointer([
      {
        target: topicSelect("payments.authorized"),
      },
      "[MouseRight]",
    ]);
    const publishToPaneOne = within(await screen.findByRole("menu"))
      .getAllByRole("menuitem", { name: "Publish" })[0];
    await user.click(publishToPaneOne);

    await waitFor(() => expect(tauri.startKafkaConsumerSession).toHaveBeenCalledOnce());
    expect(tauri.startKafkaConsumerSession).toHaveBeenCalledWith(
      expect.objectContaining({ topics: ["payments.authorized"] }),
      expect.any(Function),
    );
    expect(pane("1")).toHaveTextContent("payments.authorized");
    expect(pane("2")).toHaveTextContent("Empty pane");
  });

  it("keeps the shell available when the runtime is unavailable and avoids unhandled promise rejections", async () => {
    const unhandled = vi.fn();
    window.addEventListener("unhandledrejection", unhandled);
    tauri.loadAppState.mockRejectedValueOnce(new Error("invoke missing"));
    tauri.listKafkaTopics.mockRejectedValueOnce(new Error("IPC unavailable"));

    const { user } = renderApp();

    expect(
      screen.getByRole("heading", { level: 1, name: "Empty pane" }),
    ).toBeVisible();
    expect(await screen.findByText("Tauri runtime unavailable")).toBeVisible();
    expect(screen.getByText("invoke missing")).toBeVisible();
    expect(screen.getByText("Topic list refresh failed")).toBeVisible();
    expect(screen.getAllByText("IPC unavailable")[0]).toBeVisible();
    expect(unhandled).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Inactive");
    expect(screen.getByLabelText("Activity log")).not.toHaveTextContent(
      "IPC unavailable",
    );
    window.removeEventListener("unhandledrejection", unhandled);
  });
});

function renderApp() {
  const user = userEvent.setup();
  render(<App />);
  return { user };
}

async function loadedTopics() {
  await waitFor(() => expect(topicSelect("orders.created")).toBeVisible());
}

function topicSelect(topic: string): HTMLElement {
  const button = queryTopicSelect(topic);
  if (!button) {
    throw new Error(`Unable to find topic select button for ${topic}`);
  }

  return button;
}

function queryTopicSelect(topic: string): HTMLElement | null {
  return (
    screen
      .queryAllByRole("button")
      .find(
        (button) =>
          button.classList.contains("topic-select") &&
          button.textContent?.includes(topic),
      ) ?? null
  );
}

function pane(id: string): HTMLElement {
  return screen.getByLabelText(`Pane ${id}`);
}

function workspaceToolbarButton(name: string): HTMLElement {
  const button = screen
    .getAllByRole("button", { name })
    .find((candidate) => candidate.closest(".toolbar"));
  if (!button) {
    throw new Error(`Unable to find workspace toolbar button ${name}`);
  }

  return button;
}

function messageRowButton(topic: string): HTMLElement {
  const button = screen
    .getAllByRole("button", { name: new RegExp(escapeRegExp(topic), "i") })
    .find((candidate) => candidate.hasAttribute("aria-expanded"));
  if (!button) {
    throw new Error(`Unable to find message row for ${topic}`);
  }

  return button;
}

function emit(sessionId: string, event: MilenaBoundaryEvent) {
  act(() => {
    emittedEvents[sessionId](event);
  });
}

function consumerSession(
  sessionId: string,
  groupId: string,
  topic: string,
): KafkaConsumerSession {
  return {
    sessionId,
    groupId,
    topics: [topic],
    status: "started",
  };
}

function stoppedSession(
  sessionId: string,
  groupId: string,
): StopKafkaConsumerSessionResponse {
  return {
    sessionId,
    groupId,
    status: "stopped",
    cleanup: {
      groupId,
      attempted: true,
      succeeded: true,
      error: null,
    },
  };
}

function producerAck(): PublishKafkaRecordResponse {
  return {
    topic: "orders.created",
    partition: 2,
    offset: 42,
    status: "delivered",
  };
}

function kafkaRecord(
  sessionId: string,
  overrides: Partial<KafkaRecordEvent> = {},
): MilenaBoundaryEvent {
  return {
    event: "kafkaRecord",
    data: {
      record: {
        sessionId,
        topic: "orders.created",
        partition: 0,
        offset: 1,
        key: null,
        payload: "{\"offset\":1}",
        headers: [],
        receivedAt: "2026-06-12T12:00:00.000Z",
        ...overrides,
      },
    },
  };
}

function consumerError(sessionId: string, message: string): MilenaBoundaryEvent {
  return {
    event: "kafkaConsumerError",
    data: {
      sessionId,
      message,
    },
  };
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function installLocalStorage() {
  const values = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => Array.from(values.keys())[index] ?? null,
    removeItem: (key) => {
      values.delete(key);
    },
    setItem: (key, value) => {
      values.set(key, value);
    },
  };

  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: storage,
  });
}
