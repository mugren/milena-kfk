import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, vi } from "vitest";
import AppComponent, {
  Pane as PaneComponent,
  RightRail as RightRailComponent,
  WorkspaceShell as WorkspaceShellComponent,
} from "./App";
import type {
  AppState,
  KafkaConsumerSession,
  KafkaRecordEvent,
  KafkaTopicMetadata,
  MilenaBoundaryEvent,
  PublishKafkaRecordResponse,
  RuntimeAuthConfig,
  SavedEnvironment,
  StopKafkaConsumerSessionResponse,
} from "./lib/tauri";

const tauri = vi.hoisted(() => ({
  loadAppState: vi.fn(),
  listEnvironments: vi.fn(),
  materializeRuntimeAuthConfig: vi.fn(),
  materializeTemporaryRuntimeAuthConfig: vi.fn(),
  saveEnvironment: vi.fn(),
  deleteEnvironment: vi.fn(),
  listKafkaTopics: vi.fn(),
  startKafkaConsumerSession: vi.fn(),
  stopKafkaConsumerSession: vi.fn(),
  publishKafkaRecord: vi.fn(),
}));

export { tauri };

vi.mock("./lib/tauri", async () => {
  const actual = await vi.importActual<typeof import("./lib/tauri")>(
    "./lib/tauri",
  );

  return {
    ...actual,
    loadAppState: tauri.loadAppState,
    listEnvironments: tauri.listEnvironments,
    materializeRuntimeAuthConfig: tauri.materializeRuntimeAuthConfig,
    materializeTemporaryRuntimeAuthConfig: tauri.materializeTemporaryRuntimeAuthConfig,
    saveEnvironment: tauri.saveEnvironment,
    deleteEnvironment: tauri.deleteEnvironment,
    listKafkaTopics: tauri.listKafkaTopics,
    startKafkaConsumerSession: tauri.startKafkaConsumerSession,
    stopKafkaConsumerSession: tauri.stopKafkaConsumerSession,
    publishKafkaRecord: tauri.publishKafkaRecord,
  };
});

export const App = AppComponent;
export const Pane = PaneComponent;
export const RightRail = RightRailComponent;
export const WorkspaceShell = WorkspaceShellComponent;

export const appState: AppState = {
  appName: "Milena",
  version: "0.1.0",
  platform: "macos-dev",
  capabilities: ["command-boundary", "event-channel", "macos-dev-build"],
};

export const topics: KafkaTopicMetadata[] = [
  { name: "orders.created", partitionCount: 12 },
  { name: "milena.issue14.cleanup", partitionCount: 4 },
  { name: "milena.issue14.interop", partitionCount: 3 },
  { name: "payments.authorized", partitionCount: 8 },
  { name: "inventory.adjusted", partitionCount: 6 },
];

export const savedLocalEnvironment: SavedEnvironment = {
  schemaVersion: 1,
  name: "Local Dev",
  brokers: ["localhost:19092"],
  authMode: "plaintext",
  username: null,
  advancedProperties: "",
};

export const savedStagingEnvironment: SavedEnvironment = {
  schemaVersion: 1,
  name: "Staging",
  brokers: ["staging.kafka.internal:9094"],
  authMode: "saslSslScramSha512",
  username: "deploy",
  advancedProperties: "client.dns.lookup=use_all_dns_ips",
};

export const stagingRuntimeAuth: RuntimeAuthConfig = {
  environment: "Staging",
  brokers: ["staging.kafka.internal:9094"],
  properties: {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "deploy",
    "sasl.password": "existing-secret",
    "client.dns.lookup": "use_all_dns_ips",
  },
};

export const topicPinStorageKey = "milena.topicPins.v1";
export const messagePreferencesStorageKey = "milena.messageRenderPreferences.v1";
export const lastSelectedEnvironmentStorageKey = "milena.lastSelectedEnvironment.v1";

let emittedEvents: Record<string, (event: MilenaBoundaryEvent) => void>;
let nextSession: number;

export function resetRendererHarness() {
  emittedEvents = {};
  nextSession = 0;
  installLocalStorage();
  vi.resetAllMocks();

  tauri.loadAppState.mockResolvedValue(appState);
  tauri.listEnvironments.mockResolvedValue({
    environments: [savedLocalEnvironment, savedStagingEnvironment],
  });
  tauri.materializeRuntimeAuthConfig.mockImplementation(async (name) => ({
    environment: name,
    brokers: name === "Staging" ? ["staging.kafka.internal:9094"] : ["localhost:19092"],
    properties:
      name === "Staging"
        ? {
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": "deploy",
            "sasl.password": "existing-secret",
            "client.dns.lookup": "use_all_dns_ips",
          }
        : { "security.protocol": "PLAINTEXT" },
  }));
  tauri.materializeTemporaryRuntimeAuthConfig.mockImplementation(async (request) => ({
    environment: request.name,
    brokers: request.brokers,
    properties:
      request.authMode === "saslSslPlain" || request.authMode === "saslSslScramSha512"
        ? {
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":
              request.authMode === "saslSslPlain" ? "PLAIN" : "SCRAM-SHA-512",
            "sasl.username": request.username ?? "",
            "sasl.password": request.password ?? "existing-secret",
          }
        : { "security.protocol": "PLAINTEXT" },
  }));
  tauri.saveEnvironment.mockImplementation(async (request) => ({
    schemaVersion: 1,
    name: request.name,
    brokers: request.brokers,
    authMode: request.authMode,
    username: request.username,
    advancedProperties: request.advancedProperties,
  }));
  tauri.deleteEnvironment.mockImplementation(async (name) => ({
    name,
    warning: null,
  }));
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
}

export function cleanupRendererHarness() {
  cleanup();
  window.localStorage.clear();
}

export function renderApp() {
  const user = userEvent.setup();
  render(<WorkspaceShell />);
  return { user };
}

export function renderAppChooser() {
  const user = userEvent.setup();
  render(<App />);
  return { user };
}

export async function loadedTopics() {
  if (!queryTopicSelect("orders.created")) {
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  }

  await waitFor(() => expect(topicSelect("orders.created")).toBeVisible());
}

export function topicSelect(topic: string): HTMLElement {
  const button = queryTopicSelect(topic);
  if (!button) {
    throw new Error(`Unable to find topic select button for ${topic}`);
  }

  return button;
}

export function queryTopicSelect(topic: string): HTMLElement | null {
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

export function topicRow(topic: string): HTMLElement {
  const row = topicSelect(topic).closest(".topic");
  if (!row) {
    throw new Error(`Unable to find topic row for ${topic}`);
  }

  return row as HTMLElement;
}

export async function openTopic(
  user: ReturnType<typeof userEvent.setup>,
  topic: string,
  action = "Open",
) {
  await user.click(topicOpenActions(topic));
  await user.click(
    within(await screen.findByRole("menu")).getByRole("menuitem", {
      name: action,
    }),
  );
}

export function topicOpenActions(topic: string): HTMLElement {
  return screen.getByRole("button", { name: `Open actions for ${topic}` });
}

export function pane(id: string): HTMLElement {
  return screen.getByLabelText(`Pane ${id}`);
}

export function messageRowButton(topic: string): HTMLElement {
  const button = screen
    .getAllByRole("button", { name: new RegExp(escapeRegExp(topic), "i") })
    .find((candidate) => candidate.hasAttribute("aria-expanded"));
  if (!button) {
    throw new Error(`Unable to find message row for ${topic}`);
  }

  return button;
}

export function emit(sessionId: string, event: MilenaBoundaryEvent) {
  act(() => {
    emittedEvents[sessionId](event);
  });
}

export function registerSessionEventHandler(
  sessionId: string,
  onEvent: (event: MilenaBoundaryEvent) => void,
) {
  emittedEvents[sessionId] = onEvent;
}

export function consumerSession(
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

export function stoppedSession(
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

export function producerAck(): PublishKafkaRecordResponse {
  return {
    topic: "orders.created",
    partition: 2,
    offset: 42,
    status: "delivered",
  };
}

export function kafkaRecord(
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

export function consumerError(sessionId: string, message: string): MilenaBoundaryEvent {
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
