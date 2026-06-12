import { describe, expect, it, vi } from "vitest";
import {
  buildPublishKafkaRecordRequest,
  canSendPublisherRecord,
  createInitialPublisherState,
  formatPublisherPayload,
  getPublisherPaneState,
  openCombinedPublishPollPane,
  openPublisherPane,
  sendPublisherRecord,
  setPublisherKey,
  setPublisherPayload,
  type PublishRecord,
  type PublisherState,
} from "./publisher";
import {
  createInitialWorkspaceState,
  markPanePollingStarted,
  markPanePollingStarting,
  type WorkspaceState,
} from "./workspace";
import type {
  KafkaConsumerSession,
  RuntimeAuthConfig,
  StartKafkaConsumerSessionRequest,
} from "./tauri";

const auth: RuntimeAuthConfig = {
  environment: "local",
  brokers: ["localhost:9092"],
  properties: {},
};

describe("publisher pane flow", () => {
  it("opens a combined publish and poll pane before enabling send", async () => {
    const workspace = workspaceStore(createInitialWorkspaceState());
    const publisher = publisherStore(createInitialPublisherState());
    const startConsumerSession = vi.fn(
      async (request: StartKafkaConsumerSessionRequest) =>
        session("session-1", "group-1", request.topics[0]),
    );

    publisher.updatePublisherState((current) =>
      openPublisherPane(current, 1, "orders.created"),
    );
    const loadingPane = workspace.getWorkspace().panes[0];
    const draftBeforePoll = getPublisherPaneState(
      publisher.getPublisherState(),
      1,
      "orders.created",
    );

    expect(canSendPublisherRecord(loadingPane, draftBeforePoll)).toBe(false);

    await openCombinedPublishPollPane({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...workspace,
      ...publisher,
      startConsumerSession,
    });

    const readyPane = workspace.getWorkspace().panes[0];
    const draft = getPublisherPaneState(
      publisher.getPublisherState(),
      1,
      "orders.created",
    );

    expect(startConsumerSession).toHaveBeenCalledOnce();
    expect(readyPane).toMatchObject({
      topic: "orders.created",
      mode: "poll",
      status: "ready",
      consumerGroup: "group-1",
    });
    expect(draft.topic).toBe("orders.created");
    expect(canSendPublisherRecord(readyPane, draft)).toBe(true);
  });

  it("formats JSON payloads as a secondary draft action", () => {
    let state = openPublisherPane(
      createInitialPublisherState(),
      1,
      "orders.created",
    );
    state = setPublisherPayload(state, 1, "{\"id\":1,\"status\":\"created\"}");

    const formatted = formatPublisherPayload(state, 1);
    const draft = getPublisherPaneState(formatted, 1, "orders.created");

    expect(draft.error).toBeNull();
    expect(draft.payload).toBe(`{
  "id": 1,
  "status": "created"
}`);
  });

  it("blocks invalid JSON before publish in default mode", async () => {
    const workspace = workspaceStore(readyWorkspace());
    const publisher = publisherStore(
      setPublisherPayload(
        openPublisherPane(createInitialPublisherState(), 1, "orders.created"),
        1,
        "{\"id\":",
      ),
    );
    const publishRecord = vi.fn<PublishRecord>();
    const onError = vi.fn();

    const result = await sendPublisherRecord({
      paneId: 1,
      auth,
      ...workspace,
      ...publisher,
      publishRecord,
      onError,
    });

    const draft = getPublisherPaneState(
      publisher.getPublisherState(),
      1,
      "orders.created",
    );
    expect(result).toBeNull();
    expect(publishRecord).not.toHaveBeenCalled();
    expect(draft.status).toBe("error");
    expect(draft.error).toBe("Payload must be valid JSON");
    expect(onError).not.toHaveBeenCalled();
  });

  it("sends an optional Kafka message key when provided", async () => {
    const workspace = workspaceStore(readyWorkspace());
    const publisher = publisherStore(
      setPublisherKey(
        setPublisherPayload(
          openPublisherPane(createInitialPublisherState(), 1, "orders.created"),
          1,
          "{\"id\":1}",
        ),
        1,
        " order-1 ",
      ),
    );
    const publishRecord = vi.fn<PublishRecord>().mockResolvedValue({
      topic: "orders.created",
      partition: 2,
      offset: 42,
      status: "delivered",
    });

    await sendPublisherRecord({
      paneId: 1,
      auth,
      ...workspace,
      ...publisher,
      publishRecord,
      now: () => new Date("2026-06-12T12:00:00.000Z"),
    });

    expect(publishRecord).toHaveBeenCalledWith({
      auth,
      topic: "orders.created",
      key: "order-1",
      payload: "{\"id\":1}",
    });
  });

  it("records producer ack state separately from consumer activity", async () => {
    const workspace = workspaceStore(readyWorkspace());
    const publisher = publisherStore(
      setPublisherPayload(
        openPublisherPane(createInitialPublisherState(), 1, "orders.created"),
        1,
        "{\"id\":1}",
      ),
    );
    const publishRecord = vi.fn<PublishRecord>().mockResolvedValue({
      topic: "orders.created",
      partition: 0,
      offset: 7,
      status: "delivered",
    });
    const onError = vi.fn();

    await sendPublisherRecord({
      paneId: 1,
      auth,
      ...workspace,
      ...publisher,
      publishRecord,
      onError,
      now: () => new Date("2026-06-12T12:00:00.000Z"),
    });

    const draft = getPublisherPaneState(
      publisher.getPublisherState(),
      1,
      "orders.created",
    );
    const pane = workspace.getWorkspace().panes[0];

    expect(draft.status).toBe("delivered");
    expect(draft.ack).toMatchObject({
      topic: "orders.created",
      partition: 0,
      offset: 7,
      status: "delivered",
      sentAt: "2026-06-12T12:00:00.000Z",
    });
    expect(pane.activity).toEqual([]);
    expect(onError).not.toHaveBeenCalled();
  });

  it("keeps send errors in producer status and reports a global error callback", async () => {
    const workspace = workspaceStore(readyWorkspace());
    const publisher = publisherStore(
      setPublisherPayload(
        openPublisherPane(createInitialPublisherState(), 1, "orders.created"),
        1,
        "{\"id\":1}",
      ),
    );
    const publishRecord = vi
      .fn<PublishRecord>()
      .mockRejectedValue(new Error("broker rejected produce"));
    const onError = vi.fn();

    const result = await sendPublisherRecord({
      paneId: 1,
      auth,
      ...workspace,
      ...publisher,
      publishRecord,
      onError,
    });

    const draft = getPublisherPaneState(
      publisher.getPublisherState(),
      1,
      "orders.created",
    );

    expect(result).toBeNull();
    expect(draft.status).toBe("error");
    expect(draft.error).toBe("broker rejected produce");
    expect(draft.ack).toBeNull();
    expect(workspace.getWorkspace().panes[0].activity).toEqual([]);
    expect(onError).toHaveBeenCalledWith("broker rejected produce");
  });

  it("omits the Kafka message key when the key field is blank", () => {
    const pane = readyWorkspace().panes[0];
    const publisher = getPublisherPaneState(
      setPublisherPayload(
        openPublisherPane(createInitialPublisherState(), 1, "orders.created"),
        1,
        "{\"id\":1}",
      ),
      1,
      "orders.created",
    );

    const request = buildPublishKafkaRecordRequest(auth, pane, publisher);

    expect(request.ok).toBe(true);
    if (request.ok) {
      expect(request.request.key).toBeNull();
    }
  });
});

function workspaceStore(initial: WorkspaceState) {
  let workspace = initial;

  return {
    getWorkspace: () => workspace,
    updateWorkspace: (update: (current: WorkspaceState) => WorkspaceState) => {
      workspace = update(workspace);
    },
  };
}

function publisherStore(initial: PublisherState) {
  let publisher = initial;

  return {
    getPublisherState: () => publisher,
    updatePublisherState: (update: (current: PublisherState) => PublisherState) => {
      publisher = update(publisher);
    },
  };
}

function readyWorkspace(): WorkspaceState {
  return markPanePollingStarted(
    markPanePollingStarting(createInitialWorkspaceState(), 1, "orders.created"),
    1,
    session("session-1", "group-1", "orders.created"),
  );
}

function session(
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
