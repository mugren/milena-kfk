import { describe, expect, it, vi } from "vitest";
import {
  buildLatestOnlyConsumerRequest,
  startPanePollingSession,
  stopPanePollingSession,
  type StartConsumerSession,
  type StopConsumerSession,
} from "./polling";
import {
  appendPaneActivity,
  createInitialWorkspaceState,
  markPanePollingStarted,
  markPanePollingStarting,
  splitPane,
  type WorkspaceState,
} from "./workspace";
import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  RuntimeAuthConfig,
} from "./tauri";

const auth: RuntimeAuthConfig = {
  environment: "local",
  brokers: ["localhost:9092"],
  properties: {},
};

describe("pane polling sessions", () => {
  it("starts consumers with latest-only offsets", async () => {
    const store = workspaceStore(createInitialWorkspaceState());
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockResolvedValue(session("session-1", "group-1", "orders.created"));

    await startPanePollingSession({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
    });

    expect(startConsumerSession).toHaveBeenCalledWith(
      buildLatestOnlyConsumerRequest(auth, "orders.created"),
      expect.any(Function),
    );
    expect(startConsumerSession.mock.calls[0][0].fromBeginning).toBe(false);
    expect(store.getWorkspace().panes[0].consumerGroup).toBe("group-1");
  });

  it("keeps the latest 1,000 pane messages", () => {
    let workspace = markPanePollingStarted(
      markPanePollingStarting(createInitialWorkspaceState(), 1, "orders.created"),
      1,
      session("session-1", "group-1", "orders.created"),
    );

    for (let offset = 0; offset < 1_005; offset += 1) {
      workspace = appendPaneActivity(workspace, 1, record("session-1", offset));
    }

    const pane = workspace.panes[0];
    expect(pane.activity).toHaveLength(1_000);
    expect(recordOffset(pane.activity[0])).toBe(1_004);
    expect(recordOffset(pane.activity[999])).toBe(5);
  });

  it("stops the pane session and attempts cleanup", async () => {
    const store = workspaceStore(
      markPanePollingStarted(
        markPanePollingStarting(
          createInitialWorkspaceState(),
          1,
          "orders.created",
        ),
        1,
        session("session-1", "group-1", "orders.created"),
      ),
    );
    const stopConsumerSession = vi
      .fn<StopConsumerSession>()
      .mockResolvedValue(stopped("session-1", "group-1"));

    await stopPanePollingSession({
      paneId: 1,
      ...store,
      stopConsumerSession,
    });

    expect(stopConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    expect(store.getWorkspace().panes[0]).toMatchObject({
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
    });
  });

  it("records unique group ids per pane", async () => {
    const store = workspaceStore(splitPane(createInitialWorkspaceState(), 1, "right"));
    let nextGroup = 0;
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementation(async (request) => {
        nextGroup += 1;
        return session(
          `session-${nextGroup}`,
          `group-${nextGroup}`,
          request.topics[0],
        );
      });

    await startPanePollingSession({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
    });
    await startPanePollingSession({
      paneId: 2,
      topic: "payments.authorized",
      auth,
      ...store,
      startConsumerSession,
    });

    expect(store.getWorkspace().panes.map((pane) => pane.consumerGroup)).toEqual([
      "group-1",
      "group-2",
    ]);
  });

  it("isolates one pane failure from another active poll", async () => {
    const store = workspaceStore(splitPane(createInitialWorkspaceState(), 1, "right"));
    const onError = vi.fn();
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockRejectedValueOnce(new Error("broker unavailable"))
      .mockImplementationOnce(async (_request, onEvent) => {
        onEvent(record("session-2", 9));
        return session("session-2", "group-2", "payments.authorized");
      });

    await Promise.all([
      startPanePollingSession({
        paneId: 1,
        topic: "orders.created",
        auth,
        ...store,
        startConsumerSession,
        onError,
      }),
      startPanePollingSession({
        paneId: 2,
        topic: "payments.authorized",
        auth,
        ...store,
        startConsumerSession,
      }),
    ]);

    const [failedPane, activePane] = store.getWorkspace().panes;
    expect(failedPane.status).toBe("error");
    expect(failedPane.error).toBe("broker unavailable");
    expect(onError).toHaveBeenCalledWith("broker unavailable");
    expect(activePane.status).toBe("ready");
    expect(activePane.consumerGroup).toBe("group-2");
    expect(activePane.activity).toHaveLength(1);
    expect(recordOffset(activePane.activity[0])).toBe(9);
  });

  it("renders runtime consumer errors on the affected pane only", async () => {
    const store = workspaceStore(splitPane(createInitialWorkspaceState(), 1, "right"));
    let emitPaneOne: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("pane one event emitter was not captured");
    };
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementationOnce(async (_request, onEvent) => {
        emitPaneOne = onEvent;
        return session("session-1", "group-1", "orders.created");
      })
      .mockResolvedValueOnce(
        session("session-2", "group-2", "payments.authorized"),
      );

    await startPanePollingSession({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
    });
    await startPanePollingSession({
      paneId: 2,
      topic: "payments.authorized",
      auth,
      ...store,
      startConsumerSession,
    });

    emitPaneOne(consumerError("session-1", "broker heartbeat failed"));

    const [failedPane, activePane] = store.getWorkspace().panes;
    expect(failedPane.status).toBe("error");
    expect(failedPane.error).toBe("broker heartbeat failed");
    expect(failedPane.activity).toHaveLength(1);
    expect(activePane.status).toBe("ready");
    expect(activePane.error).toBeNull();
  });

  it("reports cleanup failures without restoring a stopped pane", async () => {
    const store = workspaceStore(
      markPanePollingStarted(
        markPanePollingStarting(
          createInitialWorkspaceState(),
          1,
          "orders.created",
        ),
        1,
        session("session-1", "group-1", "orders.created"),
      ),
    );
    const onStopError = vi.fn();
    const stopConsumerSession = vi
      .fn<StopConsumerSession>()
      .mockRejectedValue(new Error("client cleanup failed"));

    await stopPanePollingSession({
      paneId: 1,
      ...store,
      stopConsumerSession,
      onStopError,
    });

    expect(onStopError).toHaveBeenCalledWith("client cleanup failed");
    expect(store.getWorkspace().panes[0]).toMatchObject({
      mode: "idle",
      status: "idle",
      session: null,
    });
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

function stopped(sessionId: string, groupId: string) {
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

function record(sessionId: string, offset: number): MilenaBoundaryEvent {
  return {
    event: "kafkaRecord",
    data: {
      record: {
        sessionId,
        topic: "orders.created",
        partition: 0,
        offset,
        key: null,
        payload: `{"offset":${offset}}`,
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

function recordOffset(event: MilenaBoundaryEvent) {
  if (event.event !== "kafkaRecord") {
    throw new Error("expected kafka record");
  }

  return event.data.record.offset;
}
