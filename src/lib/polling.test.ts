import { describe, expect, it, vi } from "vitest";
import {
  buildLatestOnlyConsumerRequest,
  defaultTabConsumerGroup,
  closePanePollingSession,
  closeTabPollingSession,
  startPanePollingSession,
  startTabPollingSession,
  stopPanePollingSession,
  type StartConsumerSession,
  type StopConsumerSession,
} from "./polling";
import {
  appendPaneActivity,
  createInitialWorkspaceState,
  markPanePollingStarted,
  markPanePollingStarting,
  moveTabToAdjacentGroup,
  openTopicInFocusedGroup,
  selectTab,
  type WorkspaceState,
  type WorkspaceTab,
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
    const store = workspaceStore(
      openTopicInFocusedGroup(createInitialWorkspaceState(), "orders.created")
        .workspace,
    );
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
      buildLatestOnlyConsumerRequest(
        auth,
        "orders.created",
        defaultTabConsumerGroup(1),
      ),
      expect.any(Function),
    );
    expect(startConsumerSession.mock.calls[0][0].fromBeginning).toBe(false);
    expect(store.getWorkspace().panes[0].consumerGroup).toBe("group-1");
  });

  it("does not start polling without an existing tab for the topic", async () => {
    const store = workspaceStore(
      openTopicInFocusedGroup(createInitialWorkspaceState(), "orders.created")
        .workspace,
    );
    const startConsumerSession = vi.fn<StartConsumerSession>();

    const started = await startTabPollingSession({
      tabId: 1,
      topic: "payments.authorized",
      auth,
      ...store,
      startConsumerSession,
    });

    expect(started).toBeNull();
    expect(startConsumerSession).not.toHaveBeenCalled();
    expect(store.getWorkspace().panes[0]).toMatchObject({
      topic: "orders.created",
      status: "idle",
    });
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

  it("stops the pane session, preserves messages, and attempts cleanup", async () => {
    const store = workspaceStore(
      appendPaneActivity(
        markPanePollingStarted(
          markPanePollingStarting(
            createInitialWorkspaceState(),
            1,
            "orders.created",
          ),
          1,
          session("session-1", "group-1", "orders.created"),
        ),
        1,
        record("session-1", 42),
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
    });
    expect(store.getWorkspace().panes[0].activity).toHaveLength(1);
    expect(recordOffset(store.getWorkspace().panes[0].activity[0])).toBe(42);
  });

  it("closes the pane and still reports cleanup failures", async () => {
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

    await closePanePollingSession({
      paneId: 1,
      ...store,
      stopConsumerSession,
      onStopError,
    });

    expect(stopConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    expect(onStopError).toHaveBeenCalledWith("client cleanup failed");
    expect(store.getWorkspace().panes).toEqual([]);
    expect(store.getWorkspace().selectedPaneId).toBe(0);
  });

  it("closes a hidden tab, cleans up its session, and ignores late events", async () => {
    let workspace = openTopicInFocusedGroup(
      createInitialWorkspaceState(),
      "orders.created",
    ).workspace;
    workspace = openTopicInFocusedGroup(workspace, "payments.authorized").workspace;
    const store = workspaceStore(workspace);
    const onEvent = vi.fn();
    let emitClosedTab: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("closed tab event emitter was not captured");
    };
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementation(async (request, onEvent) => {
        emitClosedTab = onEvent;
        return session(
          "session-hidden",
          request.groupId ?? "group-hidden",
          request.topics[0],
        );
      });
    const stopConsumerSession = vi
      .fn<StopConsumerSession>()
      .mockResolvedValue(stopped("session-hidden", defaultTabConsumerGroup(1)));

    await startTabPollingSession({
      tabId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
      onEvent,
    });
    store.updateWorkspace((current) => selectTab(current, 2));

    await closeTabPollingSession({
      tabId: 1,
      ...store,
      stopConsumerSession,
    });
    emitClosedTab(record("session-hidden", 7));

    expect(stopConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-hidden",
    });
    expect(store.getWorkspace().panes.map((tab) => tab.id)).toEqual([2]);
    expect(tabById(store.getWorkspace(), 2).activity).toEqual([]);
    expect(onEvent).not.toHaveBeenCalled();
  });

  it("records unique group ids per pane", async () => {
    const store = workspaceStore(splitTopicTabs());
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

  it("keeps hidden open tabs polling under the 1,000-message cap", async () => {
    let workspace = openTopicInFocusedGroup(
      createInitialWorkspaceState(),
      "orders.created",
    ).workspace;
    workspace = openTopicInFocusedGroup(workspace, "payments.authorized").workspace;
    const store = workspaceStore(workspace);
    let emitHiddenTab: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("hidden tab event emitter was not captured");
    };
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementation(async (request, onEvent) => {
        emitHiddenTab = onEvent;
        return session(
          "session-hidden",
          request.groupId ?? "group-hidden",
          request.topics[0],
        );
      });

    await startTabPollingSession({
      tabId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
    });
    store.updateWorkspace((current) => selectTab(current, 2));

    for (let offset = 0; offset < 1_005; offset += 1) {
      emitHiddenTab(record("session-hidden", offset));
    }

    const workspaceAfterRecords = store.getWorkspace();
    const hiddenTab = tabById(workspaceAfterRecords, 1);
    const activeTab = tabById(workspaceAfterRecords, 2);
    expect(workspaceAfterRecords.selectedPaneId).toBe(2);
    expect(hiddenTab.activity).toHaveLength(1_000);
    expect(recordOffset(hiddenTab.activity[0])).toBe(1_004);
    expect(recordOffset(hiddenTab.activity[999])).toBe(5);
    expect(activeTab.activity).toEqual([]);
  });

  it("preserves moved tab polling state and keeps routing by tab id", async () => {
    let workspace = openTopicInFocusedGroup(
      createInitialWorkspaceState(),
      "orders.created",
    ).workspace;
    workspace = openTopicInFocusedGroup(workspace, "payments.authorized").workspace;
    const store = workspaceStore(workspace);
    let emitMovedTab: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("moved tab event emitter was not captured");
    };
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementation(async (request, onEvent) => {
        emitMovedTab = onEvent;
        return session(
          "session-moved",
          request.groupId ?? "group-moved",
          request.topics[0],
        );
      });

    await startTabPollingSession({
      tabId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
    });
    emitMovedTab(record("session-moved", 1));
    store.updateWorkspace(
      (current) => moveTabToAdjacentGroup(current, 1, "right").workspace,
    );
    emitMovedTab(consumerError("session-moved", "broker heartbeat failed"));
    emitMovedTab(record("other-session", 2));

    const movedTab = tabById(store.getWorkspace(), 1);
    const untouchedTab = tabById(store.getWorkspace(), 2);
    expect(store.getWorkspace().groups).toHaveLength(2);
    expect(movedTab).toMatchObject({
      id: 1,
      topic: "orders.created",
      consumerGroup: defaultTabConsumerGroup(1),
      mode: "poll",
      status: "error",
      error: "broker heartbeat failed",
    });
    expect(movedTab.session).toMatchObject({ sessionId: "session-moved" });
    expect(movedTab.activity).toHaveLength(2);
    expect(movedTab.activity[0].event).toBe("kafkaConsumerError");
    expect(recordOffset(movedTab.activity[1])).toBe(1);
    expect(untouchedTab.activity).toEqual([]);
  });

  it("isolates one pane failure from another active poll", async () => {
    const store = workspaceStore(splitTopicTabs());
    const onError = vi.fn();
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockRejectedValueOnce(new Error("broker unavailable"))
      .mockImplementationOnce(async (request, onEvent) => {
        const sessionId = request.groupId ?? "session-2";
        onEvent(record(sessionId, 9));
        return session(sessionId, "group-2", "payments.authorized");
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
    const store = workspaceStore(splitTopicTabs());
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

  it("replaces an existing session before starting a new poll", async () => {
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
      .mockRejectedValue(new Error("cleanup timeout"));
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockResolvedValue(session("session-2", "group-2", "orders.created"));

    const started = await startPanePollingSession({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
      stopConsumerSession,
      onStopError,
    });

    expect(stopConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    expect(onStopError).toHaveBeenCalledWith("cleanup timeout");
    expect(started?.sessionId).toBe("session-2");
    expect(store.getWorkspace().panes[0]).toMatchObject({
      topic: "orders.created",
      consumerGroup: "group-2",
      status: "ready",
    });
  });

  it("ignores stale events from a replaced tab polling session", async () => {
    const store = workspaceStore(
      openTopicInFocusedGroup(createInitialWorkspaceState(), "orders.created")
        .workspace,
    );
    const onEvent = vi.fn();
    let emitFirstRun: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("first run event emitter was not captured");
    };
    let emitSecondRun: (event: MilenaBoundaryEvent) => void = () => {
      throw new Error("second run event emitter was not captured");
    };
    const stopConsumerSession = vi
      .fn<StopConsumerSession>()
      .mockResolvedValue(stopped("session-1", defaultTabConsumerGroup(1)));
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementationOnce(async (request, onEvent) => {
        emitFirstRun = onEvent;
        return session(
          "session-1",
          request.groupId ?? "group-1",
          request.topics[0],
        );
      })
      .mockImplementationOnce(async (request, onEvent) => {
        emitSecondRun = onEvent;
        emitFirstRun(record("session-1", 1));
        return session(
          "session-2",
          request.groupId ?? "group-2",
          request.topics[0],
        );
      });

    await startTabPollingSession({
      tabId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
      stopConsumerSession,
      onEvent,
    });
    await startTabPollingSession({
      tabId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
      stopConsumerSession,
      onEvent,
    });
    emitFirstRun(record("session-1", 2));
    emitSecondRun(record("session-2", 3));

    const tab = tabById(store.getWorkspace(), 1);
    expect(stopConsumerSession).toHaveBeenCalledWith({ sessionId: "session-1" });
    expect(tab.session).toMatchObject({ sessionId: "session-2" });
    expect(tab.activity).toHaveLength(1);
    expect(recordOffset(tab.activity[0])).toBe(3);
    expect(onEvent).toHaveBeenCalledTimes(1);
  });

  it("cancels stale async starts through isCurrent and cleans up the new session", async () => {
    const store = workspaceStore(
      openTopicInFocusedGroup(createInitialWorkspaceState(), "orders.created")
        .workspace,
    );
    let current = true;
    const stopConsumerSession = vi
      .fn<StopConsumerSession>()
      .mockResolvedValue(stopped("session-1", "group-1"));
    const startConsumerSession = vi
      .fn<StartConsumerSession>()
      .mockImplementation(async () => {
        current = false;
        return session("session-1", "group-1", "orders.created");
      });

    const started = await startPanePollingSession({
      paneId: 1,
      topic: "orders.created",
      auth,
      ...store,
      startConsumerSession,
      stopConsumerSession,
      isCurrent: () => current,
    });

    expect(started).toBeNull();
    expect(stopConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    expect(store.getWorkspace().panes[0]).toMatchObject({
      topic: "orders.created",
      status: "loading",
      session: null,
      consumerGroup: defaultTabConsumerGroup(1),
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

function tabById(workspace: WorkspaceState, tabId: number): WorkspaceTab {
  const tab = workspace.groups
    .flatMap((group) => group.tabs)
    .find((candidate) => candidate.id === tabId);
  if (!tab) {
    throw new Error(`expected tab ${tabId}`);
  }

  return tab;
}

function splitTopicTabs(): WorkspaceState {
  let workspace = openTopicInFocusedGroup(
    createInitialWorkspaceState(),
    "orders.created",
  ).workspace;
  workspace = openTopicInFocusedGroup(workspace, "payments.authorized").workspace;
  return moveTabToAdjacentGroup(workspace, 2, "right").workspace;
}
