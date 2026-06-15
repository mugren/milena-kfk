import { describe, expect, it } from "vitest";
import {
  appendPaneActivity,
  assignTopicToPane,
  canStartPaneSession,
  clearPaneActivity,
  closePane,
  createInitialWorkspaceState,
  expandPane,
  getSelectedPane,
  isPaneEmpty,
  markPaneLoading,
  markPaneError,
  markPanePollingStarted,
  markPanePollingStarting,
  markPaneReady,
  openTopicInWorkspace,
  restoreExpandedPane,
  selectPane,
  selectTopicPreview,
  splitPane,
  stopPane,
} from "./workspace";
import type {
  KafkaConsumerSession,
  MilenaBoundaryEvent,
  TopicSessionPreview,
} from "./tauri";

const event: MilenaBoundaryEvent = {
  event: "boundaryOpened",
  data: {
    sessionId: "session-1",
    topic: "orders.created",
    mode: "poll",
  },
};

const session: TopicSessionPreview = {
  sessionId: "session-1",
  topic: "orders.created",
  mode: "poll",
  status: "ready",
};

describe("workspace state", () => {
  it("starts without a preloaded empty pane or restored session activity", () => {
    const workspace = createInitialWorkspaceState();
    const pane = getSelectedPane(workspace);

    expect(workspace.layout).toBe("single");
    expect(workspace.selectedTopic).toBeNull();
    expect(workspace.expandedPaneId).toBeNull();
    expect(workspace.panes).toEqual([]);
    expect(pane).toBeUndefined();
    expect(canStartPaneSession(pane)).toBe(false);
  });

  it("selects a topic preview without opening a pane", () => {
    const workspace = selectTopicPreview(
      createInitialWorkspaceState(),
      "orders.created",
    );

    expect(workspace.selectedTopic).toBe("orders.created");
    expect(workspace.selectedPaneId).toBe(0);
    expect(workspace.nextPaneId).toBe(1);
    expect(workspace.panes).toEqual([]);
    expect(getSelectedPane(workspace)).toBeUndefined();
  });

  it("selects a topic preview without changing the selected pane assignment", () => {
    const assigned = assignedWorkspace("orders.created");
    const previewed = selectTopicPreview(assigned, "payments.authorized");

    expect(previewed.selectedTopic).toBe("payments.authorized");
    expect(previewed.selectedPaneId).toBe(1);
    expect(previewed.panes).toHaveLength(1);
    expect(previewed.panes[0]).toMatchObject({
      topic: "orders.created",
      consumerGroup: "milena-preview-1",
      mode: "idle",
      status: "idle",
    });
  });

  it("selects a topic preview without mutating an active polling session", () => {
    const polling = markPanePollingStarted(
      markPanePollingStarting(
        assignedWorkspace("orders.created"),
        1,
        "orders.created",
      ),
      1,
      consumerSession("session-1", "group-1", "orders.created"),
    );
    const previewed = selectTopicPreview(polling, "payments.authorized");

    expect(previewed.selectedTopic).toBe("payments.authorized");
    expect(previewed.panes[0]).toMatchObject({
      topic: "orders.created",
      consumerGroup: "group-1",
      mode: "poll",
      status: "ready",
      session: consumerSession("session-1", "group-1", "orders.created"),
    });
  });

  it("creates the first pane when a topic is assigned into an empty workspace", () => {
    const workspace = assignTopicToPane(
      createInitialWorkspaceState(),
      0,
      "orders.created",
    );
    const pane = getSelectedPane(workspace);

    expect(workspace.selectedPaneId).toBe(1);
    expect(workspace.nextPaneId).toBe(2);
    expect(workspace.panes).toHaveLength(1);
    expect(pane?.topic).toBe("orders.created");
    expect(pane?.consumerGroup).toBe("milena-preview-1");
  });

  it("opens a topic into the first pane without starting Kafka activity", () => {
    const result = openTopicInWorkspace(
      createInitialWorkspaceState(),
      "orders.created",
      "selected",
    );
    const pane = getSelectedPane(result.workspace);

    expect(result.status).toBe("opened");
    expect(result.workspace.layout).toBe("single");
    expect(result.workspace.selectedTopic).toBe("orders.created");
    expect(result.workspace.selectedPaneId).toBe(1);
    expect(result.workspace.panes).toHaveLength(1);
    expect(pane).toMatchObject({
      topic: "orders.created",
      consumerGroup: "milena-preview-1",
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
    });
  });

  it("resets selected pane session state when replacing an active topic", () => {
    const active = markPaneError(
      appendPaneActivity(
        markPanePollingStarted(
          markPanePollingStarting(
            assignedWorkspace("orders.created"),
            1,
            "orders.created",
          ),
          1,
          consumerSession("session-1", "group-1", "orders.created"),
        ),
        1,
        {
          event: "kafkaRecord",
          data: {
            record: {
              sessionId: "session-1",
              topic: "orders.created",
              partition: 0,
              offset: 4,
              key: null,
              payload: "{\"offset\":4}",
            },
          },
        },
      ),
      1,
      "consumer stalled",
    );

    const replaced = openTopicInWorkspace(
      active,
      "payments.authorized",
      "selected",
    ).workspace;

    expect(replaced.panes[0]).toMatchObject({
      topic: "payments.authorized",
      consumerGroup: "milena-preview-1",
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
      error: null,
      tone: "normal",
    });
  });

  it("opens a topic into requested split placements without starting polling", () => {
    const right = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "right",
    ).workspace;

    expect(right.layout).toBe("two-right");
    expect(right.selectedPaneId).toBe(2);
    expect(right.panes.map((pane) => pane.topic)).toEqual([
      "orders.created",
      "payments.authorized",
    ]);
    expect(right.panes[1]).toMatchObject({
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
    });

    const top = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "top",
    ).workspace;

    expect(top.layout).toBe("two-top");
    expect(top.selectedPaneId).toBe(2);
    expect(top.panes.map((pane) => pane.topic)).toEqual([
      "payments.authorized",
      "orders.created",
    ]);

    const bottom = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "bottom",
    ).workspace;

    expect(bottom.layout).toBe("two-top");
    expect(bottom.selectedPaneId).toBe(2);
    expect(bottom.panes.map((pane) => pane.topic)).toEqual([
      "orders.created",
      "payments.authorized",
    ]);
  });

  it("rejects split topic opens beyond four panes without changing workspace", () => {
    const fullWorkspace = splitPane(
      splitPane(assignedWorkspace("orders.created"), 1, "right"),
      1,
      "top",
    );
    const result = openTopicInWorkspace(
      fullWorkspace,
      "payments.authorized",
      "right",
    );

    expect(fullWorkspace.panes).toHaveLength(4);
    expect(result.status).toBe("pane-limit");
    expect(result.workspace).toBe(fullWorkspace);
  });

  it("splits right into two panes and selects a newly empty pane", () => {
    const workspace = assignTopicToPane(
      createInitialWorkspaceState(),
      1,
      "orders.created",
    );
    const split = splitPane(workspace, 1, "right");
    const selected = getSelectedPane(split);

    expect(split.layout).toBe("two-right");
    expect(split.panes.map((pane) => pane.id)).toEqual([1, 2]);
    expect(split.selectedPaneId).toBe(2);
    expect(isPaneEmpty(selected!)).toBe(true);
    expect(selected?.topic).toBeNull();
    expect(selected?.session).toBeNull();
    expect(selected?.activity).toEqual([]);
  });

  it("splits top with explicit ordering and no inherited Kafka activity", () => {
    const workspace = assignTopicToPane(
      createInitialWorkspaceState(),
      1,
      "payments.authorized",
    );
    const active = markPaneReady(
      appendPaneActivity(markPaneLoading(workspace, 1, "poll"), 1, event),
      1,
      session,
    );
    const split = splitPane(active, 1, "top");

    expect(split.layout).toBe("two-top");
    expect(split.panes.map((pane) => pane.id)).toEqual([2, 1]);
    expect(isPaneEmpty(split.panes[0])).toBe(true);
    expect(split.panes[1].session?.sessionId).toBe("session-1");
  });

  it("normalizes a second split to a four-pane workspace", () => {
    const twoPane = splitPane(assignedWorkspace("orders.created"), 1, "right");
    const fourPane = splitPane(twoPane, 1, "top");

    expect(fourPane.layout).toBe("quad");
    expect(fourPane.panes).toHaveLength(4);
    expect(fourPane.panes.filter(isPaneEmpty)).toHaveLength(3);
    expect(fourPane.selectedPaneId).toBe(3);
  });

  it("closes one of two panes and leaves the remaining pane full-size", () => {
    const workspace = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "right",
    ).workspace;

    const closed = closePane(workspace, 1);

    expect(closed.layout).toBe("single");
    expect(closed.selectedPaneId).toBe(2);
    expect(closed.panes.map((pane) => pane.topic)).toEqual([
      "payments.authorized",
    ]);
    expect(closed.nextPaneId).toBe(workspace.nextPaneId);
  });

  it("normalizes a four-pane close by dropping leftover empty placeholders", () => {
    const twoPane = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "right",
    ).workspace;
    const fourPane = openTopicInWorkspace(
      splitPane(twoPane, 1, "top"),
      "inventory.adjusted",
      "selected",
    ).workspace;

    const closed = closePane(fourPane, 1);

    expect(fourPane.layout).toBe("quad");
    expect(fourPane.panes.filter(isPaneEmpty)).toHaveLength(1);
    expect(closed.layout).toBe("two-right");
    expect(closed.panes.map((pane) => pane.topic)).toEqual([
      "inventory.adjusted",
      "payments.authorized",
    ]);
    expect(closed.panes.filter(isPaneEmpty)).toHaveLength(0);
    expect(closed.panes.some((pane) => pane.id === closed.selectedPaneId)).toBe(
      true,
    );
  });

  it("expands a pane and restores the prior layout without recreating pane state", () => {
    const workspace = openTopicInWorkspace(
      assignedWorkspace("orders.created"),
      "payments.authorized",
      "right",
    ).workspace;
    const active = markPaneError(
      appendPaneActivity(
        markPanePollingStarted(
          markPanePollingStarting(workspace, 2, "payments.authorized"),
          2,
          consumerSession("session-2", "group-2", "payments.authorized"),
        ),
        2,
        {
          event: "kafkaRecord",
          data: {
            record: {
              sessionId: "session-2",
              topic: "payments.authorized",
              partition: 0,
              offset: 4,
              key: null,
              payload: "{\"offset\":4}",
            },
          },
        },
      ),
      2,
      "consumer stalled",
    );

    const expanded = expandPane(active, 2);
    const restored = restoreExpandedPane(expanded);

    expect(active.layout).toBe("two-right");
    expect(expanded.layout).toBe("two-right");
    expect(expanded.expandedPaneId).toBe(2);
    expect(expanded.selectedPaneId).toBe(2);
    expect(expanded.selectedTopic).toBe("payments.authorized");
    expect(expanded.panes).toBe(active.panes);
    expect(expanded.panes[1]).toMatchObject({
      topic: "payments.authorized",
      consumerGroup: "group-2",
      mode: "poll",
      status: "error",
      error: "consumer stalled",
    });
    expect(expanded.panes[1].session).toEqual(
      consumerSession("session-2", "group-2", "payments.authorized"),
    );
    expect(expanded.panes[1].activity).toHaveLength(1);
    expect(restored.layout).toBe("two-right");
    expect(restored.expandedPaneId).toBeNull();
    expect(restored.panes).toBe(active.panes);
    expect(restored.panes[1].error).toBe("consumer stalled");
  });

  it("tracks selected pane state only for existing panes", () => {
    const workspace = splitPane(assignedWorkspace("orders.created"), 1, "right");
    const selected = selectPane(workspace, 1);
    const unchanged = selectPane(selected, 99);

    expect(selected.selectedPaneId).toBe(1);
    expect(getSelectedPane(selected)?.id).toBe(1);
    expect(unchanged).toBe(selected);
  });

  it("does not start empty panes", () => {
    const workspace = splitPane(assignedWorkspace("orders.created"), 1, "right");
    const loading = markPaneLoading(workspace, 2, "poll");

    expect(loading).toBe(workspace);
    expect(loading.panes[1].status).toBe("idle");
  });

  it("starts, stops, and preserves pane assignment", () => {
    const assigned = assignTopicToPane(
      createInitialWorkspaceState(),
      1,
      "orders.created",
    );
    const ready = markPaneReady(
      appendPaneActivity(markPaneLoading(assigned, 1, "poll"), 1, event),
      1,
      session,
    );
    const stopped = stopPane(ready, 1);
    const pane = getSelectedPane(stopped);

    expect(pane?.topic).toBe("orders.created");
    expect(pane?.consumerGroup).toBe("milena-preview-1");
    expect(pane?.mode).toBe("idle");
    expect(pane?.status).toBe("idle");
    expect(pane?.session).toBeNull();
    expect(pane?.activity).toEqual([]);
  });

  it("rejects pane events while idle and from stale sessions", () => {
    const idle = appendPaneActivity(assignedWorkspace("orders.created"), 1, event);
    const ready = markPanePollingStarted(
      markPanePollingStarting(
        assignedWorkspace("orders.created"),
        1,
        "orders.created",
      ),
      1,
      consumerSession("session-1", "group-1", "orders.created"),
    );
    const stale = appendPaneActivity(ready, 1, {
      event: "kafkaRecord",
      data: {
        record: {
          sessionId: "session-2",
          topic: "orders.created",
          partition: 0,
          offset: 12,
          key: null,
          payload: "{\"offset\":12}",
        },
      },
    });

    expect(idle.panes[0].activity).toEqual([]);
    expect(stale.panes[0].activity).toEqual([]);
  });

  it("resets pane session state when a new poll starts", () => {
    const active = markPaneError(
      appendPaneActivity(
        markPanePollingStarted(
          markPanePollingStarting(
            assignedWorkspace("orders.created"),
            1,
            "orders.created",
          ),
          1,
          consumerSession("session-1", "group-1", "orders.created"),
        ),
        1,
        {
          event: "kafkaRecord",
          data: {
            record: {
              sessionId: "session-1",
              topic: "orders.created",
              partition: 0,
              offset: 4,
              key: null,
              payload: "{\"offset\":4}",
            },
          },
        },
      ),
      1,
      "consumer stalled",
    );

    const restarting = markPanePollingStarting(
      active,
      1,
      "payments.authorized",
    );

    expect(restarting.panes[0]).toMatchObject({
      topic: "payments.authorized",
      consumerGroup: null,
      mode: "poll",
      status: "loading",
      session: null,
      activity: [],
      error: null,
      tone: "normal",
    });
  });

  it("clears pane activity without stopping the active poll session", () => {
    const active = appendPaneActivity(
      markPanePollingStarted(
        markPanePollingStarting(
          assignedWorkspace("orders.created"),
          1,
          "orders.created",
        ),
        1,
        consumerSession("session-1", "group-1", "orders.created"),
      ),
      1,
      {
        event: "kafkaRecord",
        data: {
          record: {
            sessionId: "session-1",
            topic: "orders.created",
            partition: 0,
            offset: 4,
            key: null,
            payload: "{\"offset\":4}",
          },
        },
      },
    );

    const cleared = clearPaneActivity(active, 1);

    expect(cleared.panes[0]).toMatchObject({
      topic: "orders.created",
      consumerGroup: "group-1",
      mode: "poll",
      status: "ready",
      session: consumerSession("session-1", "group-1", "orders.created"),
      activity: [],
      error: null,
      tone: "normal",
    });
  });
});

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

function assignedWorkspace(topic: string) {
  return assignTopicToPane(createInitialWorkspaceState(), 0, topic);
}
