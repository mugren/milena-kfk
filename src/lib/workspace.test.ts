import { describe, expect, it } from "vitest";
import {
  appendPaneActivity,
  assignTopicToPane,
  canStartPaneSession,
  createInitialWorkspaceState,
  getSelectedPane,
  isPaneEmpty,
  markPaneLoading,
  markPaneReady,
  selectPane,
  splitPane,
  stopPane,
} from "./workspace";
import type { MilenaBoundaryEvent, TopicSessionPreview } from "./tauri";

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
  it("starts with one empty pane and no restored session activity", () => {
    const workspace = createInitialWorkspaceState();
    const pane = getSelectedPane(workspace);

    expect(workspace.layout).toBe("single");
    expect(workspace.panes).toHaveLength(1);
    expect(pane).toBeDefined();
    expect(isPaneEmpty(pane!)).toBe(true);
    expect(canStartPaneSession(pane)).toBe(false);
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
    const twoPane = splitPane(createInitialWorkspaceState(), 1, "right");
    const fourPane = splitPane(twoPane, 1, "top");

    expect(fourPane.layout).toBe("quad");
    expect(fourPane.panes).toHaveLength(4);
    expect(fourPane.panes.filter(isPaneEmpty)).toHaveLength(4);
    expect(fourPane.selectedPaneId).toBe(3);
  });

  it("tracks selected pane state only for existing panes", () => {
    const workspace = splitPane(createInitialWorkspaceState(), 1, "right");
    const selected = selectPane(workspace, 1);
    const unchanged = selectPane(selected, 99);

    expect(selected.selectedPaneId).toBe(1);
    expect(getSelectedPane(selected)?.id).toBe(1);
    expect(unchanged).toBe(selected);
  });

  it("does not start empty panes", () => {
    const workspace = createInitialWorkspaceState();
    const loading = markPaneLoading(workspace, 1, "poll");

    expect(loading).toBe(workspace);
    expect(getSelectedPane(loading)?.status).toBe("idle");
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
});
