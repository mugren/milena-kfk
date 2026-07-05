import { describe, expect, it } from "vitest";
import {
  MAX_GROUPS,
  MAX_TABS_PER_GROUP,
  appendPaneActivity,
  assignTopicToPane,
  clearPaneActivity,
  closeTab,
  createInitialWorkspaceState,
  getActiveTab,
  getFocusedGroup,
  getSelectedPane,
  markPaneError,
  markPaneLoading,
  markPanePollingStarted,
  markPanePollingStarting,
  markPaneReady,
  moveActiveTabToGroup,
  openTopicInFocusedGroup,
  openTopicInWorkspace,
  selectTab,
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

describe("workspace tab/group state", () => {
  it("starts without a focused group, tab, or restored session activity", () => {
    const workspace = createInitialWorkspaceState();

    expect(workspace.groups).toEqual([]);
    expect(workspace.focusedGroupId).toBe(0);
    expect(workspace.expandedGroupId).toBeNull();
    expect(workspace.selectedTopic).toBeNull();
    expect(getFocusedGroup(workspace)).toBeUndefined();
    expect(getActiveTab(workspace)).toBeUndefined();
  });

  it("opens the first topic as the first focused group and tab", () => {
    const result = openTopicInFocusedGroup(
      createInitialWorkspaceState(),
      "orders.created",
    );
    const group = getFocusedGroup(result.workspace);
    const tab = getActiveTab(result.workspace);

    expect(result.status).toBe("opened");
    expect(result.workspace.focusedGroupId).toBe(1);
    expect(result.workspace.nextGroupId).toBe(2);
    expect(result.workspace.nextTabId).toBe(2);
    expect(result.workspace.selectedTopic).toBe("orders.created");
    expect(result.workspace.groups).toHaveLength(1);
    expect(group).toMatchObject({ id: 1, activeTabId: 1 });
    expect(tab).toMatchObject({
      id: 1,
      topic: "orders.created",
      title: "orders.created",
      consumerGroup: "milena-preview-1",
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
    });
  });

  it("opens additional topics as focused tabs in the focused group", () => {
    const first = openTopicInFocusedGroup(
      createInitialWorkspaceState(),
      "orders.created",
    ).workspace;
    const second = openTopicInFocusedGroup(
      first,
      "payments.authorized",
    ).workspace;
    const group = getFocusedGroup(second);

    expect(second.groups).toHaveLength(1);
    expect(group?.activeTabId).toBe(2);
    expect(group?.tabs.map((tab) => tab.topic)).toEqual([
      "orders.created",
      "payments.authorized",
    ]);
    expect(getActiveTab(second)?.topic).toBe("payments.authorized");
  });

  it("enumerates duplicate tab titles workspace-wide without renumbering existing tabs", () => {
    let workspace = createInitialWorkspaceState();
    workspace = openTopicInFocusedGroup(workspace, "orders.created").workspace;
    workspace = openTopicInFocusedGroup(workspace, "orders.created").workspace;
    workspace = openTopicInFocusedGroup(workspace, "orders.created").workspace;

    const afterClosingSecond = closeTab(workspace, 2).workspace;
    const reopened = openTopicInFocusedGroup(
      afterClosingSecond,
      "orders.created",
    ).workspace;

    expect(workspace.groups[0].tabs.map((tab) => tab.title)).toEqual([
      "orders.created",
      "orders.created (2)",
      "orders.created (3)",
    ]);
    expect(afterClosingSecond.groups[0].tabs.map((tab) => tab.title)).toEqual([
      "orders.created",
      "orders.created (3)",
    ]);
    expect(reopened.groups[0].tabs.map((tab) => tab.title)).toEqual([
      "orders.created",
      "orders.created (3)",
      "orders.created (2)",
    ]);
  });

  it("returns a tab-limit status without changing state at eight tabs per group", () => {
    let workspace = createInitialWorkspaceState();
    for (let index = 0; index < MAX_TABS_PER_GROUP; index += 1) {
      workspace = openTopicInFocusedGroup(
        workspace,
        `topic.${index}`,
      ).workspace;
    }

    const result = openTopicInFocusedGroup(workspace, "topic.overflow");

    expect(workspace.groups[0].tabs).toHaveLength(MAX_TABS_PER_GROUP);
    expect(result.status).toBe("tab-limit");
    expect(result.workspace).toBe(workspace);
  });

  it("returns a tab-limit status when moving into a full adjacent group", () => {
    let workspace = withTopics([
      "topic.0",
      "topic.1",
      "topic.2",
      "topic.3",
      "topic.4",
      "topic.5",
      "topic.6",
      "topic.7",
    ]);
    for (let tabId = 8; tabId >= 2; tabId -= 1) {
      workspace = moveActiveTabToGroup(selectTab(workspace, tabId), "right")
        .workspace;
    }
    workspace = openTopicInFocusedGroup(workspace, "topic.overflow").workspace;

    const attempted = selectTab(workspace, 1);
    const result = moveActiveTabToGroup(attempted, "right");

    expect(workspace.groups[1].tabs).toHaveLength(MAX_TABS_PER_GROUP);
    expect(result.status).toBe("tab-limit");
    expect(result.workspace).toBe(attempted);
  });

  it("moves the active tab right into an adjacent group when one exists", () => {
    const workspace = withTopics([
      "orders.created",
      "payments.authorized",
      "inventory.adjusted",
    ]);
    const withSecondActive = selectTab(workspace, 2);
    const split = moveActiveTabToGroup(withSecondActive, "right").workspace;
    const focusedBackOnFirst = selectTab(split, 1);

    const moved = moveActiveTabToGroup(focusedBackOnFirst, "right");

    expect(moved.status).toBe("moved");
    expect(moved.workspace.groups).toHaveLength(2);
    expect(moved.workspace.groups[0].tabs.map((tab) => tab.id)).toEqual([3]);
    expect(moved.workspace.groups[0].activeTabId).toBe(3);
    expect(moved.workspace.groups[1].tabs.map((tab) => tab.id)).toEqual([2, 1]);
    expect(moved.workspace.groups[1].activeTabId).toBe(1);
    expect(moved.workspace.focusedGroupId).toBe(moved.workspace.groups[1].id);
  });

  it("moves the active tab bottom by creating an adjacent group and preserving tab-owned state", () => {
    const active = markPaneError(
      appendPaneActivity(
        markPanePollingStarted(
          markPanePollingStarting(
            withTopics(["orders.created"]),
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

    const moved = moveActiveTabToGroup(active, "bottom");
    const movedTab = getActiveTab(moved.workspace);

    expect(moved.status).toBe("moved");
    expect(moved.workspace.groups).toHaveLength(1);
    expect(moved.workspace.focusedGroupId).toBe(2);
    expect(movedTab).toMatchObject({
      id: 1,
      topic: "orders.created",
      title: "orders.created",
      consumerGroup: "group-1",
      mode: "poll",
      status: "error",
      error: "consumer stalled",
    });
    expect(movedTab?.session).toEqual(
      consumerSession("session-1", "group-1", "orders.created"),
    );
    expect(movedTab?.activity).toHaveLength(1);
  });

  it("returns a group-limit status when a move would need a fifth group", () => {
    let workspace = withTopics([
      "topic.0",
      "topic.1",
      "topic.2",
      "topic.3",
      "topic.4",
      "topic.5",
      "topic.6",
      "topic.7",
    ]);
    workspace = moveActiveTabToGroup(workspace, "right").workspace;
    workspace = moveActiveTabToGroup(selectTab(workspace, 7), "right").workspace;
    workspace = moveActiveTabToGroup(selectTab(workspace, 6), "right").workspace;
    workspace = moveActiveTabToGroup(workspace, "right").workspace;
    workspace = moveActiveTabToGroup(selectTab(workspace, 7), "right").workspace;
    workspace = moveActiveTabToGroup(workspace, "right").workspace;

    const result = moveActiveTabToGroup(workspace, "right");

    expect(workspace.groups).toHaveLength(MAX_GROUPS);
    expect(result.status).toBe("group-limit");
    expect(result.workspace).toBe(workspace);
  });

  it("selects nearest left tab after closing or moving the active tab, otherwise right", () => {
    const workspace = withTopics([
      "orders.created",
      "payments.authorized",
      "inventory.adjusted",
    ]);

    const closedMiddle = closeTab(selectTab(workspace, 2), 2).workspace;
    const movedFirst = moveActiveTabToGroup(
      selectTab(closedMiddle, 1),
      "right",
    ).workspace;

    expect(closedMiddle.groups[0].tabs.map((tab) => tab.id)).toEqual([1, 3]);
    expect(closedMiddle.groups[0].activeTabId).toBe(1);
    expect(movedFirst.groups[0].tabs.map((tab) => tab.id)).toEqual([3]);
    expect(movedFirst.groups[0].activeTabId).toBe(3);
  });

  it("returns to empty workspace state after closing the final tab", () => {
    const workspace = withTopics(["orders.created"]);
    const closed = closeTab(workspace, 1);

    expect(closed.status).toBe("closed");
    expect(closed.workspace.groups).toEqual([]);
    expect(closed.workspace.focusedGroupId).toBe(0);
    expect(closed.workspace.selectedTopic).toBeNull();
    expect(getActiveTab(closed.workspace)).toBeUndefined();
  });

  it("keeps temporary pane aliases synchronized for current UI and polling callers", () => {
    const workspace = openTopicInWorkspace(
      createInitialWorkspaceState(),
      "orders.created",
      "selected",
    ).workspace;
    const ready = markPaneReady(
      appendPaneActivity(markPaneLoading(workspace, 1, "poll"), 1, event),
      1,
      session,
    );
    const stopped = stopPane(ready, 1);
    const reassigned = assignTopicToPane(stopped, 1, "payments.authorized");
    const cleared = clearPaneActivity(reassigned, 1);

    expect(cleared.panes).toEqual(cleared.groups[0].tabs);
    expect(cleared.selectedPaneId).toBe(cleared.groups[0].activeTabId);
    expect(getSelectedPane(cleared)).toBe(getActiveTab(cleared));
    expect(cleared.panes[0]).toMatchObject({
      id: 1,
      topic: "payments.authorized",
      title: "payments.authorized",
      consumerGroup: "milena-preview-1",
      mode: "idle",
      status: "idle",
      session: null,
      activity: [],
    });
  });
});

function withTopics(topics: string[]) {
  return topics.reduce(
    (workspace, topic) => openTopicInFocusedGroup(workspace, topic).workspace,
    createInitialWorkspaceState(),
  );
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
