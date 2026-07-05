import { describe, expect, it, vi } from "vitest";
import {
  createDockviewWorkspaceOptions,
  getDockviewDropDecision,
  handleDockviewWillDrop,
  mapWorkspaceToDockview,
  reconcileDockviewSnapshot,
  toDockviewGroupId,
  toDockviewPanelId,
} from "./dockviewWorkspace";
import {
  createInitialWorkspaceState,
  moveActiveTabToGroup,
  openTopicInFocusedGroup,
  selectTab,
} from "./workspace";
import type { WorkspaceState } from "./workspace";

describe("dockview workspace adapter", () => {
  it("maps canonical groups and tabs to stable Dockview ids without copying Kafka state", () => {
    const workspace = withTopics([
      "orders.created",
      "payments.authorized",
      "inventory.reserved",
    ]);
    const split = moveActiveTabToGroup(selectTab(workspace, 2), "right").workspace;

    const dockview = mapWorkspaceToDockview(split);

    expect(dockview.groups).toEqual([
      {
        id: toDockviewGroupId(1),
        activePanelId: toDockviewPanelId(1),
        panelIds: [toDockviewPanelId(1), toDockviewPanelId(3)],
      },
      {
        id: toDockviewGroupId(2),
        activePanelId: toDockviewPanelId(2),
        panelIds: [toDockviewPanelId(2)],
      },
    ]);
    expect(dockview.activeGroupId).toBe(toDockviewGroupId(2));
    expect(dockview.panels).toEqual([
      {
        id: toDockviewPanelId(1),
        tabId: 1,
        title: "orders.created",
        component: "topicPanel",
      },
      {
        id: toDockviewPanelId(3),
        tabId: 3,
        title: "inventory.reserved",
        component: "topicPanel",
      },
      {
        id: toDockviewPanelId(2),
        tabId: 2,
        title: "payments.authorized",
        component: "topicPanel",
      },
    ]);
  });

  it("updates focused group and active tab from a valid Dockview snapshot", () => {
    const workspace = splitWorkspace();

    const result = reconcileDockviewSnapshot(workspace, {
      activeGroupId: toDockviewGroupId(1),
      activePanelId: toDockviewPanelId(3),
      groups: [
        {
          id: toDockviewGroupId(1),
          activePanelId: toDockviewPanelId(3),
          panelIds: [toDockviewPanelId(1), toDockviewPanelId(3)],
        },
        {
          id: toDockviewGroupId(2),
          activePanelId: toDockviewPanelId(2),
          panelIds: [toDockviewPanelId(2)],
        },
      ],
    });

    expect(result.status).toBe("applied");
    expect(result.workspace.focusedGroupId).toBe(1);
    expect(result.workspace.groups[0].activeTabId).toBe(3);
    expect(result.prunePanelIds).toEqual([]);
    expect(result.rejectedGroupIds).toEqual([]);
  });

  it("applies valid Dockview drag layouts while preserving tab state", () => {
    const workspace = splitWorkspace();

    const result = reconcileDockviewSnapshot(workspace, {
      activeGroupId: "dockview-generated-group",
      activePanelId: toDockviewPanelId(2),
      groups: [
        {
          id: toDockviewGroupId(1),
          activePanelId: toDockviewPanelId(1),
          panelIds: [toDockviewPanelId(1), toDockviewPanelId(3)],
        },
        {
          id: "dockview-generated-group",
          activePanelId: toDockviewPanelId(2),
          panelIds: [toDockviewPanelId(2)],
        },
      ],
    });

    expect(result.status).toBe("applied");
    expect(result.workspace.groups).toHaveLength(2);
    expect(result.workspace.groups[0]).toMatchObject({
      id: 1,
      activeTabId: 1,
    });
    expect(result.workspace.groups[0].tabs.map((tab) => tab.id)).toEqual([1, 3]);
    expect(result.workspace.groups[1]).toMatchObject({
      id: 3,
      activeTabId: 2,
    });
    expect(result.workspace.groups[1].tabs.map((tab) => tab.id)).toEqual([2]);
    expect(result.workspace.focusedGroupId).toBe(3);
    expect(result.workspace.selectedPaneId).toBe(2);
    expect(result.workspace.nextGroupId).toBe(4);
  });

  it("rejects unexpected Dockview groups and panels instead of accepting them into workspace state", () => {
    const workspace = splitWorkspace();

    const result = reconcileDockviewSnapshot(workspace, {
      activeGroupId: "group-99",
      activePanelId: "tab-999",
      groups: [
        {
          id: toDockviewGroupId(1),
          activePanelId: toDockviewPanelId(1),
          panelIds: [toDockviewPanelId(1), toDockviewPanelId(3), "tab-999"],
        },
        {
          id: "group-99",
          activePanelId: "tab-999",
          panelIds: ["tab-999"],
        },
      ],
    });

    expect(result.status).toBe("rejected");
    expect(result.workspace).toBe(workspace);
    expect(result.prunePanelIds).toEqual(["tab-999"]);
    expect(result.rejectedGroupIds).toEqual(["group-99"]);
  });

  it("blocks unsupported Dockview drops and practical cap violations", () => {
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

    expect(
      getDockviewDropDecision(workspace, {
        position: "left",
        sourcePanelId: toDockviewPanelId(1),
        targetGroupId: toDockviewGroupId(1),
      }),
    ).toEqual({ allow: false, reason: "unsupported-position" });
    expect(
      getDockviewDropDecision(workspace, {
        position: "above",
        sourcePanelId: toDockviewPanelId(1),
        targetGroupId: toDockviewGroupId(1),
      }),
    ).toEqual({ allow: false, reason: "unsupported-position" });
    expect(
      getDockviewDropDecision(workspace, {
        position: "center",
        sourcePanelId: toDockviewPanelId(1),
        targetGroupId: toDockviewGroupId(2),
      }),
    ).toEqual({ allow: false, reason: "tab-limit" });
    expect(
      getDockviewDropDecision(workspace, {
        position: "right",
        sourcePanelId: "tab-999",
        targetGroupId: toDockviewGroupId(1),
      }),
    ).toEqual({ allow: false, reason: "unknown-panel" });

    let cappedGroups = withTopics([
      "cap.0",
      "cap.1",
      "cap.2",
      "cap.3",
      "cap.4",
      "cap.5",
      "cap.6",
      "cap.7",
    ]);
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 8), "right")
      .workspace;
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 7), "right")
      .workspace;
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 7), "right")
      .workspace;
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 6), "right")
      .workspace;
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 6), "right")
      .workspace;
    cappedGroups = moveActiveTabToGroup(selectTab(cappedGroups, 6), "right")
      .workspace;

    expect(cappedGroups.groups).toHaveLength(4);
    expect(
      getDockviewDropDecision(cappedGroups, {
        position: "bottom",
        sourcePanelId: toDockviewPanelId(2),
        targetGroupId: toDockviewGroupId(cappedGroups.focusedGroupId),
      }),
    ).toEqual({ allow: false, reason: "group-limit" });

    const preventDefault = vi.fn();
    expect(
      handleDockviewWillDrop(workspace, {
        position: "left",
        panel: { id: toDockviewPanelId(1) },
        group: { id: toDockviewGroupId(1) },
        preventDefault,
      }),
    ).toEqual({ allow: false, reason: "unsupported-position" });
    expect(preventDefault).toHaveBeenCalledTimes(1);
  });

  it("blocks floating groups, popouts, and default context menu actions from adapter options", () => {
    const options = createDockviewWorkspaceOptions();

    expect(options.disableFloatingGroups).toBe(true);
    expect(options.getTabContextMenuItems()).toEqual([]);
    expect(options.getTabGroupChipContextMenuItems()).toEqual([]);
  });

  it("keeps adapter mapping and reconciliation idempotent for StrictMode double invocation", () => {
    const workspace = splitWorkspace();
    const snapshot = mapWorkspaceToDockview(workspace);

    expect(mapWorkspaceToDockview(workspace)).toEqual(
      mapWorkspaceToDockview(workspace),
    );

    const first = reconcileDockviewSnapshot(workspace, snapshot);
    const second = reconcileDockviewSnapshot(first.workspace, snapshot);

    expect(first.status).toBe("applied");
    expect(second.status).toBe("applied");
    expect(second.workspace).toEqual(first.workspace);
    expect(second.prunePanelIds).toEqual([]);
    expect(second.rejectedGroupIds).toEqual([]);
  });
});

function splitWorkspace(): WorkspaceState {
  const workspace = withTopics([
    "orders.created",
    "payments.authorized",
    "inventory.reserved",
  ]);
  return moveActiveTabToGroup(selectTab(workspace, 2), "right").workspace;
}

function withTopics(topics: string[]): WorkspaceState {
  return topics.reduce(
    (workspace, topic) => openTopicInFocusedGroup(workspace, topic).workspace,
    createInitialWorkspaceState(),
  );
}
