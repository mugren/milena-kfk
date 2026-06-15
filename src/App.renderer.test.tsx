/**
 * @vitest-environment jsdom
 */
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  cleanupRendererHarness,
  consumerError,
  consumerSession,
  emit,
  kafkaRecord,
  loadedTopics,
  messagePreferencesStorageKey,
  messageRowButton,
  openTopic,
  pane,
  Pane,
  queryTopicSelect,
  registerSessionEventHandler,
  renderApp,
  resetRendererHarness,
  RightRail,
  WorkspaceShell,
  tauri,
  topicOpenActions,
  topicPinStorageKey,
  topicRow,
  topics,
  topicSelect,
} from "./App.renderer.test-utils";
import type { KafkaConsumerSession } from "./lib/tauri";
import type { WorkspacePane } from "./lib/workspace";

beforeEach(resetRendererHarness);
afterEach(cleanupRendererHarness);

describe("App renderer flow harness", () => {
  it("renders the shell without opening a Kafka connection", async () => {
    renderApp();

    expect(document.title).toBe("Milena - Kafka Reader");
    const workspace = screen.getByLabelText("Milena workspace");
    expect(
      within(workspace).queryByRole("heading", { level: 1 }),
    ).not.toBeInTheDocument();
    expect(within(workspace).getByText(/^Select a topic/)).toBeVisible();
    const topicsRail = screen.getByLabelText("Kafka topics");
    expect(within(topicsRail).getByText("Milena - Kafka Reader")).toBeVisible();
    expect(within(topicsRail).getByText("local-dev")).toBeVisible();
    expect(within(topicsRail).queryByText("macos-dev")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("no panes");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Inactive");
    expect(screen.getByText("No topics")).toBeVisible();
    expect(screen.getByText("localhost:19092")).toBeVisible();
    expect(tauri.loadAppState).toHaveBeenCalledOnce();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
    expect(tauri.publishKafkaRecord).not.toHaveBeenCalled();
    // @ts-ignore Vitest runs this assertion in Node; the renderer tsconfig has no Node types.
    const { readFileSync } = await import("node:fs");
    const appCss = readFileSync("src/App.css", "utf8");
    expect(appCss).toContain("font-family: -apple-system");
    expect(appCss).toContain("--topics-rail-width: clamp(320px, 22vw, 360px)");
    expect(appCss).toContain("var(--topics-rail-width)");
    expect(appCss).not.toContain("Inter");
  });

  it("shows change environment only when the workspace can return to the chooser", async () => {
    const onChangeEnvironment = vi.fn();
    renderApp();

    expect(
      screen.queryByRole("button", { name: "Change environment" }),
    ).not.toBeInTheDocument();

    cleanupRendererHarness();
    resetRendererHarness();
    const user = userEvent.setup();
    render(<WorkspaceShell onChangeEnvironment={onChangeEnvironment} />);

    await user.click(screen.getByRole("button", { name: "Change environment" }));

    expect(onChangeEnvironment).toHaveBeenCalledWith("local-dev");
  });

  it("keeps hideable side columns with icon controls without losing pane state", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await user.click(screen.getByRole("button", { name: "Pin orders.created" }));
    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");

    emit(
      "session-1",
      kafkaRecord("session-1", {
        offset: 42,
        payload: "{\"offset\":42}",
      }),
    );
    expect(await screen.findByText('{"offset":42}')).toBeVisible();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");

    const hideTopics = screen.getByRole("button", { name: "Hide topic sidebar" });
    const hideInspector = screen.getByRole("button", {
      name: "Hide inspector column",
    });
    expect(hideTopics).toHaveTextContent("");
    expect(hideInspector).toHaveTextContent("");
    expect(hideTopics.querySelector("svg")).not.toBeNull();
    expect(hideInspector.querySelector("svg")).not.toBeNull();
    expect(screen.getByLabelText("Kafka topics")).toBeVisible();
    expect(screen.getByLabelText("Activity log")).toBeVisible();
    expect(screen.getByRole("main")).not.toHaveClass("topics-hidden");
    expect(screen.getByRole("main")).not.toHaveClass("inspector-hidden");

    await user.click(hideTopics);
    expect(screen.queryByLabelText("Kafka topics")).not.toBeInTheDocument();
    expect(screen.getByRole("main")).toHaveClass("topics-hidden");
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(pane("1")).toHaveTextContent('{"offset":42}');
    const showTopics = screen.getByRole("button", { name: "Show topic sidebar" });
    expect(showTopics).toHaveTextContent("");
    expect(showTopics.querySelector("svg")).not.toBeNull();

    await user.click(showTopics);
    expect(screen.getByLabelText("Kafka topics")).toBeVisible();
    expect(screen.getByRole("main")).not.toHaveClass("topics-hidden");

    await user.click(hideInspector);
    expect(screen.queryByLabelText("Activity log")).not.toBeInTheDocument();
    expect(screen.getByRole("main")).toHaveClass("inspector-hidden");
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(pane("1")).toHaveTextContent('{"offset":42}');
    const showInspector = screen.getByRole("button", {
      name: "Show inspector column",
    });
    expect(showInspector).toHaveTextContent("");
    expect(showInspector.querySelector("svg")).not.toBeNull();

    await user.click(showInspector);
    expect(screen.getByLabelText("Activity log")).toBeVisible();
    expect(screen.getByRole("main")).not.toHaveClass("inspector-hidden");
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(pane("1")).toHaveTextContent('{"offset":42}');
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");
    expect(screen.getByRole("button", { name: "Unpin orders.created" }))
      .toHaveAttribute("aria-pressed", "true");
    expect(topicSelect("orders.created").closest(".topic")).toHaveClass("active");
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(pane("1")).toHaveTextContent('{"offset":42}');
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "orders.created p0 / 42",
    );
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(tauri.startKafkaConsumerSession).toHaveBeenCalledOnce();
  });

  it("loads topics only after manual refresh, persists pins, selects topics, and recovers through another refresh", async () => {
    tauri.listKafkaTopics
      .mockRejectedValueOnce(new Error("SASL auth failed"))
      .mockResolvedValueOnce({ topics });

    const { user } = renderApp();

    expect(
      within(screen.getByLabelText("Milena workspace"))
        .queryByRole("heading", { level: 1 }),
    ).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect((await screen.findAllByText("SASL auth failed"))[0]).toBeVisible();
    expect(tauri.listKafkaTopics).toHaveBeenCalledWith({
      auth: expect.objectContaining({
        brokers: ["localhost:19092"],
        properties: expect.objectContaining({
          "sasl.username": "milena_plain",
          "sasl.password": "milena-plain-secret",
          "ssl.ca.location": "docker/kafka/generated/ssl/ca.crt",
        }),
      }),
    });
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

    await user.click(topicSelect("orders.created"));
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Publish" })).not.toBeInTheDocument();
    expect(
      within(screen.getByLabelText("Milena workspace"))
        .queryByRole("heading", { level: 1 }),
    ).not.toBeInTheDocument();
    expect(topicSelect("orders.created").closest(".topic")).toHaveClass("active");
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
    expect(tauri.loadAppState).toHaveBeenCalledOnce();
  });

  it("shows selected topics as right-rail previews without backend capability tags", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await user.click(topicSelect("orders.created"));

    const rightRail = screen.getByLabelText("Activity log");
    expect(within(rightRail).getByText("Topic preview")).toBeVisible();
    expect(within(rightRail).getAllByText("orders.created").length)
      .toBeGreaterThan(0);
    expect(within(rightRail).getByText("12 partitions")).toBeVisible();
    expect(within(rightRail).getByText("json")).toBeVisible();
    expect(within(rightRail).getByText("Open actions")).toBeVisible();
    expect(within(rightRail).getByText("Poll")).toBeVisible();
    expect(within(rightRail).getByText("Publish")).toBeVisible();
    expect(rightRail).not.toHaveTextContent("command-boundary");
    expect(rightRail).not.toHaveTextContent("event-channel");
    expect(rightRail).not.toHaveTextContent("macos-dev-build");
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
  });

  it("keeps long topic rows readable while exposing selected and pinned row state", async () => {
    const { user } = renderApp();
    await loadedTopics();

    const longTopic = "milena.issue14.cleanup";
    const row = topicRow(longTopic);
    expect(row).toHaveAttribute("data-selected", "false");
    expect(row).toHaveAttribute("data-pinned", "false");
    expect(topicSelect(longTopic)).toHaveTextContent(longTopic);
    expect(topicSelect(longTopic)).toHaveTextContent("4 partitions / json");

    const pin = screen.getByRole("button", { name: `Pin ${longTopic}` });
    expect(pin).toHaveAttribute("title", `Pin ${longTopic}`);
    expect(pin).toHaveAttribute("aria-pressed", "false");
    expect(pin).toHaveTextContent("");

    await user.click(topicSelect(longTopic));
    expect(topicRow(longTopic)).toHaveAttribute("data-selected", "true");

    await user.click(pin);
    const unpin = screen.getByRole("button", { name: `Unpin ${longTopic}` });
    expect(unpin).toHaveAttribute("title", `Unpin ${longTopic}`);
    expect(unpin).toHaveAttribute("aria-pressed", "true");
    expect(unpin).toHaveTextContent("");
    expect(topicRow(longTopic)).toHaveAttribute("data-pinned", "true");
    expect(topicRow(longTopic)).toHaveClass("active", "pinned");
    expect(window.localStorage.getItem(topicPinStorageKey)).toContain(longTopic);

    await user.click(unpin);
    expect(screen.getByRole("button", { name: `Pin ${longTopic}` }))
      .toHaveAttribute("aria-pressed", "false");
    expect(topicRow(longTopic)).toHaveAttribute("data-pinned", "false");
  });

  it("shows compact selected pane state while keeping activity clearable", async () => {
    const user = userEvent.setup();
    const onClearActivity = vi.fn();
    render(
      <RightRail
        activePane={{
          ...paneState("ready"),
          id: 2,
          topic: "payments.authorized",
          consumerGroup: "group-2",
        }}
        activity={[
          {
            id: "activity-1",
            source: "consumer",
            severity: "error",
            message: "Consumer session failed",
            detail: "broker heartbeat failed",
            paneId: 2,
            time: "12:00:00",
          },
        ]}
        layout="two-right"
        paneCount={2}
        topicPreview={null}
        onClearActivity={onClearActivity}
      />,
    );

    const rightRail = screen.getByLabelText("Activity log");
    expect(within(rightRail).getByText("Selected pane")).toBeVisible();
    expect(within(rightRail).getAllByText("Pane 2").length).toBeGreaterThan(0);
    expect(within(rightRail).getByText("ready")).toBeVisible();
    expect(within(rightRail).getByText("payments.authorized")).toBeVisible();
    expect(within(rightRail).getByText("group-2")).toBeVisible();
    expect(within(rightRail).getByText("2 panes / right")).toBeVisible();
    expect(within(rightRail).getByText("Consumer session failed")).toBeVisible();
    expect(within(rightRail).getByText("broker heartbeat failed")).toBeVisible();
    expect(rightRail).not.toHaveTextContent("Pane state");
    expect(rightRail).not.toHaveTextContent("Topic preview");
    expect(rightRail).not.toHaveTextContent("command-boundary");

    await user.click(within(rightRail).getByRole("button", { name: "Clear" }));
    expect(onClearActivity).toHaveBeenCalledOnce();
  });

  it("shows inactive selected pane details without making activity look active", () => {
    render(
      <RightRail
        activePane={{
          ...paneState("idle"),
          topic: null,
          consumerGroup: null,
        }}
        activity={[]}
        layout="single"
        paneCount={1}
        topicPreview={null}
        onClearActivity={vi.fn()}
      />,
    );

    const rightRail = screen.getByLabelText("Activity log");
    expect(within(rightRail).getByText("Inspector")).toBeVisible();
    expect(within(rightRail).getByText("Selected pane")).toBeVisible();
    expect(within(rightRail).getAllByText("Pane 1").length).toBeGreaterThan(0);
    expect(within(rightRail).getByText("idle")).toBeVisible();
    expect(within(rightRail).getAllByText("none")).toHaveLength(2);
    expect(within(rightRail).getByText("1 pane")).toBeVisible();
    expect(within(rightRail).getByText("Activity & errors")).toBeVisible();
    expect(within(rightRail).getByText("Inactive")).toBeVisible();
    expect(rightRail).not.toHaveTextContent("Pane state");
    expect(rightRail).not.toHaveTextContent("Topic preview");
  });

  it("opens topics only through explicit actions and enforces the four-pane split limit", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await user.click(topicSelect("orders.created"));
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();

    await openTopic(user, "orders.created");
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();

    await openTopic(user, "payments.authorized", "Split right");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Pane 2");
    expect(pane("2")).toHaveTextContent("payments.authorized");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "payments.authorized",
    );
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "2 panes / right",
    );

    await user.click(pane("1"));
    await openTopic(user, "inventory.adjusted", "Split top");

    expect(screen.getAllByLabelText(/Pane \d/)).toHaveLength(4);
    expect(pane("3")).toHaveTextContent("inventory.adjusted");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("2 x 2");
    for (const button of screen.getAllByRole("button", { name: /Split pane/ })) {
      expect(button).toBeDisabled();
    }

    await openTopic(user, "payments.authorized", "Split right");
    expect(screen.getAllByLabelText(/Pane \d/)).toHaveLength(4);
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "Pane limit reached",
    );
  });

  it("starts latest-only polling, renders delivered events locally, records global activity, and stops cleanly", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");

    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));

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

    await user.click(
      within(pane("1")).getByRole("button", {
        name: "Clear messages for pane 1",
      }),
    );
    expect(pane("1")).not.toHaveTextContent('{"offset":42}');
    expect(pane("1")).toHaveTextContent("session-1");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");
    expect(tauri.stopKafkaConsumerSession).not.toHaveBeenCalled();

    emit(
      "session-1",
      kafkaRecord("session-1", {
        offset: 43,
        payload: "{\"offset\":43}",
      }),
    );
    expect(await screen.findByText('{"offset":43}')).toBeVisible();

    await user.click(within(pane("1")).getByRole("button", { name: "Stop" }));

    await waitFor(() =>
      expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
        sessionId: "session-1",
      }),
    );
    expect(pane("1")).toHaveTextContent("idle");
    expect(pane("1")).toHaveTextContent("Inactive");
  });

  it("filters consumer records by key or payload and highlights visible matches", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");

    emit("session-1", kafkaRecord("session-1", {
      key: "Order-Alpha-42",
      offset: 42,
      payload: "{\"status\":\"paid\"}",
    }));
    emit("session-1", kafkaRecord("session-1", {
      key: "Invoice-Beta-9",
      offset: 43,
      payload: "{\"status\":\"queued\"}",
    }));
    emit("session-1", kafkaRecord("session-1", {
      key: null,
      offset: 44,
      payload: "{\"event\":\"PaymentAuthorized\"}",
    }));

    const paneOne = pane("1");
    const filter = within(paneOne).getByLabelText("Filter records for pane 1");

    await user.type(filter, "alpha");
    expect(messageRowButton("Order-Alpha-42")).toBeVisible();
    expect(paneOne).not.toHaveTextContent("Invoice-Beta-9");
    expect(paneOne).not.toHaveTextContent("PaymentAuthorized");
    expect(within(messageRowButton("Order-Alpha-42")).getByText("Alpha").tagName)
      .toBe("MARK");

    await user.clear(filter);
    await user.type(filter, "paymentauthorized");
    expect(paneOne).not.toHaveTextContent("Order-Alpha-42");
    expect(screen.getByText("PaymentAuthorized")).toBeVisible();
    expect(screen.getByText("PaymentAuthorized").tagName).toBe("MARK");
  });

  it("uses one stateful pane session button while starting, polling, and stopped", async () => {
    const user = userEvent.setup();
    const onPoll = vi.fn();
    const onStop = vi.fn();
    const { rerender } = render(
      <Pane {...paneRenderProps({ pane: paneState("idle"), onPoll, onStop })} />,
    );

    let sessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(sessionButtons).toHaveLength(1);
    expect(sessionButtons[0]).toHaveAccessibleName("Poll");
    expect(sessionButtons[0]).toHaveTextContent("Poll");
    expect(sessionButtons[0].querySelector("svg")).not.toBeNull();
    expect(sessionButtons[0].closest(".pane-session-actions")).not.toBeNull();
    expect(sessionButtons[0].closest(".pane-topic-row")).not.toBeNull();

    await user.click(sessionButtons[0]);
    expect(onPoll).toHaveBeenCalledOnce();
    expect(onStop).not.toHaveBeenCalled();

    rerender(
      <Pane {...paneRenderProps({ pane: paneState("loading"), onPoll, onStop })} />,
    );
    sessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(sessionButtons).toHaveLength(1);
    expect(sessionButtons[0]).toHaveAccessibleName("Stop");
    expect(sessionButtons[0]).toHaveTextContent("Stop");
    expect(sessionButtons[0].querySelector("svg")).not.toBeNull();
    expect(within(pane("1")).getByText("Starting")).toBeVisible();
    await user.click(sessionButtons[0]);
    expect(onStop).toHaveBeenCalledOnce();

    rerender(
      <Pane {...paneRenderProps({ pane: paneState("ready"), onPoll, onStop })} />,
    );
    expect(within(pane("1")).getByText("Polling")).toBeVisible();
    sessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(sessionButtons).toHaveLength(1);
    expect(sessionButtons[0]).toHaveAccessibleName("Stop");
    expect(sessionButtons[0].querySelector("svg")).not.toBeNull();

    rerender(
      <Pane {...paneRenderProps({ pane: paneState("idle"), onPoll, onStop })} />,
    );
    sessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(sessionButtons).toHaveLength(1);
    expect(sessionButtons[0]).toHaveAccessibleName("Poll");
    expect(sessionButtons[0].querySelector("svg")).not.toBeNull();

    rerender(
      <Pane {...paneRenderProps({ pane: paneState("error"), onPoll, onStop })} />,
    );
    sessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(sessionButtons).toHaveLength(1);
    expect(sessionButtons[0]).toHaveAccessibleName("Poll");
  });

  it("keeps icon close controls visible for selected and unselected panes", async () => {
    const user = userEvent.setup();
    const onCloseSelected = vi.fn();
    const onCloseUnselected = vi.fn();

    render(
      <>
        <Pane
          {...paneRenderProps({
            pane: paneState("idle"),
            active: true,
            onClose: onCloseSelected,
          })}
        />
        <Pane
          {...paneRenderProps({
            pane: {
              ...paneState("idle"),
              id: 2,
              topic: "payments.authorized",
              consumerGroup: "milena-preview-2",
            },
            active: false,
            onClose: onCloseUnselected,
          })}
        />
      </>,
    );

    const selectedClose = within(pane("1")).getByRole("button", {
      name: "Close pane 1",
    });
    const selectedActions = within(
      pane("1").querySelector(".pane-actions") as HTMLElement,
    ).getAllByRole("button");
    const unselectedClose = within(pane("2")).getByRole("button", {
      name: "Close pane 2",
    });

    expect(selectedClose).toBeVisible();
    expect(selectedActions.map((button) => button.getAttribute("aria-label")))
      .toEqual([
        "Maximize pane 1",
        "Close pane 1",
        "Split pane 1 right",
        "Split pane 1 top",
      ]);
    expect(
      within(pane("1")).getByRole("button", { name: "Split pane 1 right" }),
    ).toHaveTextContent("");
    expect(
      within(pane("1")).getByRole("button", { name: "Split pane 1 top" }),
    ).toHaveTextContent("");
    expect(unselectedClose).toBeVisible();
    expect(unselectedClose).toHaveClass("pane-close-button");
    expect(unselectedClose.querySelector("svg")).not.toBeNull();
    expect(unselectedClose).not.toHaveTextContent("x");

    await user.click(unselectedClose);
    expect(onCloseUnselected).toHaveBeenCalledOnce();
    expect(onCloseSelected).not.toHaveBeenCalled();
  });

  it("expands a pane into the workspace and restores the prior pane layout", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await openTopic(user, "orders.created");
    await openTopic(user, "payments.authorized", "Split right");

    expect(screen.getAllByLabelText(/Pane \d/)).toHaveLength(2);
    await user.click(
      within(pane("2")).getByRole("button", {
        name: "Expand publisher for pane 2",
      }),
    );
    const paneTwoPayload = within(pane("2")).getByLabelText(
      "JSON payload for pane 2",
    );
    fireEvent.change(paneTwoPayload, { target: { value: "{\"draft\":2}" } });
    expect(paneTwoPayload).toHaveValue("{\"draft\":2}");

    await user.click(
      within(pane("1")).getByRole("button", { name: "Maximize pane 1" }),
    );

    expect(pane("1")).toHaveTextContent("orders.created");
    expect(screen.queryByLabelText("Pane 2")).not.toBeInTheDocument();
    expect(
      within(pane("1")).getByRole("button", { name: "Restore pane 1" }),
    ).toBeVisible();

    await user.click(
      within(pane("1")).getByRole("button", { name: "Restore pane 1" }),
    );
    expect(screen.getAllByLabelText(/Pane \d/)).toHaveLength(2);
    expect(pane("1")).toHaveTextContent("orders.created");
    expect(pane("2")).toHaveTextContent("payments.authorized");
    await user.click(
      within(pane("2")).getByRole("button", {
        name: "Expand publisher for pane 2",
      }),
    );
    expect(
      within(pane("2")).getByLabelText("JSON payload for pane 2"),
    ).toHaveValue("{\"draft\":2}");
  });

  it("isolates multi-pane consumer events and errors", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await openTopic(user, "payments.authorized", "Split right");

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
    const paneOneSessionButtons = within(pane("1")).getAllByRole("button", {
      name: /^(Poll|Stop)$/,
    });
    expect(paneOneSessionButtons).toHaveLength(1);
    expect(paneOneSessionButtons[0]).toHaveAccessibleName("Poll");
  });

  it("closes an active polling pane after best-effort cleanup", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await openTopic(user, "payments.authorized", "Split right");
    await user.click(pane("1"));
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");
    tauri.stopKafkaConsumerSession.mockRejectedValueOnce(
      new Error("client cleanup failed"),
    );

    await user.click(
      within(pane("1")).getByRole("button", { name: "Close pane 1" }),
    );

    await waitFor(() =>
      expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument(),
    );
    expect(pane("2")).toHaveTextContent("payments.authorized");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("1 pane");
    expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    await waitFor(() =>
      expect(screen.getByLabelText("Activity log")).toHaveTextContent(
        "client cleanup failed",
      ),
    );
  });

  it("closes a loading pane and cleans up the stale session when start resolves", async () => {
    const { user } = renderApp();
    let resolveStart: () => void = () => {
      throw new Error("start resolver was not captured");
    };
    tauri.startKafkaConsumerSession.mockImplementationOnce(
      async (request, onEvent) =>
        new Promise<KafkaConsumerSession>((resolve) => {
          resolveStart = () => {
            const started = consumerSession(
              "session-loading",
              "group-loading",
              request.topics[0],
            );
            registerSessionEventHandler(started.sessionId, onEvent);
            resolve(started);
          };
        }),
    );

    await loadedTopics();
    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await waitFor(() =>
      expect(within(pane("1")).getAllByText("loading").length).toBeGreaterThan(
        0,
      ),
    );

    await user.click(
      within(pane("1")).getByRole("button", { name: "Close pane 1" }),
    );

    await waitFor(() =>
      expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument(),
    );
    expect(tauri.stopKafkaConsumerSession).not.toHaveBeenCalled();

    await act(async () => {
      resolveStart();
    });

    await waitFor(() =>
      expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
        sessionId: "session-loading",
      }),
    );
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
  });

  it("keeps publisher collapsed by default and preserves pane drafts across collapse", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");

    const paneOne = pane("1");
    const consumer = within(paneOne).getByLabelText("Consumer for pane 1");
    const publisher = within(paneOne).getByLabelText("Publisher for pane 1");
    expect(
      consumer.compareDocumentPosition(publisher) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    // @ts-ignore Vitest runs this assertion in Node; the renderer tsconfig has no Node types.
    const { readFileSync } = await import("node:fs");
    const appCss = readFileSync("src/App.css", "utf8");
    expect(paneOne.querySelector(".pane-workbench")).toHaveClass(
      "has-collapsed-publisher",
    );
    expect(appCss).toMatch(/\.pane\s*{[^}]*grid-template-rows:\s*auto auto 1fr;/s);
    expect(appCss).toMatch(/\.pane\.has-error\s*{[^}]*grid-template-rows:\s*auto auto auto 1fr;/s);
    expect(appCss).toMatch(/\.pane-workbench\s*{[^}]*position:\s*relative;[^}]*--publisher-reserved-height:\s*58px;[^}]*height:\s*100%;/s);
    expect(appCss).toMatch(/\.pane-workbench\.has-expanded-publisher\s*{[^}]*--publisher-reserved-height:\s*min\(430px,\s*62vh,\s*calc\(100% - 112px\)\);/s);
    expect(appCss).toMatch(/\.consumer\s*{[^}]*height:\s*100%;[^}]*padding-bottom:\s*var\(--publisher-reserved-height\);/s);
    expect(appCss).toMatch(/\.publisher\s*{[^}]*position:\s*absolute;[^}]*right:\s*0;[^}]*bottom:\s*0;[^}]*left:\s*0;[^}]*max-height:\s*var\(--publisher-reserved-height\);[^}]*overflow:\s*hidden;/s);
    expect(within(paneOne).getByRole("button", { name: "Poll" })).toBeVisible();
    expect(
      within(paneOne).getByLabelText("Render mode for orders.created"),
    ).toBeVisible();
    expect(screen.queryByLabelText("Kafka key for pane 1")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("JSON payload for pane 1")).not.toBeInTheDocument();

    await user.click(
      within(publisher).getByRole("button", {
        name: "Expand publisher for pane 1",
      }),
    );
    expect(paneOne.querySelector(".pane-workbench")).toHaveClass(
      "has-expanded-publisher",
    );
    const key = screen.getByLabelText("Kafka key for pane 1");
    const payload = screen.getByLabelText("JSON payload for pane 1");
    fireEvent.change(key, { target: { value: "order-1" } });
    fireEvent.change(payload, { target: { value: "{\"id\":1}" } });

    await user.click(
      within(publisher).getByRole("button", {
        name: "Collapse publisher for pane 1",
      }),
    );
    expect(screen.queryByLabelText("Kafka key for pane 1")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("JSON payload for pane 1")).not.toBeInTheDocument();

    await user.click(
      within(publisher).getByRole("button", {
        name: "Expand publisher for pane 1",
      }),
    );
    expect(screen.getByLabelText("Kafka key for pane 1")).toHaveValue("order-1");
    expect(screen.getByLabelText("JSON payload for pane 1")).toHaveValue(
      "{\"id\":1}",
    );
  });

  it("runs the publish flow, gates sends, trims optional keys, formats JSON, and displays producer failures", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await user.click(
      within(screen.getByLabelText("Publisher for pane 1")).getByRole("button", {
        name: "Expand publisher for pane 1",
      }),
    );

    const send = within(pane("1")).getByRole("button", { name: "Send" });
    expect(send).toBeDisabled();

    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await waitFor(() =>
      expect(screen.getAllByText("Ready to publish").length).toBeGreaterThan(0),
    );

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
    await openTopic(user, "orders.created");
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
    const consumer = within(pane("1")).getByLabelText("Consumer for pane 1");
    expect(within(consumer).queryByText("orders.created")).not.toBeInTheDocument();
    expect(messageRowButton("{\"id\":")).toHaveClass("has-key");

    await user.selectOptions(
      screen.getByLabelText("Render mode for orders.created"),
      "raw",
    );
    expect(window.localStorage.getItem(messagePreferencesStorageKey)).toContain(
      "raw",
    );
    expect(screen.queryByText("invalid JSON")).not.toBeInTheDocument();

    await user.click(messageRowButton("{\"id\":"));
    expect(messageRowButton("{\"id\":")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByLabelText("Kafka headers")).toHaveTextContent("trace-id");
    expect(screen.getByLabelText("Kafka headers")).toHaveTextContent("abc");
    expect(screen.getByText("key order-42")).toBeVisible();
  });

  it("uses the wide payload preview column for records without keys", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");

    emit("session-1", kafkaRecord("session-1", {
      offset: 39,
      payload: JSON.stringify({
        topic: "orders.created",
        event: "preview",
        shape: "wide summary row",
      }),
    }));

    const summary = messageRowButton("orders.created");
    expect(summary).toHaveClass("no-key");
    expect(summary).not.toHaveClass("has-key");
    expect(within(summary).queryByText(/^key /)).not.toBeInTheDocument();
    expect(summary).toHaveTextContent("wide summary row");

    // @ts-ignore Vitest runs this assertion in Node; the renderer tsconfig has no Node types.
    const { readFileSync } = await import("node:fs");
    const appCss = readFileSync("src/App.css", "utf8");
    expect(appCss).toMatch(/\.message-row-summary\.no-key\s*{[^}]*grid-template-columns:\s*18px minmax\(66px,\s*auto\) minmax\(58px,\s*auto\) minmax\(0,\s*1fr\);/s);
    expect(appCss).toMatch(/\.message-row-summary\.has-key\s*{[^}]*grid-template-columns:\s*18px minmax\(66px,\s*auto\) minmax\(58px,\s*auto\) minmax\(70px,\s*0\.35fr\) minmax\(0,\s*1fr\);/s);
    expect(appCss).toMatch(/\.message-preview code\s*{[^}]*min-width:\s*0;[^}]*flex:\s*1 1 auto;/s);
  });

  it("does not label expanded records as truncated when only the summary preview is shortened", async () => {
    const { user } = renderApp();
    await loadedTopics();
    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");

    emit("session-1", kafkaRecord("session-1", {
      offset: 40,
      payload: JSON.stringify({
        mgId: "GAMEPLAN",
        instanceId: 30013,
        strategyName: "manual_mid_yes_no",
        requestId: 1781305950813,
        status: "config_applied",
        config:
          "{\"mojoId\":\"KXBTCD-26JUN1517-T66499.99\",\"qty_yes\":10,\"qty_no\":15,\"mid\":25,\"vig\":1,\"widen_to_market\":false,\"max_vig\":1,\"limits\":{\"max_loss\":1000}}",
      }),
    }));

    const row = messageRowButton("GAMEPLAN");
    expect(within(row).getByText("truncated")).toBeVisible();

    await user.click(row);

    expect(screen.getByText(/qty_yes/)).toBeVisible();
    expect(screen.queryByText("payload truncated")).not.toBeInTheDocument();
  });

  it("opens and closes the topic open menu and routes actions to panes", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await user.click(topicOpenActions("orders.created"));
    const menu = await screen.findByRole("menu");
    expect(menu).toHaveTextContent("orders.created");
    expect(within(menu).getByRole("menuitem", { name: "Open" })).toBeVisible();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();

    await openTopic(user, "orders.created");
    await user.click(topicOpenActions("payments.authorized"));
    const selectedMenu = await screen.findByRole("menu");
    expect(within(selectedMenu).getByRole("menuitem", { name: "Open selected" }))
      .toBeVisible();
    expect(within(selectedMenu).getByRole("menuitem", { name: "Split right" }))
      .toBeVisible();
    expect(within(selectedMenu).getByRole("menuitem", { name: "Split top" }))
      .toBeVisible();
    expect(within(selectedMenu).getByRole("menuitem", { name: "Split bottom" }))
      .toBeVisible();
    await user.click(
      within(selectedMenu).getByRole("menuitem", { name: "Open selected" }),
    );

    expect(pane("1")).toHaveTextContent("payments.authorized");
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
  });

  it("stops stale polling when a topic replaces the selected pane", async () => {
    const { user } = renderApp();
    await loadedTopics();

    await openTopic(user, "orders.created");
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");
    expect(tauri.startKafkaConsumerSession).toHaveBeenCalledOnce();

    await openTopic(user, "payments.authorized", "Open selected");

    expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
      sessionId: "session-1",
    });
    expect(tauri.startKafkaConsumerSession).toHaveBeenCalledOnce();
    expect(pane("1")).toHaveTextContent("payments.authorized");
    expect(pane("1")).not.toHaveTextContent("session-1");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("idle");
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "milena-preview-1",
    );
    expect(screen.getByLabelText("Activity log")).not.toHaveTextContent(
      "group-1",
    );
  });

  it("keeps the shell available when the runtime is unavailable and avoids unhandled promise rejections", async () => {
    const unhandled = vi.fn();
    window.addEventListener("unhandledrejection", unhandled);
    tauri.loadAppState.mockRejectedValueOnce(new Error("invoke missing"));
    tauri.listKafkaTopics.mockRejectedValueOnce(new Error("IPC unavailable"));

    const { user } = renderApp();

    expect(
      within(screen.getByLabelText("Milena workspace"))
        .queryByRole("heading", { level: 1 }),
    ).not.toBeInTheDocument();
    expect(await screen.findByText("Tauri runtime unavailable")).toBeVisible();
    expect(screen.getByText("invoke missing")).toBeVisible();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Refresh" }));

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

function paneRenderProps({
  pane,
  active = true,
  onPoll = vi.fn(),
  onStop = vi.fn(),
  onClose = vi.fn(),
  onExpand = vi.fn(),
  onRestore = vi.fn(),
  isExpanded = false,
}: {
  pane: WorkspacePane;
  active?: boolean;
  onPoll?: () => void;
  onStop?: () => void;
  onClose?: () => void;
  onExpand?: () => void;
  onRestore?: () => void;
  isExpanded?: boolean;
}) {
  return {
    pane,
    active,
    canSplit: true,
    isExpanded,
    onActivate: vi.fn(),
    onPoll,
    publisher: {
      topic: pane.topic,
      key: "",
      payload: "",
      status: "idle" as const,
      error: null,
      ack: null,
    },
    onPayloadChange: vi.fn(),
    onKeyChange: vi.fn(),
    onFormatPayload: vi.fn(),
    onSend: vi.fn(),
    environmentKey: "local-dev",
    renderPreferences: {},
    onRenderModeChange: vi.fn(),
    onSplit: vi.fn(),
    onExpand,
    onRestore,
    onStop,
    onClearMessages: vi.fn(),
    onClose,
  };
}

function paneState(status: WorkspacePane["status"]): WorkspacePane {
  const polling = status === "loading" || status === "ready" || status === "error";
  return {
    id: 1,
    topic: "orders.created",
    consumerGroup: polling ? "group-stateful" : "milena-preview-1",
    mode: polling ? "poll" : "idle",
    status,
    session: polling
      ? consumerSession("session-stateful", "group-stateful", "orders.created")
      : null,
    activity: [],
    error: status === "error" ? "broker heartbeat failed" : null,
    tone: status === "error" ? "error" : "normal",
  };
}
