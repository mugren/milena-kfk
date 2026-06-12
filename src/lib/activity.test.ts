import { describe, expect, it } from "vitest";
import {
  appendBoundaryEventActivity,
  appendGlobalActivity,
  appendGlobalError,
  clearGlobalActivity,
  type GlobalActivityEntry,
} from "./activity";
import type { MilenaBoundaryEvent } from "./tauri";

describe("global activity log", () => {
  it("records attributed pane events without mutating existing entries", () => {
    const existing = [entry("existing")];
    const next = appendBoundaryEventActivity(existing, record(7), 2, {
      now: () => new Date("2026-06-12T12:00:00.000Z"),
      nextId: () => "event-1",
    });

    expect(next).toHaveLength(2);
    expect(next[0]).toMatchObject({
      id: "event-1",
      severity: "info",
      source: "consumer",
      message: "kafkaRecord",
      detail: "orders.created p0 / 7",
      paneId: 2,
    });
    expect(existing).toHaveLength(1);
  });

  it("records clearable global errors", () => {
    const logged = appendGlobalError([], "topics", "Topic list refresh failed", {
      detail: "auth rejected",
      nextId: () => "error-1",
    });

    expect(logged).toMatchObject([
      {
        id: "error-1",
        severity: "error",
        source: "topics",
        message: "Topic list refresh failed",
        detail: "auth rejected",
        paneId: null,
      },
    ]);
    expect(clearGlobalActivity()).toEqual([]);
  });

  it("keeps the newest entries within the configured limit", () => {
    const entries = ["first", "second"].reduce(
      (current, message) =>
        appendGlobalActivity(
          current,
          {
            severity: "info",
            source: "app",
            message,
            detail: null,
            paneId: null,
          },
          { limit: 1, nextId: () => message },
        ),
      [] as GlobalActivityEntry[],
    );

    expect(entries.map((event) => event.message)).toEqual(["second"]);
  });

  it("maps boundary events to consumer activity severity and detail", () => {
    const cases: Array<{
      event: MilenaBoundaryEvent;
      message: string;
      severity: "info" | "error";
      detail: string;
    }> = [
      {
        event: {
          event: "boundaryOpened",
          data: {
            sessionId: "session-1",
            topic: "orders.created",
            mode: "poll",
          },
        },
        message: "boundaryOpened",
        severity: "info",
        detail: "orders.created",
      },
      {
        event: {
          event: "boundaryReady",
          data: {
            sessionId: "session-1",
            capabilities: ["command-boundary"],
          },
        },
        message: "boundaryReady",
        severity: "info",
        detail: "session-1",
      },
      {
        event: {
          event: "kafkaConsumerStarted",
          data: {
            sessionId: "session-1",
            groupId: "group-1",
            topics: ["orders.created"],
          },
        },
        message: "kafkaConsumerStarted",
        severity: "info",
        detail: "group-1",
      },
      {
        event: record(7),
        message: "kafkaRecord",
        severity: "info",
        detail: "orders.created p0 / 7",
      },
      {
        event: {
          event: "kafkaConsumerError",
          data: {
            sessionId: "session-1",
            message: "consumer failed",
          },
        },
        message: "kafkaConsumerError",
        severity: "error",
        detail: "consumer failed",
      },
      {
        event: {
          event: "kafkaConsumerStopped",
          data: {
            sessionId: "session-1",
            groupId: "group-1",
          },
        },
        message: "kafkaConsumerStopped",
        severity: "info",
        detail: "group-1",
      },
    ];

    const entries = cases.map((activityCase, index) =>
      appendBoundaryEventActivity([], activityCase.event, 3, {
        nextId: () => `event-${index}`,
      })[0],
    );

    expect(
      entries.map(({ message, severity, source, detail, paneId }) => ({
        message,
        severity,
        source,
        detail,
        paneId,
      })),
    ).toEqual(
      cases.map(({ message, severity, detail }) => ({
        message,
        severity,
        source: "consumer",
        detail,
        paneId: 3,
      })),
    );
  });
});

function entry(id: string): GlobalActivityEntry {
  return {
    id,
    severity: "info",
    source: "app",
    message: id,
    detail: null,
    paneId: null,
    time: "12:00:00 PM",
  };
}

function record(offset: number): MilenaBoundaryEvent {
  return {
    event: "kafkaRecord",
    data: {
      record: {
        sessionId: "session-1",
        topic: "orders.created",
        partition: 0,
        offset,
        key: null,
        payload: `{"offset":${offset}}`,
      },
    },
  };
}
