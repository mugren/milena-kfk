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
