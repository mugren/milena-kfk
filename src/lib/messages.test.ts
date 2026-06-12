import { describe, expect, it } from "vitest";
import {
  MESSAGE_RENDER_PREFERENCES_KEY,
  readMessageRenderPreferences,
  renderKafkaRecord,
  setTopicMessageRenderMode,
  stampKafkaRecordReceivedAt,
  topicMessageRenderMode,
  type MessageRenderPreferenceStore,
} from "./messages";
import type { KafkaRecordEvent, MilenaBoundaryEvent } from "./tauri";

describe("message rendering", () => {
  it("renders JSON payloads by default", () => {
    const rendered = renderKafkaRecord(
      record({ payload: "{\"id\":42,\"status\":\"paid\"}" }),
    );

    expect(rendered.payload.format).toBe("json");
    expect(rendered.payload.preview).toBe('{"id":42,"status":"paid"}');
    expect(rendered.payload.content).toContain('"status": "paid"');
    expect(rendered.payload.marker).toBeNull();
  });

  it("falls back to raw text and marks invalid JSON inline", () => {
    const rendered = renderKafkaRecord(record({ payload: "{\"id\":" }));

    expect(rendered.payload.format).toBe("raw");
    expect(rendered.payload.preview).toBe("{\"id\":");
    expect(rendered.payload.invalidJson).toBe(true);
    expect(rendered.payload.marker).toBe("invalid JSON");
  });

  it("renders raw mode without JSON parsing or invalid markers", () => {
    const rendered = renderKafkaRecord(
      record({ payload: "{\"id\":" }),
      { mode: "raw" },
    );

    expect(rendered.payload).toMatchObject({
      mode: "raw",
      format: "raw",
      preview: "{\"id\":",
      content: "{\"id\":",
      invalidJson: false,
      marker: null,
    });
  });

  it("uses persisted raw mode preference per environment and topic", () => {
    const store = memoryStore();
    const preferences = setTopicMessageRenderMode(
      {},
      "local",
      "orders.created",
      "raw",
      store,
    );
    const restored = readMessageRenderPreferences(store);

    expect(topicMessageRenderMode(preferences, "local", "orders.created")).toBe(
      "raw",
    );
    expect(topicMessageRenderMode(restored, "local", "orders.created")).toBe(
      "raw",
    );
    expect(topicMessageRenderMode(restored, "staging", "orders.created")).toBe(
      "json",
    );
    expect(store.getItem(MESSAGE_RENDER_PREFERENCES_KEY)).toContain(
      "orders.created",
    );
  });

  it("recovers from malformed render preference storage", () => {
    const malformed = readMessageRenderPreferences(
      memoryStore({ [MESSAGE_RENDER_PREFERENCES_KEY]: "not json" }),
    );

    expect(malformed).toEqual({});
  });

  it("prunes invalid render modes from stored preferences", () => {
    const restored = readMessageRenderPreferences(
      memoryStore({
        [MESSAGE_RENDER_PREFERENCES_KEY]: JSON.stringify({
          local: {
            "orders.created": "raw",
            "payments.authorized": "xml",
            "inventory.adjusted": "json",
          },
          staging: "raw",
        }),
      }),
    );

    expect(restored).toEqual({
      local: {
        "orders.created": "raw",
        "inventory.adjusted": "json",
      },
    });
  });

  it("renders expanded rows with full payload and headers", () => {
    const rendered = renderKafkaRecord(
      record({
        key: "invoice-1",
        payload: "{\"nested\":{\"ok\":true}}",
        headers: [
          { key: "trace-id", value: "abc" },
          { key: "tenant", value: null },
        ],
      }),
      { expanded: true },
    );

    expect(rendered.expanded).toBe(true);
    expect(rendered.key).toBe("invoice-1");
    expect(rendered.payload.content).toContain('"nested": {');
    expect(rendered.headers).toEqual([
      { key: "trace-id", value: "abc" },
      { key: "tenant", value: null },
    ]);
  });

  it("normalizes object headers from backend-shaped payloads", () => {
    const rendered = renderKafkaRecord(
      record({
        headers: {
          "trace-id": "abc",
          role: ["producer", "audit"],
          empty: null,
        },
      }),
    );

    expect(rendered.headers).toEqual([
      { key: "trace-id", value: "abc" },
      { key: "role", value: "producer" },
      { key: "role", value: "audit" },
      { key: "empty", value: null },
    ]);
  });

  it("caps large rendered payloads and marks them as truncated", () => {
    const rendered = renderKafkaRecord(
      record({ payload: "{\"value\":\"abcdefghijklmnop\"}" }),
      { maxPreviewChars: 12, maxExpandedChars: 18 },
    );

    expect(rendered.payload.preview).toBe("{\"value\":\"ab");
    expect(rendered.payload.content.length).toBeLessThanOrEqual(18);
    expect(rendered.payload.truncated).toBe(true);
  });

  it("stamps receive time on Kafka record events", () => {
    const event: MilenaBoundaryEvent = {
      event: "kafkaRecord",
      data: { record: record({}) },
    };
    const stamped = stampKafkaRecordReceivedAt(
      event,
      new Date("2026-06-12T12:34:56.000Z"),
    );

    expect(stamped.event).toBe("kafkaRecord");
    if (stamped.event === "kafkaRecord") {
      expect(stamped.data.record.receivedAt).toBe(
        "2026-06-12T12:34:56.000Z",
      );
    }
  });

  it("preserves existing receivedAt values on Kafka record events", () => {
    const event: MilenaBoundaryEvent = {
      event: "kafkaRecord",
      data: {
        record: record({
          receivedAt: "2026-06-12T10:00:00.000Z",
        }),
      },
    };
    const stamped = stampKafkaRecordReceivedAt(
      event,
      new Date("2026-06-12T12:34:56.000Z"),
    );

    expect(stamped).toBe(event);
    if (stamped.event === "kafkaRecord") {
      expect(stamped.data.record.receivedAt).toBe(
        "2026-06-12T10:00:00.000Z",
      );
    }
  });
});

function record(
  overrides: Partial<KafkaRecordEvent>,
): KafkaRecordEvent {
  return {
    sessionId: "session-1",
    topic: "orders.created",
    partition: 2,
    offset: 42,
    key: null,
    payload: "{\"ok\":true}",
    receivedAt: "2026-06-12T12:34:56.000Z",
    ...overrides,
  };
}

function memoryStore(initial?: Record<string, string>): MessageRenderPreferenceStore {
  const values = new Map(Object.entries(initial ?? {}));

  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => {
      values.set(key, value);
    },
  };
}
