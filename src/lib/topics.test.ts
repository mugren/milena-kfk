import { describe, expect, it, vi } from "vitest";
import {
  createInitialTopicRailState,
  filterAndOrderTopics,
  loadTopicsForEnvironment,
  toggleTopicPin,
  type TopicFetch,
  type TopicPinStore,
} from "./topics";
import type { KafkaTopicMetadata, RuntimeAuthConfig } from "./tauri";

const topics: KafkaTopicMetadata[] = [
  { name: "orders.created", partitionCount: 12 },
  { name: "payments.authorized", partitionCount: 8 },
  { name: "inventory.adjusted", partitionCount: 6 },
];

const localAuth: RuntimeAuthConfig = {
  environment: "local",
  brokers: ["localhost:9092"],
  properties: {},
};

function memoryStore(initial?: Record<string, string>): TopicPinStore {
  const values = new Map(Object.entries(initial ?? {}));

  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => {
      values.set(key, value);
    },
  };
}

describe("topic rail state", () => {
  it("filters topics client-side and keeps pinned matches at the top", () => {
    const rows = filterAndOrderTopics(topics, "ted", [
      "inventory.adjusted",
    ]);

    expect(rows.map((topic) => topic.name)).toEqual([
      "inventory.adjusted",
      "orders.created",
    ]);
    expect(rows.map((topic) => topic.pinned)).toEqual([true, false]);
  });

  it("persists pinned topics per environment", () => {
    const store = memoryStore();
    const local = toggleTopicPin(
      createInitialTopicRailState(store),
      "local",
      "orders.created",
      store,
    );
    const staging = toggleTopicPin(
      local,
      "staging",
      "payments.authorized",
      store,
    );
    const restored = createInitialTopicRailState(store);

    expect(staging.pinnedTopicsByEnvironment).toEqual({
      local: ["orders.created"],
      staging: ["payments.authorized"],
    });
    expect(restored.pinnedTopicsByEnvironment.local).toEqual([
      "orders.created",
    ]);
    expect(restored.pinnedTopicsByEnvironment.staging).toEqual([
      "payments.authorized",
    ]);
  });

  it("loads topics once when an environment is opened again", async () => {
    const fetchTopics = vi.fn<TopicFetch>().mockResolvedValue(topics);

    const loaded = await loadTopicsForEnvironment(
      createInitialTopicRailState(),
      localAuth,
      fetchTopics,
    );
    const reopened = await loadTopicsForEnvironment(
      loaded,
      localAuth,
      fetchTopics,
    );

    expect(fetchTopics).toHaveBeenCalledTimes(1);
    expect(reopened.topics.map((topic) => topic.name)).toEqual([
      "inventory.adjusted",
      "orders.created",
      "payments.authorized",
    ]);
    expect(reopened.loadedEnvironmentKeys).toEqual(["local"]);
  });

  it("manual refresh forces a new topic list without background polling", async () => {
    const refreshedTopics = [
      ...topics,
      { name: "shipments.dispatched", partitionCount: 4 },
    ];
    const fetchTopics = vi
      .fn<TopicFetch>()
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce(refreshedTopics);

    const loaded = await loadTopicsForEnvironment(
      createInitialTopicRailState(),
      localAuth,
      fetchTopics,
    );
    const refreshed = await loadTopicsForEnvironment(
      loaded,
      localAuth,
      fetchTopics,
      true,
    );

    expect(fetchTopics).toHaveBeenCalledTimes(2);
    expect(refreshed.topics.map((topic) => topic.name)).toContain(
      "shipments.dispatched",
    );
    expect(refreshed.loadedEnvironmentKeys).toEqual(["local"]);
  });
});
