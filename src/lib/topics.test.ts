import { describe, expect, it, vi } from "vitest";
import {
  createInitialTopicRailState,
  filterAndOrderTopics,
  loadTopicsForEnvironment,
  pinsForEnvironment,
  setTopicSearch,
  toggleTopicPin,
  visibleTopicRows,
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

const stagingAuth: RuntimeAuthConfig = {
  environment: "staging",
  brokers: ["staging:9092"],
  properties: {},
};

const pinStorageKey = "milena.topicPins.v1";

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

  it("records topic load failures on the opened environment", async () => {
    const fetchTopics = vi
      .fn<TopicFetch>()
      .mockRejectedValue(new Error("auth rejected"));

    const failed = await loadTopicsForEnvironment(
      createInitialTopicRailState(),
      localAuth,
      fetchTopics,
    );

    expect(failed).toMatchObject({
      environmentKey: "local",
      status: "error",
      error: "auth rejected",
      loadedEnvironmentKeys: [],
    });
    expect(failed.topics).toEqual([]);
  });

  it("scopes search and visible pins to the active environment", async () => {
    const store = memoryStore();
    const fetchTopics = vi.fn<TopicFetch>().mockResolvedValue(topics);
    const pinned = toggleTopicPin(
      toggleTopicPin(
        createInitialTopicRailState(store),
        "local",
        "orders.created",
        store,
      ),
      "staging",
      "payments.authorized",
      store,
    );

    const localLoaded = setTopicSearch(
      await loadTopicsForEnvironment(pinned, localAuth, fetchTopics),
      "orders",
    );
    const stagingLoaded = await loadTopicsForEnvironment(
      localLoaded,
      stagingAuth,
      fetchTopics,
    );

    expect(visibleTopicRows(localLoaded).map((topic) => topic.name)).toEqual([
      "orders.created",
    ]);
    expect(pinsForEnvironment(localLoaded)).toEqual(["orders.created"]);
    expect(stagingLoaded.searchQuery).toBe("");
    expect(pinsForEnvironment(stagingLoaded)).toEqual([
      "payments.authorized",
    ]);
    expect(visibleTopicRows(stagingLoaded).map((topic) => topic.name)).toEqual([
      "payments.authorized",
      "inventory.adjusted",
      "orders.created",
    ]);
  });

  it("persists unpins back to storage", () => {
    const store = memoryStore();
    const pinned = toggleTopicPin(
      createInitialTopicRailState(store),
      "local",
      "orders.created",
      store,
    );
    const unpinned = toggleTopicPin(pinned, "local", "orders.created", store);
    const restored = createInitialTopicRailState(store);

    expect(unpinned.pinnedTopicsByEnvironment.local).toEqual([]);
    expect(restored.pinnedTopicsByEnvironment.local).toEqual([]);
    expect(store.getItem(pinStorageKey)).toBe("{\"local\":[]}");
  });

  it("recovers from malformed pin storage", () => {
    const malformedJson = createInitialTopicRailState(
      memoryStore({ [pinStorageKey]: "not json" }),
    );
    const malformedShape = createInitialTopicRailState(
      memoryStore({
        [pinStorageKey]: JSON.stringify({
          local: ["orders.created", 12, "payments.authorized"],
          staging: "payments.authorized",
        }),
      }),
    );

    expect(malformedJson.pinnedTopicsByEnvironment).toEqual({});
    expect(malformedShape.pinnedTopicsByEnvironment).toEqual({
      local: ["orders.created", "payments.authorized"],
    });
  });
});
