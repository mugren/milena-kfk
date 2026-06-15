import type { KafkaTopicMetadata, RuntimeAuthConfig } from "./tauri";

export type TopicLoadStatus = "idle" | "loading" | "ready" | "error";

export type TopicPinStore = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
};

export type TopicRailState = {
  environmentKey: string | null;
  topics: KafkaTopicMetadata[];
  loadedEnvironmentKeys: string[];
  searchQuery: string;
  pinnedTopicsByEnvironment: Record<string, string[]>;
  status: TopicLoadStatus;
  error: string | null;
};

export type TopicListRow = KafkaTopicMetadata & {
  pinned: boolean;
};

export type TopicFetch = (
  auth: RuntimeAuthConfig,
) => Promise<KafkaTopicMetadata[]>;

const PIN_STORAGE_KEY = "milena.topicPins.v1";

export function createInitialTopicRailState(
  store?: TopicPinStore,
): TopicRailState {
  return {
    environmentKey: null,
    topics: [],
    loadedEnvironmentKeys: [],
    searchQuery: "",
    pinnedTopicsByEnvironment: readPinnedTopics(store),
    status: "idle",
    error: null,
  };
}

export function environmentKey(auth: Pick<RuntimeAuthConfig, "environment">) {
  return auth.environment.trim();
}

export function openTopicEnvironment(
  state: TopicRailState,
  nextEnvironmentKey: string,
): TopicRailState {
  return {
    ...state,
    environmentKey: nextEnvironmentKey,
    searchQuery:
      state.environmentKey === nextEnvironmentKey ? state.searchQuery : "",
    error: null,
  };
}

export function shouldLoadTopicsForEnvironment(
  state: TopicRailState,
  nextEnvironmentKey: string,
  force = false,
): boolean {
  return force || !state.loadedEnvironmentKeys.includes(nextEnvironmentKey);
}

export function markTopicLoadStarted(
  state: TopicRailState,
  nextEnvironmentKey: string,
): TopicRailState {
  return {
    ...openTopicEnvironment(state, nextEnvironmentKey),
    status: "loading",
    error: null,
  };
}

export function markTopicLoadSucceeded(
  state: TopicRailState,
  nextEnvironmentKey: string,
  topics: KafkaTopicMetadata[],
): TopicRailState {
  return {
    ...state,
    environmentKey: nextEnvironmentKey,
    topics: sortTopics(topics),
    loadedEnvironmentKeys: appendUnique(
      state.loadedEnvironmentKeys,
      nextEnvironmentKey,
    ),
    status: "ready",
    error: null,
  };
}

export function markTopicLoadFailed(
  state: TopicRailState,
  nextEnvironmentKey: string,
  error: string,
): TopicRailState {
  return {
    ...state,
    environmentKey: nextEnvironmentKey,
    status: "error",
    error,
  };
}

export function setTopicSearch(
  state: TopicRailState,
  searchQuery: string,
): TopicRailState {
  return { ...state, searchQuery };
}

export function visibleTopicRows(state: TopicRailState): TopicListRow[] {
  const environmentPins = pinsForEnvironment(state);
  return filterAndOrderTopics(state.topics, state.searchQuery, environmentPins);
}

export function filterAndOrderTopics(
  topics: KafkaTopicMetadata[],
  searchQuery: string,
  pinnedTopicNames: string[],
): TopicListRow[] {
  const normalizedQuery = searchQuery.trim().toLocaleLowerCase();
  const pinned = new Set(pinnedTopicNames);
  const matchingTopics = topics.filter((topic) =>
    normalizedQuery.length === 0
      ? true
      : topic.name.toLocaleLowerCase().includes(normalizedQuery),
  );
  const rows = matchingTopics.map((topic) => ({
    ...topic,
    pinned: pinned.has(topic.name),
  }));

  return [
    ...rows.filter((topic) => topic.pinned),
    ...rows.filter((topic) => !topic.pinned),
  ];
}

export function toggleTopicPin(
  state: TopicRailState,
  environment: string,
  topicName: string,
  store?: TopicPinStore,
): TopicRailState {
  const nextPinnedTopicsByEnvironment = togglePinnedTopicForEnvironment(
    state.pinnedTopicsByEnvironment,
    environment,
    topicName,
  );
  writePinnedTopics(nextPinnedTopicsByEnvironment, store);

  return {
    ...state,
    pinnedTopicsByEnvironment: nextPinnedTopicsByEnvironment,
  };
}

export function removePinnedTopicsForEnvironment(
  environment: string,
  store?: TopicPinStore,
): Record<string, string[]> {
  const pinnedTopicsByEnvironment = readPinnedTopics(store);
  const { [environment]: _deletedEnvironment, ...nextPinnedTopicsByEnvironment } =
    pinnedTopicsByEnvironment;

  writePinnedTopics(nextPinnedTopicsByEnvironment, store);
  return nextPinnedTopicsByEnvironment;
}

export function togglePinnedTopicForEnvironment(
  pinnedTopicsByEnvironment: Record<string, string[]>,
  environment: string,
  topicName: string,
): Record<string, string[]> {
  const currentPins = pinnedTopicsByEnvironment[environment] ?? [];
  const nextPins = currentPins.includes(topicName)
    ? currentPins.filter((pinnedTopic) => pinnedTopic !== topicName)
    : [...currentPins, topicName];

  return {
    ...pinnedTopicsByEnvironment,
    [environment]: nextPins,
  };
}

export async function loadTopicsForEnvironment(
  state: TopicRailState,
  auth: RuntimeAuthConfig,
  fetchTopics: TopicFetch,
  force = false,
): Promise<TopicRailState> {
  const nextEnvironmentKey = environmentKey(auth);
  if (!shouldLoadTopicsForEnvironment(state, nextEnvironmentKey, force)) {
    return openTopicEnvironment(state, nextEnvironmentKey);
  }

  const loading = markTopicLoadStarted(state, nextEnvironmentKey);

  try {
    const topics = await fetchTopics(auth);
    return markTopicLoadSucceeded(loading, nextEnvironmentKey, topics);
  } catch (cause) {
    return markTopicLoadFailed(
      loading,
      nextEnvironmentKey,
      cause instanceof Error ? cause.message : "Topic list refresh failed",
    );
  }
}

export function pinsForEnvironment(state: TopicRailState): string[] {
  return state.environmentKey
    ? (state.pinnedTopicsByEnvironment[state.environmentKey] ?? [])
    : [];
}

export function readPinnedTopics(
  store?: TopicPinStore,
): Record<string, string[]> {
  if (!store) {
    return {};
  }

  try {
    const raw = store.getItem(PIN_STORAGE_KEY);
    if (!raw) {
      return {};
    }

    return parsePinnedTopics(JSON.parse(raw));
  } catch {
    return {};
  }
}

export function writePinnedTopics(
  pinnedTopicsByEnvironment: Record<string, string[]>,
  store?: TopicPinStore,
) {
  if (!store) {
    return;
  }

  store.setItem(PIN_STORAGE_KEY, JSON.stringify(pinnedTopicsByEnvironment));
}

function parsePinnedTopics(value: unknown): Record<string, string[]> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).flatMap(([environment, topics]) => {
      if (!Array.isArray(topics)) {
        return [];
      }

      return [
        [
          environment,
          topics.filter((topic): topic is string => typeof topic === "string"),
        ],
      ];
    }),
  );
}

function appendUnique(values: string[], value: string): string[] {
  return values.includes(value) ? values : [...values, value];
}

function sortTopics(topics: KafkaTopicMetadata[]): KafkaTopicMetadata[] {
  return [...topics].sort((left, right) => left.name.localeCompare(right.name));
}
