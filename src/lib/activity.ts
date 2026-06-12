import type { MilenaBoundaryEvent } from "./tauri";

export type GlobalActivitySeverity = "info" | "error";
export type GlobalActivitySource = "app" | "topics" | "consumer" | "producer";

export type GlobalActivityEntry = {
  id: string;
  severity: GlobalActivitySeverity;
  source: GlobalActivitySource;
  message: string;
  detail: string | null;
  paneId: number | null;
  time: string;
};

export const MAX_GLOBAL_ACTIVITY = 50;

export function appendGlobalActivity(
  entries: GlobalActivityEntry[],
  entry: Omit<GlobalActivityEntry, "id" | "time">,
  options: {
    now?: () => Date;
    limit?: number;
    nextId?: () => string;
  } = {},
): GlobalActivityEntry[] {
  const now = options.now?.() ?? new Date();
  const nextEntry: GlobalActivityEntry = {
    ...entry,
    id: options.nextId?.() ?? createActivityId(now),
    time: now.toLocaleTimeString(),
  };

  return [nextEntry, ...entries].slice(0, options.limit ?? MAX_GLOBAL_ACTIVITY);
}

export function appendGlobalError(
  entries: GlobalActivityEntry[],
  source: GlobalActivitySource,
  message: string,
  options: {
    detail?: string | null;
    paneId?: number | null;
    now?: () => Date;
    limit?: number;
    nextId?: () => string;
  } = {},
): GlobalActivityEntry[] {
  return appendGlobalActivity(
    entries,
    {
      severity: "error",
      source,
      message,
      detail: options.detail ?? null,
      paneId: options.paneId ?? null,
    },
    options,
  );
}

export function appendBoundaryEventActivity(
  entries: GlobalActivityEntry[],
  event: MilenaBoundaryEvent,
  paneId: number,
  options: {
    now?: () => Date;
    limit?: number;
    nextId?: () => string;
  } = {},
): GlobalActivityEntry[] {
  return appendGlobalActivity(
    entries,
    {
      severity: event.event === "kafkaConsumerError" ? "error" : "info",
      source: "consumer",
      message: event.event,
      detail: boundaryEventDetail(event),
      paneId,
    },
    options,
  );
}

export function clearGlobalActivity(): GlobalActivityEntry[] {
  return [];
}

function createActivityId(now: Date): string {
  return `${now.getTime()}-${Math.random().toString(36).slice(2)}`;
}

function boundaryEventDetail(event: MilenaBoundaryEvent): string | null {
  switch (event.event) {
    case "kafkaRecord":
      return `${event.data.record.topic} p${event.data.record.partition} / ${event.data.record.offset}`;
    case "kafkaConsumerError":
      return event.data.message;
    case "kafkaConsumerStarted":
    case "kafkaConsumerStopped":
      return event.data.groupId;
    case "boundaryOpened":
      return event.data.topic;
    case "boundaryReady":
      return event.data.sessionId;
  }
}
