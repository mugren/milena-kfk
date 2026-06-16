export const APPEARANCE_STORAGE_KEY = "milena.appearance.v1";

export type AppearancePreference = "system" | "light" | "dark";

export type ResolvedAppearance = "light" | "dark";

export type AppearancePreferenceStore = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
};

export type AppearanceMatchMedia = (
  query: string,
) => Pick<MediaQueryList, "matches">;

export type AppearanceDocument = {
  documentElement: {
    dataset: {
      theme?: string;
    };
  };
};

export function isAppearancePreference(
  value: unknown,
): value is AppearancePreference {
  return value === "system" || value === "light" || value === "dark";
}

export function readAppearancePreference(
  store?: AppearancePreferenceStore,
): AppearancePreference {
  try {
    const raw =
      getPreferenceStore(store)?.getItem(APPEARANCE_STORAGE_KEY) ?? null;
    return isAppearancePreference(raw) ? raw : "system";
  } catch {
    return "system";
  }
}

export function writeAppearancePreference(
  preference: AppearancePreference,
  store?: AppearancePreferenceStore,
) {
  try {
    getPreferenceStore(store)?.setItem(APPEARANCE_STORAGE_KEY, preference);
  } catch {
    return;
  }
}

export function resolveAppearancePreference(
  preference: AppearancePreference,
  matchMedia?: AppearanceMatchMedia,
): ResolvedAppearance {
  if (preference !== "system") {
    return preference;
  }

  try {
    return getMatchMedia(matchMedia)?.("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  } catch {
    return "light";
  }
}

export function applyAppearancePreference(
  preference: AppearancePreference,
  document?: AppearanceDocument,
  matchMedia?: AppearanceMatchMedia,
): ResolvedAppearance {
  const resolved = resolveAppearancePreference(preference, matchMedia);
  const rootDocument = getAppearanceDocument(document);

  if (rootDocument) {
    rootDocument.documentElement.dataset.theme = resolved;
  }

  return resolved;
}

function getPreferenceStore(
  store?: AppearancePreferenceStore,
): AppearancePreferenceStore | undefined {
  const candidate = store ?? globalThis.localStorage;
  return isPreferenceStore(candidate) ? candidate : undefined;
}

function getMatchMedia(
  matchMedia?: AppearanceMatchMedia,
): AppearanceMatchMedia | undefined {
  return matchMedia ?? globalThis.matchMedia?.bind(globalThis);
}

function getAppearanceDocument(
  document?: AppearanceDocument,
): AppearanceDocument | undefined {
  const candidate = document ?? globalThis.document;
  return isAppearanceDocument(candidate) ? candidate : undefined;
}

function isPreferenceStore(value: unknown): value is AppearancePreferenceStore {
  return (
    !!value &&
    typeof value === "object" &&
    "getItem" in value &&
    typeof value.getItem === "function" &&
    "setItem" in value &&
    typeof value.setItem === "function"
  );
}

function isAppearanceDocument(value: unknown): value is AppearanceDocument {
  return (
    !!value &&
    typeof value === "object" &&
    "documentElement" in value &&
    !!value.documentElement &&
    typeof value.documentElement === "object" &&
    "dataset" in value.documentElement &&
    !!value.documentElement.dataset &&
    typeof value.documentElement.dataset === "object"
  );
}
