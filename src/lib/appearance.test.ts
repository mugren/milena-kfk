import { afterEach, describe, expect, it, vi } from "vitest";
import {
  APPEARANCE_STORAGE_KEY,
  applyAppearancePreference,
  isAppearancePreference,
  readAppearancePreference,
  resolveAppearancePreference,
  writeAppearancePreference,
} from "./appearance";

function memoryStore(initial?: Record<string, string>) {
  const values = new Map(Object.entries(initial ?? {}));

  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
  };
}

describe("appearance preference", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reads a missing stored preference as system", () => {
    expect(readAppearancePreference(memoryStore())).toBe("system");
    expect(
      readAppearancePreference(
        memoryStore({ [APPEARANCE_STORAGE_KEY]: "" }),
      ),
    ).toBe("system");
  });

  it("round-trips valid stored preferences exactly", () => {
    for (const preference of ["system", "light", "dark"] as const) {
      const store = memoryStore();

      writeAppearancePreference(preference, store);

      expect(store.getItem(APPEARANCE_STORAGE_KEY)).toBe(preference);
      expect(readAppearancePreference(store)).toBe(preference);
    }
  });

  it("rejects invalid preference values", () => {
    expect(isAppearancePreference("system")).toBe(true);
    expect(isAppearancePreference("light")).toBe(true);
    expect(isAppearancePreference("dark")).toBe(true);
    expect(isAppearancePreference("sepia")).toBe(false);
    expect(isAppearancePreference(null)).toBe(false);

    expect(
      readAppearancePreference(
        memoryStore({ [APPEARANCE_STORAGE_KEY]: "sepia" }),
      ),
    ).toBe("system");
  });

  it("uses localStorage by default when it is available", () => {
    const store = memoryStore({ [APPEARANCE_STORAGE_KEY]: "dark" });
    vi.stubGlobal("localStorage", store);

    expect(readAppearancePreference()).toBe("dark");

    writeAppearancePreference("light");

    expect(store.getItem(APPEARANCE_STORAGE_KEY)).toBe("light");
  });

  it("resolves explicit and system preferences to light or dark", () => {
    const darkSystem = () => ({ matches: true });
    const lightSystem = () => ({ matches: false });

    expect(resolveAppearancePreference("dark", lightSystem)).toBe("dark");
    expect(resolveAppearancePreference("light", darkSystem)).toBe("light");
    expect(resolveAppearancePreference("system", darkSystem)).toBe("dark");
    expect(resolveAppearancePreference("system", lightSystem)).toBe("light");
  });

  it("applies the resolved theme to the document root", () => {
    const document = {
      documentElement: {
        dataset: {} as Record<string, string>,
      },
    };

    expect(
      applyAppearancePreference("system", document, () => ({ matches: true })),
    ).toBe("dark");
    expect(document.documentElement.dataset.theme).toBe("dark");

    expect(
      applyAppearancePreference("light", document, () => ({ matches: true })),
    ).toBe("light");
    expect(document.documentElement.dataset.theme).toBe("light");
  });

  it("is safe when browser APIs are unavailable or unusable", () => {
    const throwingStore = {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => {
        throw new Error("storage unavailable");
      },
    };
    const throwingMatchMedia = () => {
      throw new Error("matchMedia unavailable");
    };

    expect(readAppearancePreference()).toBe("system");
    expect(() => readAppearancePreference(throwingStore)).not.toThrow();
    expect(() => writeAppearancePreference("dark", throwingStore)).not.toThrow();
    expect(resolveAppearancePreference("system", throwingMatchMedia)).toBe(
      "light",
    );
    expect(() => applyAppearancePreference("dark")).not.toThrow();
  });
});
