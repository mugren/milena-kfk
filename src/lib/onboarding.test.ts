import { describe, expect, it } from "vitest";
import {
  cancelOnboardingForm,
  cancelDeleteEnvironment,
  confirmDeleteEnvironment,
  createOnboardingState,
  getSelectedEnvironment,
  requestDeleteEnvironment,
  saveOnboardingForm,
  selectEnvironment,
  setOnboardingFormField,
  setOnboardingFormTestResult,
  startAddEnvironment,
  startEditEnvironment,
  validateBrokerEntry,
  validateBrokerList,
  type OnboardingEnvironment,
  type SaveOnboardingFormResult,
} from "./onboarding";

const localEnvironment: OnboardingEnvironment = {
  name: "Local Dev",
  brokers: ["localhost:19092"],
  authMode: "plaintext",
  username: "",
  advancedPropertiesText: "",
};

const stagingEnvironment: OnboardingEnvironment = {
  name: "Staging",
  brokers: ["staging.kafka.internal:9094"],
  authMode: "saslSslScramSha512",
  username: "deploy",
  advancedPropertiesText: "client.dns.lookup=use_all_dns_ips",
};

describe("onboarding state", () => {
  it("starts first-run setup in an add form and can cancel to an empty chooser", () => {
    const firstRun = createOnboardingState({ environments: [] });

    expect(firstRun.mode).toBe("add");
    expect(firstRun.form?.mode).toBe("add");
    expect(firstRun.environments).toEqual([]);
    expect(firstRun.selectedEnvironmentName).toBeNull();
    expect(firstRun.activeRuntimeAuth).toBeNull();

    const cancelled = cancelOnboardingForm(firstRun);

    expect(cancelled.mode).toBe("list");
    expect(cancelled.form).toBeNull();
    expect(cancelled.environments).toEqual([]);
    expect(cancelled.selectedEnvironmentName).toBeNull();
    expect(cancelled.activeRuntimeAuth).toBeNull();
  });

  it("saves an added environment without opening a workspace", () => {
    const adding = startAddEnvironment(
      createOnboardingState({ environments: [] }),
    );
    const saved = saveOnboardingForm({
      ...adding,
      form: {
        ...adding.form!,
        values: {
          ...adding.form!.values,
          name: "Local Dev",
          brokersText: "localhost:19092",
        },
      },
    });
    const savedResult = expectFormSaved(saved);

    expect(savedResult.state.mode).toBe("list");
    expect(savedResult.state.selectedEnvironmentName).toBe("Local Dev");
    expect(savedResult.state.lastSelectedEnvironmentName).toBe("Local Dev");
    expect(savedResult.state.activeRuntimeAuth).toBeNull();
    expect(savedResult.state.environments).toEqual([
      {
        name: "Local Dev",
        brokers: ["localhost:19092"],
        authMode: "plaintext",
        username: "",
        advancedPropertiesText: "",
      },
    ]);
  });

  it("lists environments with remembered selection but does not auto-open it", () => {
    const state = createOnboardingState({
      environments: [stagingEnvironment, localEnvironment],
      lastSelectedEnvironmentName: "staging",
    });

    expect(state.mode).toBe("list");
    expect(state.environments.map((environment) => environment.name)).toEqual([
      "Local Dev",
      "Staging",
    ]);
    expect(state.selectedEnvironmentName).toBe("Staging");
    expect(state.lastSelectedEnvironmentName).toBe("Staging");
    expect(getSelectedEnvironment(state)).toEqual(stagingEnvironment);
    expect(state.activeRuntimeAuth).toBeNull();
  });

  it("selects an environment for chooser actions without opening it", () => {
    const state = selectEnvironment(
      createOnboardingState({
        environments: [localEnvironment, stagingEnvironment],
      }),
      "Staging",
    );

    expect(state.selectedEnvironmentName).toBe("Staging");
    expect(state.lastSelectedEnvironmentName).toBe("Staging");
    expect(state.activeRuntimeAuth).toBeNull();
  });

  it("accepts Kafka bootstrap broker entries with valid ports", () => {
    expect(validateBrokerEntry("localhost:9092")).toEqual({ ok: true });
    expect(validateBrokerEntry("broker-1.kafka.local:65535")).toEqual({
      ok: true,
    });
    expect(validateBrokerEntry("127.0.0.1:1")).toEqual({ ok: true });
    expect(validateBrokerEntry("[::1]:9092")).toEqual({ ok: true });
    expect(validateBrokerEntry("[2001:db8::10]:9094")).toEqual({ ok: true });
    expect(validateBrokerList("localhost:9092, [::1]:9093")).toEqual({
      ok: true,
      brokers: ["localhost:9092", "[::1]:9093"],
    });
  });

  it("rejects broker entries with schemes, paths, missing ports, or invalid ports", () => {
    expect(validateBrokerEntry("http://localhost:9092").ok).toBe(false);
    expect(validateBrokerEntry("localhost:9092/path").ok).toBe(false);
    expect(validateBrokerEntry("localhost").ok).toBe(false);
    expect(validateBrokerEntry("[::1]").ok).toBe(false);
    expect(validateBrokerEntry("localhost:0").ok).toBe(false);
    expect(validateBrokerEntry("localhost:65536").ok).toBe(false);
    expect(validateBrokerEntry("localhost:not-a-port").ok).toBe(false);
    expect(validateBrokerEntry("2001:db8::10:9092").ok).toBe(false);
    expect(validateBrokerEntry("[2001:db8:::10]:9092").ok).toBe(false);
  });

  it("blocks duplicate add names case-insensitively", () => {
    const state = setOnboardingFormField(
      setOnboardingFormField(
        startAddEnvironment(
          createOnboardingState({ environments: [localEnvironment] }),
        ),
        "name",
        " local dev ",
      ),
      "brokersText",
      "localhost:19093",
    );

    const result = saveOnboardingForm(state);
    const rejected = expectFormRejected(result);

    expect(rejected.errors.name).toBe("Environment name already exists");
  });

  it("opens edit forms with immutable names and saves changes under the original name", () => {
    let state = startEditEnvironment(
      createOnboardingState({ environments: [localEnvironment] }),
      "Local Dev",
    );

    expect(state.mode).toBe("edit");
    expect(state.form?.mode).toBe("edit");
    expect(state.form?.values.name).toBe("Local Dev");

    state = setOnboardingFormField(state, "name", "Renamed");
    state = setOnboardingFormField(state, "brokersText", "127.0.0.1:29092");
    const result = saveOnboardingForm(state);
    const saved = expectFormSaved(result);

    expect(saved.environment.name).toBe("Local Dev");
    expect(saved.state.environments).toEqual([
      {
        ...localEnvironment,
        brokers: ["127.0.0.1:29092"],
      },
    ]);
  });

  it("clears stale local test results when form fields change", () => {
    const tested = setOnboardingFormTestResult(
      startEditEnvironment(
        createOnboardingState({ environments: [localEnvironment] }),
        "Local Dev",
      ),
      { status: "success", message: "Connected" },
    );

    expect(tested.form?.testResult).toEqual({
      status: "success",
      message: "Connected",
    });

    const changed = setOnboardingFormField(
      tested,
      "brokersText",
      "localhost:29092",
    );

    expect(changed.form?.testResult).toBeNull();
  });

  it("validates auth-mode-specific fields before saving", () => {
    for (const authMode of ["saslSslPlain", "saslSslScramSha512"] as const) {
      const state = setOnboardingFormField(
        setOnboardingFormField(
          setOnboardingFormField(
            startAddEnvironment(createOnboardingState({ environments: [] })),
            "name",
            authMode === "saslSslPlain" ? "Local Compose" : "Secure Dev",
          ),
          "brokersText",
          authMode === "saslSslPlain"
            ? "localhost:19092"
            : "secure.kafka.internal:9094",
        ),
        "authMode",
        authMode,
      );

      const missingCredentials = saveOnboardingForm(state);
      const rejected = expectFormRejected(missingCredentials);

      expect(rejected.errors.username).toBe("Username is required");
      expect(rejected.errors.password).toBe("Password is required");

      const withCredentials = saveOnboardingForm(
        setOnboardingFormField(
          setOnboardingFormField(state, "username", "deploy"),
          "password",
          "secret",
        ),
      );
      const saved = expectFormSaved(withCredentials);

      expect(saved.environment).toMatchObject({
        authMode,
        username: "deploy",
      });
    }
  });

  it("allows an existing SCRAM environment to keep its password blank while editing", () => {
    const editing = setOnboardingFormField(
      startEditEnvironment(
        createOnboardingState({ environments: [stagingEnvironment] }),
        "Staging",
      ),
      "brokersText",
      "staging.kafka.internal:9095",
    );

    const result = saveOnboardingForm(editing);
    const saved = expectFormSaved(result);

    expect(saved.environment).toMatchObject({
      name: "Staging",
      authMode: "saslSslScramSha512",
      username: "deploy",
    });
  });

  it("models delete confirmation before removing a selected environment", () => {
    const confirming = requestDeleteEnvironment(
      createOnboardingState({
        environments: [localEnvironment, stagingEnvironment],
        lastSelectedEnvironmentName: "Local Dev",
      }),
    );

    expect(confirming.deleteConfirmation).toEqual({
      environmentName: "Local Dev",
    });
    expect(confirming.environments).toHaveLength(2);

    const cancelled = cancelDeleteEnvironment(confirming);

    expect(cancelled.deleteConfirmation).toBeNull();
    expect(cancelled.environments).toHaveLength(2);

    const deleted = confirmDeleteEnvironment(confirming);

    expect(deleted.deleteConfirmation).toBeNull();
    expect(deleted.environments.map((environment) => environment.name)).toEqual([
      "Staging",
    ]);
    expect(deleted.selectedEnvironmentName).toBe("Staging");
    expect(deleted.activeRuntimeAuth).toBeNull();
  });
});

function expectFormSaved(
  result: SaveOnboardingFormResult,
): Extract<SaveOnboardingFormResult, { ok: true }> {
  expect(result.ok).toBe(true);
  if (!result.ok) {
    throw new Error("Expected onboarding form save to succeed");
  }
  return result;
}

function expectFormRejected(
  result: SaveOnboardingFormResult,
): Extract<SaveOnboardingFormResult, { ok: false }> {
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("Expected onboarding form save to fail");
  }
  return result;
}
