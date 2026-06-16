/**
 * @vitest-environment jsdom
 */
import "@testing-library/jest-dom/vitest";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  cleanupRendererHarness,
  emit,
  kafkaRecord,
  lastSelectedEnvironmentStorageKey,
  messagePreferencesStorageKey,
  openTopic,
  pane,
  queryTopicSelect,
  renderAppChooser,
  resetRendererHarness,
  stagingRuntimeAuth,
  tauri,
  topicPinStorageKey,
  topicSelect,
} from "./App.renderer.test-utils";

beforeEach(resetRendererHarness);
afterEach(cleanupRendererHarness);

describe("App onboarding renderer flow", () => {
  it("exposes compact appearance choices in the environment chooser", async () => {
    const { user } = renderAppChooser();

    const chooser = await screen.findByLabelText("Environment chooser");
    const appearance = within(chooser).getByRole("group", {
      name: "Appearance",
    });
    const system = within(appearance).getByRole("button", {
      name: "Use system appearance",
    });
    const light = within(appearance).getByRole("button", {
      name: "Use light appearance",
    });
    const dark = within(appearance).getByRole("button", {
      name: "Use dark appearance",
    });

    expect(system).toHaveAttribute("aria-pressed", "true");
    expect(light).toHaveAttribute("aria-pressed", "false");
    expect(dark).toHaveAttribute("aria-pressed", "false");
    expect(system).toHaveAttribute("title", "System");
    expect(light).toHaveAttribute("title", "Light");
    expect(dark).toHaveAttribute("title", "Dark");
    expect(system).toHaveTextContent("");
    expect(light).toHaveTextContent("");
    expect(dark).toHaveTextContent("");
    expect(system.querySelector("svg")).not.toBeNull();
    expect(light.querySelector("svg")).not.toBeNull();
    expect(dark.querySelector("svg")).not.toBeNull();

    await user.click(light);

    expect(system).toHaveAttribute("aria-pressed", "false");
    expect(light).toHaveAttribute("aria-pressed", "true");
    expect(dark).toHaveAttribute("aria-pressed", "false");
    expect(document.documentElement).toHaveAttribute("data-theme", "light");
  });

  it("opens the add form on first run and saves without opening a workspace", async () => {
    tauri.listEnvironments.mockResolvedValueOnce({ environments: [] });
    const { user } = renderAppChooser();

    expect(await screen.findByLabelText("Add environment")).toBeVisible();
    expect(
      within(screen.getByLabelText("Saved environments")).getByText(
        "No saved environments",
      ),
    ).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("Environment name"), "QA");
    await user.type(screen.getByLabelText("Kafka brokers"), "qa.kafka.internal:9094");
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(tauri.saveEnvironment).toHaveBeenCalledOnce());
    expect(tauri.saveEnvironment).toHaveBeenCalledWith({
      name: "QA",
      brokers: ["qa.kafka.internal:9094"],
      authMode: "plaintext",
      username: null,
      password: null,
      advancedProperties: "",
    });
    expect(await screen.findByText("Saved QA")).toBeVisible();
    expect(screen.getByRole("button", { name: /QA/ }))
      .toHaveAttribute("aria-current", "true");
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
  });

  it("cancels the first-run add form with Escape", async () => {
    tauri.listEnvironments.mockResolvedValueOnce({ environments: [] });
    const { user } = renderAppChooser();

    expect(await screen.findByLabelText("Add environment")).toBeVisible();

    await user.keyboard("{Escape}");

    expect(screen.queryByLabelText("Add environment")).not.toBeInTheDocument();
    expect(
      within(screen.getByLabelText("Saved environments")).getByText(
        "No saved environments",
      ),
    ).toBeVisible();
    expect(screen.getAllByText("Chooser ready")).toHaveLength(2);
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
  });

  it("refreshes saved environments, preserves selected row state, and saves added environments in the chooser", async () => {
    window.localStorage.setItem(lastSelectedEnvironmentStorageKey, "staging");
    const { user } = renderAppChooser();

    const chooser = await screen.findByLabelText("Environment chooser");
    const list = within(chooser).getByLabelText("Saved environments");
    const rows = await within(list).findAllByRole("button");
    expect(rows.map((row) => row.textContent)).toEqual([
      "Local Devlocalhost:19092PLAINTEXT",
      "Stagingstaging.kafka.internal:9094deploySASL_SSL SCRAM",
    ]);
    expect(within(list).getByRole("button", { name: /Staging/ }))
      .toHaveAttribute("aria-current", "true");
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(tauri.listEnvironments).toHaveBeenCalledOnce();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Refresh environments" }));
    await waitFor(() => expect(tauri.listEnvironments).toHaveBeenCalledTimes(2));

    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.type(screen.getByLabelText("Environment name"), "QA");
    await user.type(screen.getByLabelText("Kafka brokers"), "qa.kafka.internal:9094");
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslScramSha512");
    await user.type(screen.getByLabelText("Kafka username"), "qa-user");
    await user.type(screen.getByLabelText("Kafka password"), "qa-secret");
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "client.id=milena-qa",
    );
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(tauri.saveEnvironment).toHaveBeenCalledOnce());
    expect(tauri.saveEnvironment).toHaveBeenCalledWith({
      name: "QA",
      brokers: ["qa.kafka.internal:9094"],
      authMode: "saslSslScramSha512",
      username: "qa-user",
      password: "qa-secret",
      advancedProperties: "client.id=milena-qa",
    });
    expect(await screen.findByText("Saved QA")).toBeVisible();
    expect(screen.getByRole("button", { name: /QA/ }))
      .toHaveAttribute("aria-current", "true");
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
    expect(tauri.publishKafkaRecord).not.toHaveBeenCalled();
  });

  it("saves local compose SASL_SSL PLAIN credentials", async () => {
    tauri.listEnvironments.mockResolvedValueOnce({ environments: [] });
    const { user } = renderAppChooser();

    await screen.findByLabelText("Add environment");
    await user.type(screen.getByLabelText("Environment name"), "local");
    await user.type(screen.getByLabelText("Kafka brokers"), "localhost:19092");
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslPlain");
    await user.type(screen.getByLabelText("Kafka username"), "milena_plain");
    await user.type(screen.getByLabelText("Kafka password"), "milena-plain-secret");
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "ssl.ca.location=docker/kafka/generated/ssl/ca.crt",
    );
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(tauri.saveEnvironment).toHaveBeenCalledOnce());
    expect(tauri.saveEnvironment).toHaveBeenCalledWith({
      name: "local",
      brokers: ["localhost:19092"],
      authMode: "saslSslPlain",
      username: "milena_plain",
      password: "milena-plain-secret",
      advancedProperties: "ssl.ca.location=docker/kafka/generated/ssl/ca.crt",
    });
    expect(await screen.findByText("Saved local")).toBeVisible();
  });

  it("shows structured save errors from Tauri commands", async () => {
    tauri.listEnvironments.mockResolvedValueOnce({ environments: [] });
    tauri.saveEnvironment.mockRejectedValueOnce({
      code: "environment-secret-store-failed",
      message: "Keychain is locked",
    });
    const { user } = renderAppChooser();

    await screen.findByLabelText("Add environment");
    await user.type(screen.getByLabelText("Environment name"), "local");
    await user.type(screen.getByLabelText("Kafka brokers"), "localhost:19092");
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslPlain");
    await user.type(screen.getByLabelText("Kafka username"), "milena_plain");
    await user.type(screen.getByLabelText("Kafka password"), "milena-plain-secret");
    await user.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByText("Keychain is locked")).toBeVisible();
    expect(screen.queryByText("Save failed")).not.toBeInTheDocument();
  });

  it("saves edited environments without opening a workspace", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.click(screen.getByRole("button", { name: "Edit" }));
    expect(screen.getByLabelText("Environment name")).toHaveValue("Staging");
    expect(screen.getByLabelText("Environment name")).toBeDisabled();

    await user.clear(screen.getByLabelText("Kafka brokers"));
    await user.type(screen.getByLabelText("Kafka brokers"), "staging.kafka.internal:9095");
    await user.clear(screen.getByLabelText("Advanced Kafka properties"));
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "client.id=milena-staging",
    );
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(tauri.saveEnvironment).toHaveBeenCalledOnce());
    expect(tauri.saveEnvironment).toHaveBeenCalledWith({
      name: "Staging",
      brokers: ["staging.kafka.internal:9095"],
      authMode: "saslSslScramSha512",
      username: "deploy",
      password: null,
      advancedProperties: "client.id=milena-staging",
    });
    expect(await screen.findByText("Saved Staging")).toBeVisible();
    expect(screen.getByRole("button", { name: /Staging/ }))
      .toHaveAttribute("aria-current", "true");
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
  });

  it("opens a selected environment only after runtime auth materializes", async () => {
    tauri.materializeRuntimeAuthConfig
      .mockRejectedValueOnce(new Error("keychain secret missing"))
      .mockResolvedValueOnce(stagingRuntimeAuth);
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.click(screen.getByRole("button", { name: "Open" }));

    expect(await screen.findByText("keychain secret missing")).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Activity log")).not.toBeInTheDocument();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Open" }));

    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledOnce());
    expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledWith("Staging");
    expect(tauri.listKafkaTopics).toHaveBeenCalledWith({
      auth: stagingRuntimeAuth,
    });
    expect(screen.getByLabelText("Kafka topics")).toHaveTextContent("Staging");
    expect(screen.getByLabelText("Kafka topics")).toHaveTextContent(
      "staging.kafka.internal:9094",
    );
    expect(screen.getByLabelText("Milena workspace")).toHaveTextContent(
      "Select a topic to preview it.",
    );
  });

  it("opens the selected environment from the chooser with Enter", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.keyboard("{Enter}");

    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledOnce());
    expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledWith("Staging");
    expect(screen.getByLabelText("Kafka topics")).toHaveTextContent("Staging");
  });

  it("keeps Enter as text editing inside advanced properties", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.type(screen.getByLabelText("Environment name"), "QA");
    await user.type(screen.getByLabelText("Kafka brokers"), "qa.kafka.internal:9094");
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "client.id=milena-qa",
    );
    await user.keyboard("{Enter}");

    expect(screen.getByLabelText("Advanced Kafka properties")).toHaveValue(
      "client.id=milena-qa\n",
    );
    expect(screen.getByLabelText("Add environment")).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(tauri.saveEnvironment).not.toHaveBeenCalled();
    expect(tauri.materializeRuntimeAuthConfig).not.toHaveBeenCalled();
  });

  it("changes environment by resetting workspace activity, stopping sessions, and ignoring stale events", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.click(screen.getByRole("button", { name: "Open" }));
    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    await waitFor(() => expect(topicSelect("orders.created")).toBeVisible());

    await openTopic(user, "orders.created");
    await user.click(
      within(screen.getByLabelText("Publisher for pane 1")).getByRole("button", {
        name: "Expand publisher for pane 1",
      }),
    );
    fireEvent.change(screen.getByLabelText("JSON payload for pane 1"), {
      target: { value: "{\"draft\":true}" },
    });
    await user.click(within(pane("1")).getByRole("button", { name: "Poll" }));
    await screen.findByText("session-1");
    emit("session-1", kafkaRecord("session-1", {
      offset: 9,
      payload: "{\"beforeChange\":true}",
    }));
    expect(await screen.findByText("{\"beforeChange\":true}")).toBeVisible();
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("kafkaRecord");

    await user.click(screen.getByRole("button", { name: "Change environment" }));

    expect(await screen.findByLabelText("Environment chooser")).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Staging/ }))
      .toHaveAttribute("aria-current", "true");
    await waitFor(() =>
      expect(tauri.stopKafkaConsumerSession).toHaveBeenCalledWith({
        sessionId: "session-1",
      }),
    );

    emit("session-1", kafkaRecord("session-1", {
      offset: 10,
      payload: "{\"stale\":true}",
    }));
    await user.click(screen.getByRole("button", { name: "Open" }));

    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledTimes(2));
    expect(screen.queryByLabelText("Pane 1")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Milena workspace")).toHaveTextContent(
      "Select a topic to preview it.",
    );
    expect(screen.getByLabelText("Activity log")).toHaveTextContent("Inactive");
    expect(screen.getByLabelText("Activity log")).not.toHaveTextContent(
      "kafkaRecord",
    );
    expect(screen.queryByText("{\"beforeChange\":true}")).not.toBeInTheDocument();
    expect(screen.queryByText("{\"stale\":true}")).not.toBeInTheDocument();

    await openTopic(user, "orders.created");
    await user.click(
      within(screen.getByLabelText("Publisher for pane 1")).getByRole("button", {
        name: "Expand publisher for pane 1",
      }),
    );
    expect(screen.getByLabelText("JSON payload for pane 1")).toHaveValue(`{
  "topic": "orders.created",
  "event": "preview"
}`);
  });

  it("keeps the workspace open when forced topic loading fails after open", async () => {
    tauri.listKafkaTopics.mockRejectedValueOnce(new Error("SASL auth failed"));
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Open" }));

    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    expect(await screen.findAllByText("SASL auth failed")).toHaveLength(2);
    expect(screen.getByLabelText("Activity log")).toHaveTextContent(
      "Topic list refresh failed",
    );
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledTimes(2));
    expect(topicSelect("orders.created")).toBeVisible();
    expect(screen.queryAllByText("SASL auth failed")).toHaveLength(1);
  });

  it("requires chooser confirmation before deleting an environment", async () => {
    const { user } = renderAppChooser();

    await screen.findByRole("button", { name: /Local Dev/ });
    await user.click(screen.getByRole("button", { name: "Delete" }));

    expect(
      screen.getByText('Delete environment "Local Dev"?'),
    ).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Cancel delete" }));

    expect(
      screen.queryByText('Delete environment "Local Dev"?'),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Local Dev/ })).toBeVisible();
    expect(tauri.deleteEnvironment).not.toHaveBeenCalled();
  });

  it("cancels edit forms and delete confirmation with Escape", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.click(screen.getByRole("button", { name: "Edit" }));

    expect(screen.getByLabelText("Edit environment")).toBeVisible();

    await user.keyboard("{Escape}");

    expect(screen.queryByLabelText("Edit environment")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open" })).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Delete" }));

    expect(screen.getByText('Delete environment "Staging"?')).toBeVisible();

    await user.keyboard("{Escape}");

    expect(
      screen.queryByText('Delete environment "Staging"?'),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Staging/ }))
      .toHaveAttribute("aria-current", "true");
    expect(screen.getByRole("button", { name: "Open" })).toBeVisible();
    expect(tauri.deleteEnvironment).not.toHaveBeenCalled();
  });

  it("deletes a confirmed chooser environment and cleans local preferences", async () => {
    window.localStorage.setItem(lastSelectedEnvironmentStorageKey, "Local Dev");
    window.localStorage.setItem(
      topicPinStorageKey,
      JSON.stringify({
        "Local Dev": ["orders.created"],
        Staging: ["payments.authorized"],
      }),
    );
    window.localStorage.setItem(
      messagePreferencesStorageKey,
      JSON.stringify({
        "Local Dev": {
          "orders.created": "raw",
        },
        Staging: {
          "payments.authorized": "json",
        },
      }),
    );
    const { user } = renderAppChooser();

    await screen.findByRole("button", { name: /Local Dev/ });
    await user.click(screen.getByRole("button", { name: "Delete" }));
    await user.click(
      screen.getByRole("button", { name: "Delete Local Dev" }),
    );

    await waitFor(() =>
      expect(tauri.deleteEnvironment).toHaveBeenCalledWith("Local Dev"),
    );
    expect(screen.queryByRole("button", { name: /Local Dev/ }))
      .not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Staging/ }))
      .toHaveAttribute("aria-current", "true");
    expect(await screen.findByText("Deleted Local Dev")).toBeVisible();
    expect(window.localStorage.getItem(lastSelectedEnvironmentStorageKey)).toBe(
      "Staging",
    );
    expect(window.localStorage.getItem(topicPinStorageKey)).toBe(
      "{\"Staging\":[\"payments.authorized\"]}",
    );
    expect(window.localStorage.getItem(messagePreferencesStorageKey)).toBe(
      "{\"Staging\":{\"payments.authorized\":\"json\"}}",
    );
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();
    expect(tauri.startKafkaConsumerSession).not.toHaveBeenCalled();
  });

  it("shows backend delete warnings in chooser status", async () => {
    tauri.deleteEnvironment.mockResolvedValueOnce({
      name: "Local Dev",
      warning: "Deleted metadata, but Keychain cleanup needs manual review",
    });
    const { user } = renderAppChooser();

    await screen.findByRole("button", { name: /Local Dev/ });
    await user.click(screen.getByRole("button", { name: "Delete" }));
    await user.click(
      screen.getByRole("button", { name: "Delete Local Dev" }),
    );

    expect(
      await screen.findByText(
        "Deleted metadata, but Keychain cleanup needs manual review",
      ),
    ).toBeVisible();
    expect(screen.queryByRole("button", { name: /Local Dev/ }))
      .not.toBeInTheDocument();
  });

  it("keeps selected-environment connection test status local to the chooser", async () => {
    tauri.materializeRuntimeAuthConfig.mockRejectedValueOnce(
      new Error("connection secret missing"),
    );
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    expect(await screen.findByText("connection secret missing")).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Activity log")).not.toBeInTheDocument();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();
  });

  it("tests a saved environment without opening the workspace", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledOnce());
    expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledWith("Local Dev");
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(tauri.listKafkaTopics).toHaveBeenLastCalledWith({
      auth: expect.objectContaining({
        environment: "Local Dev",
        brokers: ["localhost:19092"],
      }),
    });
    expect(await screen.findByText("Connection OK: 5 topics")).toBeVisible();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(queryTopicSelect("orders.created")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledTimes(2));
    expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledTimes(2);
  });

  it("opens a tested saved environment without repeating the topic metadata fetch", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(tauri.listKafkaTopics).toHaveBeenCalledOnce());
    expect(await screen.findByText("Connection OK: 5 topics")).toBeVisible();

    tauri.listKafkaTopics.mockRejectedValueOnce(new Error("metadata timeout"));
    await user.click(screen.getByRole("button", { name: "Open" }));

    expect(await screen.findByLabelText("Milena workspace")).toBeVisible();
    expect(topicSelect("orders.created")).toBeVisible();
    expect(tauri.materializeRuntimeAuthConfig).toHaveBeenCalledTimes(2);
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(screen.queryByText("metadata timeout")).not.toBeInTheDocument();
  });

  it("tests current add form values through temporary runtime auth without saving", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    expect(await screen.findByText("Fix highlighted fields")).toBeVisible();
    expect(tauri.materializeTemporaryRuntimeAuthConfig).not.toHaveBeenCalled();
    expect(tauri.listKafkaTopics).not.toHaveBeenCalled();

    await user.type(screen.getByLabelText("Environment name"), "QA");
    await user.type(screen.getByLabelText("Kafka brokers"), "qa.kafka.internal:9094");
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslScramSha512");
    await user.type(screen.getByLabelText("Kafka username"), "qa-user");
    await user.type(screen.getByLabelText("Kafka password"), "qa-secret");
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "client.id=milena-qa",
    );
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() =>
      expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledOnce(),
    );
    expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledWith({
      name: "QA",
      brokers: ["qa.kafka.internal:9094"],
      authMode: "saslSslScramSha512",
      username: "qa-user",
      password: "qa-secret",
      advancedProperties: "client.id=milena-qa",
    });
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(await screen.findByText("Test passed: 5 topics")).toBeVisible();
    expect(tauri.saveEnvironment).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(queryTopicSelect("orders.created")).not.toBeInTheDocument();

    await user.clear(screen.getByLabelText("Kafka brokers"));

    expect(screen.queryByText("Test passed: 5 topics")).not.toBeInTheDocument();
    expect(screen.queryByText("Connection OK: 5 topics")).not.toBeInTheDocument();
  });

  it("previews and hides the environment form password", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslPlain");

    const password = screen.getByLabelText("Kafka password");
    await user.type(password, "milena-plain-secret");

    expect(password).toHaveAttribute("type", "password");
    await user.click(screen.getByRole("button", { name: "Show Kafka password" }));

    expect(password).toHaveAttribute("type", "text");
    expect(password).toHaveValue("milena-plain-secret");
    await user.click(screen.getByRole("button", { name: "Hide Kafka password" }));

    expect(password).toHaveAttribute("type", "password");
    expect(password).toHaveValue("milena-plain-secret");
  });

  it("tests local compose through SASL_SSL PLAIN credentials", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.type(screen.getByLabelText("Environment name"), "Local Compose");
    await user.type(screen.getByLabelText("Kafka brokers"), "localhost:19092");
    await user.selectOptions(screen.getByLabelText("Auth mode"), "saslSslPlain");
    await user.type(screen.getByLabelText("Kafka username"), "milena_plain");
    await user.type(screen.getByLabelText("Kafka password"), "milena-plain-secret");
    await user.type(
      screen.getByLabelText("Advanced Kafka properties"),
      "ssl.ca.location=docker/kafka/generated/ssl/ca.crt",
    );
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() =>
      expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledOnce(),
    );
    expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledWith({
      name: "Local Compose",
      brokers: ["localhost:19092"],
      authMode: "saslSslPlain",
      username: "milena_plain",
      password: "milena-plain-secret",
      advancedProperties: "ssl.ca.location=docker/kafka/generated/ssl/ca.crt",
    });
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(await screen.findByText("Test passed: 5 topics")).toBeVisible();
  });

  it("tests edit form values with a blank SCRAM password through the existing saved secret", async () => {
    const { user } = renderAppChooser();

    await screen.findByLabelText("Environment chooser");
    await user.click(screen.getByRole("button", { name: /Staging/ }));
    await user.click(screen.getByRole("button", { name: "Edit" }));
    await user.clear(screen.getByLabelText("Kafka brokers"));
    await user.type(screen.getByLabelText("Kafka brokers"), "staging.kafka.internal:9095");
    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() =>
      expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledOnce(),
    );
    expect(tauri.materializeTemporaryRuntimeAuthConfig).toHaveBeenCalledWith({
      name: "Staging",
      brokers: ["staging.kafka.internal:9095"],
      authMode: "saslSslScramSha512",
      username: "deploy",
      password: null,
      advancedProperties: "client.dns.lookup=use_all_dns_ips",
    });
    expect(tauri.listKafkaTopics).toHaveBeenCalledOnce();
    expect(await screen.findByText("Test passed: 5 topics")).toBeVisible();
    expect(tauri.saveEnvironment).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Milena workspace")).not.toBeInTheDocument();
    expect(queryTopicSelect("orders.created")).not.toBeInTheDocument();
  });
});
