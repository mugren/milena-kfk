import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent,
  type MutableRefObject,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from "react";
import {
  DockviewReact,
  type DockviewApi,
  type DockviewReadyEvent,
  type DockviewWillDropEvent,
  type IDockviewHeaderActionsProps,
  type IDockviewPanel,
  type IDockviewPanelProps,
} from "dockview-react";
import "dockview-react/dist/styles/dockview.css";
import {
  Edit3,
  Eye,
  EyeOff,
  Plus,
  RefreshCw,
  Save,
  Search,
  Trash2,
  ChevronDown,
  ChevronUp,
  Maximize2,
  Minimize2,
  Monitor,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
  Pin,
  PinOff,
  Play,
  SplitSquareHorizontal,
  SplitSquareVertical,
  Square,
  Sun,
  X,
} from "lucide-react";
import {
  AppState,
  RuntimeAuthConfig,
  deleteEnvironment,
  listEnvironments,
  listKafkaTopics,
  loadAppState,
  materializeRuntimeAuthConfig,
  materializeTemporaryRuntimeAuthConfig,
  publishKafkaRecord,
  saveEnvironment,
  startKafkaConsumerSession,
  stopKafkaConsumerSession,
  setAppAppearanceTheme,
  type MilenaCommandError,
  type MilenaBoundaryEvent,
  type KafkaTopicMetadata,
  type SavedEnvironment,
} from "./lib/tauri";
import {
  applyAppearancePreference,
  readAppearancePreference,
  writeAppearancePreference,
  type AppearancePreference,
} from "./lib/appearance";
import {
  appendBoundaryEventActivity,
  appendGlobalError,
  clearGlobalActivity,
  type GlobalActivityEntry,
  type GlobalActivitySource,
} from "./lib/activity";
import {
  closePanePollingSession,
  getPanePollingSessionId,
  startPanePollingSession,
  stopPanePollingSession,
} from "./lib/polling";
import {
  consumerRecordMatchesFilter,
  removeMessageRenderPreferencesForEnvironment,
  readMessageRenderPreferences,
  renderKafkaRecord,
  setTopicMessageRenderMode,
  topicMessageRenderMode,
  type MessageRenderMode,
  type MessageRenderPreferences,
  type MessageRenderPreferenceStore,
  type RenderedKafkaRecord,
} from "./lib/messages";
import {
  canSendPublisherRecord,
  createInitialPublisherState,
  formatPublisherPayload,
  getPublisherPaneState,
  producerAckLabel,
  sendPublisherRecord,
  setPublisherKey,
  setPublisherPayload,
  validatePublisherPayload,
  type PublisherPaneState,
  type PublisherState,
} from "./lib/publisher";
import {
  createInitialTopicRailState,
  environmentKey,
  markTopicLoadFailed,
  markTopicLoadStarted,
  markTopicLoadSucceeded,
  openTopicEnvironment,
  removePinnedTopicsForEnvironment,
  setTopicSearch,
  toggleTopicPin,
  visibleTopicRows,
  type TopicPinStore,
} from "./lib/topics";
import {
  canStartPaneSession,
  clearPaneActivity,
  createInitialWorkspaceState,
  expandGroup as expandWorkspaceGroupState,
  expandPane as expandWorkspacePaneState,
  getSelectedPane,
  isPaneEmpty,
  moveTabToAdjacentGroup,
  openTopicInWorkspace,
  restoreExpandedGroup,
  restoreExpandedPane,
  selectPane as selectWorkspacePane,
  selectTopicPreview,
  splitPane as splitWorkspacePane,
  type TabMoveDirection,
  type SplitDirection,
  type TopicOpenPlacement,
  type WorkspacePane,
  type WorkspaceState,
} from "./lib/workspace";
import {
  DOCKVIEW_TOPIC_PANEL_COMPONENT,
  createDockviewWorkspaceOptions,
  fromDockviewGroupId,
  fromDockviewPanelId,
  handleDockviewWillDrop,
  mapWorkspaceToDockview,
  reconcileDockviewSnapshot,
  snapshotDockviewApi,
  toDockviewPanelId,
  type DockviewReconcileResult,
  type DockviewWorkspaceSnapshot,
} from "./lib/dockviewWorkspace";
import {
  cancelOnboardingForm,
  cancelDeleteEnvironment,
  confirmDeleteEnvironment,
  createOnboardingState,
  getSelectedEnvironment,
  requestDeleteEnvironment,
  saveOnboardingForm,
  selectEnvironment,
  setOnboardingFormTestResult,
  setOnboardingFormField,
  startAddEnvironment,
  startEditEnvironment,
  validateOnboardingForm,
  type EnvironmentAuthMode,
  type OnboardingEnvironment,
  type OnboardingFormValues,
  type OnboardingState,
} from "./lib/onboarding";

type TopicMenuState = {
  topic: string;
  x: number;
  y: number;
} | null;

type TopicPreviewModel = {
  name: string;
  partitionCount: number | null;
  renderMode: MessageRenderMode;
};

type VisibleColumns = {
  topics: boolean;
  inspector: boolean;
};

type RailColumn = keyof VisibleColumns;

type RailWidths = Record<RailColumn, number>;

type RailResizeState = {
  column: RailColumn;
  pointerId: number;
  startX: number;
  startWidth: number;
};

const RAIL_WIDTH_LIMITS: Record<
  RailColumn,
  { min: number; max: number; default: number }
> = {
  topics: { min: 260, max: 480, default: 340 },
  inspector: { min: 240, max: 420, default: 280 },
};

const RAIL_RESIZE_STEP = 16;
const RAIL_RESIZE_LARGE_STEP = 40;

type DockviewPlacementHint = {
  tabId: number;
  direction: "right" | "below";
} | null;

type DockviewPanelParams = {
  pane: WorkspacePane;
  active: boolean;
  environmentKey: string;
  isExpanded: boolean;
  publisher: PublisherPaneState;
  renderPreferences: MessageRenderPreferences;
  onActivate: () => void;
  onPoll: () => void;
  onPayloadChange: (payload: string) => void;
  onKeyChange: (key: string) => void;
  onFormatPayload: () => void;
  onSend: () => void;
  onRenderModeChange: (topic: string, mode: MessageRenderMode) => void;
  onStop: () => void;
  onClearMessages: () => void;
  onClose: () => void;
  showWorkspaceActions?: boolean;
};

const APPEARANCE_OPTIONS: {
  value: AppearancePreference;
  label: string;
  ariaLabel: string;
  icon: typeof Monitor;
}[] = [
  {
    value: "system",
    label: "System",
    ariaLabel: "Use system appearance",
    icon: Monitor,
  },
  {
    value: "light",
    label: "Light",
    ariaLabel: "Use light appearance",
    icon: Sun,
  },
  {
    value: "dark",
    label: "Dark",
    ariaLabel: "Use dark appearance",
    icon: Moon,
  },
];

type TestedTopicList = {
  environmentSignature: string;
  topics: KafkaTopicMetadata[];
};

const APP_SHELL_NAME = "Milena - Kafka Reader";
const LAST_SELECTED_ENVIRONMENT_KEY = "milena.lastSelectedEnvironment.v1";

const localDevRuntimeAuth: RuntimeAuthConfig = {
  environment: "local-dev",
  brokers: ["localhost:19092"],
  properties: {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "milena_plain",
    "sasl.password": "milena-plain-secret",
    "ssl.ca.location": "docker/kafka/generated/ssl/ca.crt",
    "ssl.endpoint.identification.algorithm": "https",
  },
};

function useAppliedAppearancePreference(
  appearancePreference: AppearancePreference,
  enabled: boolean,
) {
  useEffect(() => {
    if (!enabled) {
      return undefined;
    }

    applyAppearancePreference(appearancePreference);
    void setAppAppearanceTheme(appearancePreference);

    if (appearancePreference !== "system") {
      return undefined;
    }

    const systemScheme = window.matchMedia?.("(prefers-color-scheme: dark)");
    if (!systemScheme) {
      return undefined;
    }

    function applySystemAppearance() {
      applyAppearancePreference("system");
    }

    systemScheme.addEventListener("change", applySystemAppearance);
    return () => {
      systemScheme.removeEventListener("change", applySystemAppearance);
    };
  }, [appearancePreference, enabled]);
}

function App() {
  const [appearancePreference, setAppearancePreference] =
    useState(readAppearancePreference);
  const [activeRuntimeAuth, setActiveRuntimeAuth] =
    useState<RuntimeAuthConfig | null>(null);
  const [initialWorkspaceTopics, setInitialWorkspaceTopics] =
    useState<KafkaTopicMetadata[] | null>(null);
  const [testedTopicList, setTestedTopicList] = useState<TestedTopicList | null>(
    null,
  );
  const [onboarding, setOnboarding] = useState<OnboardingState | null>(null);
  const [chooserStatus, setChooserStatus] = useState<ChooserStatus>({
    tone: "loading",
    message: "Loading environments",
  });

  useEffect(() => {
    document.title = APP_SHELL_NAME;
  }, []);

  useAppliedAppearancePreference(appearancePreference, true);

  useEffect(() => {
    void refreshEnvironments("quiet");
  }, []);

  async function refreshEnvironments(mode: "quiet" | "manual" = "manual") {
    setChooserStatus({
      tone: "loading",
      message: mode === "manual" ? "Refreshing environments" : "Loading environments",
    });

    try {
      const response = await listEnvironments();
      const environments = response.environments.map(toOnboardingEnvironment);
      const state = createOnboardingState({
        environments,
        lastSelectedEnvironmentName: readLastSelectedEnvironmentName(),
      });
      setOnboarding(state);
      if (state.selectedEnvironmentName) {
        rememberLastSelectedEnvironment(state.selectedEnvironmentName);
      }
      setChooserStatus({
        tone: "idle",
        message:
          environments.length === 0
            ? "No saved environments"
            : `${environments.length} saved environments`,
      });
    } catch (cause) {
      const message = errorMessage(cause, "Saved environments unavailable");
      setOnboarding(createOnboardingState({ environments: [] }));
      setChooserStatus({
        tone: "error",
        message,
      });
    }
  }

  function updateOnboarding(update: (current: OnboardingState) => OnboardingState) {
    setOnboarding((current) => (current ? update(current) : current));
  }

  function changeAppearancePreference(preference: AppearancePreference) {
    writeAppearancePreference(preference);
    setAppearancePreference(preference);
  }

  function chooseEnvironment(environmentName: string) {
    updateOnboarding((current) => {
      const next = selectEnvironment(current, environmentName);
      if (next.selectedEnvironmentName) {
        rememberLastSelectedEnvironment(next.selectedEnvironmentName);
      }
      return next;
    });
    setChooserStatus({ tone: "idle", message: `${environmentName} selected` });
  }

  function startAdd() {
    updateOnboarding(startAddEnvironment);
    setChooserStatus({ tone: "idle", message: "New environment" });
  }

  function startEdit() {
    updateOnboarding(startEditEnvironment);
    setChooserStatus({ tone: "idle", message: "Editing environment" });
  }

  function cancelForm() {
    updateOnboarding(cancelOnboardingForm);
    setChooserStatus({ tone: "idle", message: "Chooser ready" });
  }

  function requestDelete() {
    if (!onboarding) {
      return;
    }

    const selectedEnvironment = getSelectedEnvironment(onboarding);
    if (!selectedEnvironment) {
      return;
    }

    setOnboarding(requestDeleteEnvironment(onboarding, selectedEnvironment.name));
    setChooserStatus({
      tone: "idle",
      message: `Confirm delete ${selectedEnvironment.name}`,
    });
  }

  function cancelDelete() {
    updateOnboarding(cancelDeleteEnvironment);
    setChooserStatus({ tone: "idle", message: "Delete cancelled" });
  }

  async function confirmDelete() {
    const environmentName = onboarding?.deleteConfirmation?.environmentName;
    if (!environmentName) {
      return;
    }

    setChooserStatus({ tone: "loading", message: `Deleting ${environmentName}` });
    try {
      const response = await deleteEnvironment(environmentName);
      removePinnedTopicsForEnvironment(environmentName, getTopicPinStore());
      removeMessageRenderPreferencesForEnvironment(
        environmentName,
        getMessageRenderPreferenceStore(),
      );
      setOnboarding((current) => {
        if (!current) {
          return current;
        }

        const next = confirmDeleteEnvironment(current);
        if (next.selectedEnvironmentName) {
          rememberLastSelectedEnvironment(next.selectedEnvironmentName);
        } else {
          forgetLastSelectedEnvironment();
        }
        return next;
      });
      setChooserStatus({
        tone: response.warning ? "warning" : "success",
        message: response.warning ?? `Deleted ${environmentName}`,
      });
    } catch (cause) {
      const message = errorMessage(cause, "Delete failed");
      setChooserStatus({ tone: "error", message });
    }
  }

  function setFormField<K extends keyof OnboardingFormValues>(
    field: K,
    value: OnboardingFormValues[K],
  ) {
    if (onboarding?.form?.testResult) {
      setChooserStatus({
        tone: "idle",
        message: onboarding.form.mode === "add" ? "New environment" : "Editing environment",
      });
    }
    updateOnboarding((current) => setOnboardingFormField(current, field, value));
  }

  async function testSelectedEnvironment() {
    const selectedEnvironment = onboarding ? getSelectedEnvironment(onboarding) : null;
    if (!selectedEnvironment) {
      return;
    }

    setChooserStatus({ tone: "loading", message: "Testing connection" });
    try {
      const auth = await materializeRuntimeAuthConfig(selectedEnvironment.name);
      const topicList = await listKafkaTopics({ auth });
      setTestedTopicList({
        environmentSignature: onboardingEnvironmentSignature(selectedEnvironment),
        topics: topicList.topics,
      });
      setChooserStatus({
        tone: "success",
        message: connectionSuccessMessage(topicList.topics.length),
      });
    } catch (cause) {
      const message = errorMessage(cause, "Connection test failed");
      setChooserStatus({ tone: "error", message });
    }
  }

  async function openSelectedEnvironment() {
    const selectedEnvironment = onboarding ? getSelectedEnvironment(onboarding) : null;
    if (!selectedEnvironment) {
      return;
    }

    setChooserStatus({
      tone: "loading",
      message: `Opening ${selectedEnvironment.name}`,
    });
    try {
      const auth = await materializeRuntimeAuthConfig(selectedEnvironment.name);
      rememberLastSelectedEnvironment(selectedEnvironment.name);
      const testedTopics =
        testedTopicList?.environmentSignature ===
        onboardingEnvironmentSignature(selectedEnvironment)
          ? testedTopicList.topics
          : null;
      setInitialWorkspaceTopics(testedTopics);
      setActiveRuntimeAuth(auth);
    } catch (cause) {
      const message = errorMessage(cause, "Environment open failed");
      setInitialWorkspaceTopics(null);
      setChooserStatus({ tone: "error", message });
    }
  }

  function changeEnvironment(environmentName: string) {
    setActiveRuntimeAuth(null);
    updateOnboarding((current) => {
      const next = selectEnvironment(current, environmentName);
      if (next.selectedEnvironmentName) {
        rememberLastSelectedEnvironment(next.selectedEnvironmentName);
      }
      return next;
    });
    setInitialWorkspaceTopics(null);
    setChooserStatus({ tone: "idle", message: `${environmentName} selected` });
  }

  async function testFormConnection() {
    if (!onboarding?.form) {
      return;
    }

    const currentForm = onboarding.form;
    const testSignature = onboardingFormSignature(currentForm);
    const validation = validateOnboardingForm(currentForm, onboarding.environments);
    if (!validation.ok) {
      setOnboarding((current) => {
        if (!current?.form) {
          return current;
        }
        return {
          ...current,
          form: {
            ...current.form,
            errors: validation.errors,
            testResult: null,
          },
        };
      });
      setChooserStatus({ tone: "error", message: "Fix highlighted fields" });
      return;
    }

    setChooserStatus({ tone: "loading", message: "Testing connection" });
    try {
      const environment = validation.environment;
      const auth = await materializeTemporaryRuntimeAuthConfig({
        name: environment.name,
        brokers: environment.brokers,
        authMode: environment.authMode,
        username:
          isSaslAuthMode(environment.authMode)
            ? environment.username
            : null,
        password:
          isSaslAuthMode(environment.authMode)
            ? currentForm.values.password || null
            : null,
        advancedProperties: environment.advancedPropertiesText,
      });
      const topicList = await listKafkaTopics({ auth });
      const message = `Test passed: ${pluralizeTopics(topicList.topics.length)}`;
      setTestedTopicList({
        environmentSignature: onboardingEnvironmentSignature(environment),
        topics: topicList.topics,
      });
      setOnboarding((current) => {
        if (!current?.form || onboardingFormSignature(current.form) !== testSignature) {
          return current;
        }
        return setOnboardingFormTestResult(current, {
          status: "success",
          message,
        });
      });
      setChooserStatus({
        tone: "success",
        message: connectionSuccessMessage(topicList.topics.length),
      });
    } catch (cause) {
      const message = errorMessage(cause, "Connection test failed");
      setOnboarding((current) => {
        if (!current?.form || onboardingFormSignature(current.form) !== testSignature) {
          return current;
        }
        return setOnboardingFormTestResult(current, {
          status: "error",
          message,
        });
      });
      setChooserStatus({ tone: "error", message });
    }
  }

  async function saveForm() {
    if (!onboarding?.form) {
      return;
    }

    const currentForm = onboarding.form;
    const savedLocally = saveOnboardingForm(onboarding);
    if (!savedLocally.ok) {
      setOnboarding(savedLocally.state);
      setChooserStatus({ tone: "error", message: "Fix highlighted fields" });
      return;
    }

    setChooserStatus({ tone: "loading", message: "Saving environment" });
    try {
      const saved = await saveEnvironment({
        name: savedLocally.environment.name,
        brokers: savedLocally.environment.brokers,
        authMode: savedLocally.environment.authMode,
        username:
          isSaslAuthMode(savedLocally.environment.authMode)
            ? savedLocally.environment.username
            : null,
        password:
          isSaslAuthMode(savedLocally.environment.authMode)
            ? currentForm.values.password || null
            : null,
        advancedProperties: savedLocally.environment.advancedPropertiesText,
      });
      const savedEnvironment = toOnboardingEnvironment(saved);
      setOnboarding((current) => {
        if (!current) {
          return current;
        }

        const environments =
          current.environments.some(
            (environment) => environment.name === savedEnvironment.name,
          )
            ? current.environments.map((environment) =>
                environment.name === savedEnvironment.name
                  ? savedEnvironment
                  : environment,
              )
            : [...current.environments, savedEnvironment];
        const next = createOnboardingState({
          environments,
          lastSelectedEnvironmentName: savedEnvironment.name,
        });
        rememberLastSelectedEnvironment(savedEnvironment.name);
        return next;
      });
      setChooserStatus({
        tone: "success",
        message: `Saved ${savedEnvironment.name}`,
      });
    } catch (cause) {
      const message = errorMessage(cause, "Save failed");
      setChooserStatus({ tone: "error", message });
      setOnboarding(onboarding);
    }
  }

  if (activeRuntimeAuth) {
    return (
      <WorkspaceShell
        activeRuntimeAuth={activeRuntimeAuth}
        autoLoadTopics={initialWorkspaceTopics === null}
        appearancePreference={appearancePreference}
        initialTopics={initialWorkspaceTopics}
        onAppearancePreferenceChange={changeAppearancePreference}
        onChangeEnvironment={changeEnvironment}
      />
    );
  }

  return (
    <EnvironmentChooser
      onboarding={onboarding}
      appearancePreference={appearancePreference}
      status={chooserStatus}
      onAdd={startAdd}
      onAppearancePreferenceChange={changeAppearancePreference}
      onCancel={cancelForm}
      onCancelDelete={cancelDelete}
      onConfirmDelete={() => void confirmDelete()}
      onEdit={startEdit}
      onFieldChange={setFormField}
      onRefresh={() => void refreshEnvironments("manual")}
      onRequestDelete={requestDelete}
      onSave={() => void saveForm()}
      onSelect={chooseEnvironment}
      onOpen={() => void openSelectedEnvironment()}
      onTestForm={() => void testFormConnection()}
      onTestSelected={() => void testSelectedEnvironment()}
    />
  );
}

type ChooserStatus = {
  tone: "idle" | "loading" | "success" | "warning" | "error";
  message: string;
};

function EnvironmentChooser({
  onboarding,
  appearancePreference,
  status,
  onAdd,
  onAppearancePreferenceChange,
  onCancel,
  onCancelDelete,
  onConfirmDelete,
  onEdit,
  onFieldChange,
  onOpen,
  onRefresh,
  onRequestDelete,
  onSave,
  onSelect,
  onTestForm,
  onTestSelected,
}: {
  onboarding: OnboardingState | null;
  appearancePreference: AppearancePreference;
  status: ChooserStatus;
  onAdd: () => void;
  onAppearancePreferenceChange: (preference: AppearancePreference) => void;
  onCancel: () => void;
  onCancelDelete: () => void;
  onConfirmDelete: () => void;
  onEdit: () => void;
  onFieldChange: <K extends keyof OnboardingFormValues>(
    field: K,
    value: OnboardingFormValues[K],
  ) => void;
  onOpen: () => void;
  onRefresh: () => void;
  onRequestDelete: () => void;
  onSave: () => void;
  onSelect: (environmentName: string) => void;
  onTestForm: () => void;
  onTestSelected: () => void;
}) {
  const selectedEnvironment = onboarding ? getSelectedEnvironment(onboarding) : null;
  const deleteConfirmation = onboarding?.deleteConfirmation ?? null;
  const form = onboarding?.form ?? null;
  const environments = onboarding?.environments ?? [];
  const busy = status.tone === "loading";

  useEffect(() => {
    function handleChooserKeyDown(event: KeyboardEvent) {
      if (
        event.defaultPrevented ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey
      ) {
        return;
      }

      if (event.key === "Escape") {
        if (deleteConfirmation) {
          event.preventDefault();
          onCancelDelete();
          return;
        }
        if (form) {
          event.preventDefault();
          onCancel();
        }
        return;
      }

      if (
        event.key !== "Enter" ||
        busy ||
        !selectedEnvironment ||
        form ||
        deleteConfirmation ||
        onboarding?.mode !== "list" ||
        isEditableShortcutTarget(event.target)
      ) {
        return;
      }

      event.preventDefault();
      onOpen();
    }

    window.addEventListener("keydown", handleChooserKeyDown);
    return () => window.removeEventListener("keydown", handleChooserKeyDown);
  }, [
    busy,
    deleteConfirmation,
    form,
    onboarding?.mode,
    onCancel,
    onCancelDelete,
    onOpen,
    selectedEnvironment,
  ]);

  return (
    <main className="chooser-shell" aria-label="Environment chooser">
      <section className="chooser-panel">
        <header className="chooser-header">
          <div className="rail-title">
            <span className="eyebrow">Milena</span>
            <strong>Environments</strong>
            <small>Saved Kafka connection profiles</small>
          </div>
          <div className="chooser-actions">
            <AppearanceControl
              value={appearancePreference}
              onChange={onAppearancePreferenceChange}
            />
            <span className={`status-pill ${status.tone}`}>{status.message}</span>
            <button
              className="rail-toggle-button"
              type="button"
              aria-label="Refresh environments"
              title="Refresh environments"
              disabled={status.tone === "loading"}
              onClick={onRefresh}
            >
              <RefreshCw aria-hidden="true" size={15} strokeWidth={1.9} />
            </button>
            <button
              className="secondary compact"
              type="button"
              onClick={onAdd}
            >
              <Plus aria-hidden="true" size={14} strokeWidth={2} />
              <span>Add</span>
            </button>
          </div>
        </header>

        <div className="chooser-grid">
          <section className="environment-list" aria-label="Saved environments">
            {environments.length === 0 ? (
              <div className="environment-empty">
                <strong>No saved environments</strong>
                <span>Add a profile to make it available in the chooser.</span>
              </div>
            ) : (
              environments.map((environment) => {
                const selected = environment.name === onboarding?.selectedEnvironmentName;
                const remembered =
                  environment.name === onboarding?.lastSelectedEnvironmentName;
                return (
                  <button
                    className={[
                      "environment-row",
                      selected ? "active" : "",
                      remembered ? "remembered" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    aria-current={selected ? "true" : undefined}
                    key={environment.name}
                    onClick={() => onSelect(environment.name)}
                  >
                    <span className="environment-marker" />
                    <span>
                      <strong>{environment.name}</strong>
                      <small>{environment.brokers.join(", ")}</small>
                      {environment.username ? (
                        <small>{environment.username}</small>
                      ) : null}
                    </span>
                    <span className="status-pill">
                      {authModeLabel(environment.authMode)}
                    </span>
                  </button>
                );
              })
            )}
          </section>

          <section className="environment-detail" aria-label="Environment details">
            {form ? (
              <EnvironmentForm
                form={form}
                onCancel={onCancel}
                onFieldChange={onFieldChange}
                onSave={onSave}
                onTest={onTestForm}
                saving={busy}
              />
            ) : deleteConfirmation ? (
              <EnvironmentDeleteConfirmation
                environmentName={deleteConfirmation.environmentName}
                busy={busy}
                onCancel={onCancelDelete}
                onConfirm={onConfirmDelete}
              />
            ) : selectedEnvironment ? (
              <EnvironmentSummary
                environment={selectedEnvironment}
                onEdit={onEdit}
                onOpen={onOpen}
                onRequestDelete={onRequestDelete}
                onTest={onTestSelected}
                busy={busy}
              />
            ) : (
              <div className="environment-empty detail-empty">
                <strong>Chooser ready</strong>
                <span>Select an environment or add a new one.</span>
              </div>
            )}
          </section>
        </div>
      </section>
    </main>
  );
}

function EnvironmentSummary({
  environment,
  busy,
  onEdit,
  onOpen,
  onRequestDelete,
  onTest,
}: {
  environment: OnboardingEnvironment;
  busy: boolean;
  onEdit: () => void;
  onOpen: () => void;
  onRequestDelete: () => void;
  onTest: () => void;
}) {
  return (
    <div className="environment-summary-panel">
      <header>
        <div className="rail-title">
          <span className="eyebrow">Selected</span>
          <strong>{environment.name}</strong>
          <small>{authModeLabel(environment.authMode)}</small>
        </div>
        <button className="secondary compact" type="button" onClick={onEdit}>
          <Edit3 aria-hidden="true" size={14} strokeWidth={2} />
          <span>Edit</span>
        </button>
      </header>
      <dl className="environment-facts">
        <div>
          <dt>Brokers</dt>
          <dd>{environment.brokers.join(", ")}</dd>
        </div>
        <div>
          <dt>Username</dt>
          <dd>{environment.username || "none"}</dd>
        </div>
        <div>
          <dt>Advanced properties</dt>
          <dd>{environment.advancedPropertiesText || "none"}</dd>
        </div>
      </dl>
      <div className="environment-placeholder-actions">
        <button type="button" className="primary" onClick={onOpen} disabled={busy}>
          Open
        </button>
        <button type="button" onClick={onTest} disabled={busy}>
          Test connection
        </button>
        <button type="button" className="danger" onClick={onRequestDelete} disabled={busy}>
          <Trash2 aria-hidden="true" size={14} strokeWidth={2} />
          <span>Delete</span>
        </button>
      </div>
    </div>
  );
}

function EnvironmentDeleteConfirmation({
  environmentName,
  busy,
  onCancel,
  onConfirm,
}: {
  environmentName: string;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div className="environment-summary-panel environment-delete-confirmation">
      <header>
        <div className="rail-title">
          <span className="eyebrow">Confirm delete</span>
          <strong>{`Delete environment "${environmentName}"?`}</strong>
          <small>Secrets and saved metadata will be removed.</small>
        </div>
      </header>
      <div className="environment-delete-body">
        <dl className="environment-facts">
          <div>
            <dt>Environment</dt>
            <dd>{environmentName}</dd>
          </div>
        </dl>
      </div>
      <div className="environment-placeholder-actions">
        <button type="button" onClick={onCancel} disabled={busy}>
          Cancel delete
        </button>
        <button type="button" className="danger primary" onClick={onConfirm} disabled={busy}>
          <Trash2 aria-hidden="true" size={14} strokeWidth={2} />
          <span>{`Delete ${environmentName}`}</span>
        </button>
      </div>
    </div>
  );
}

function EnvironmentForm({
  form,
  onCancel,
  onFieldChange,
  onSave,
  onTest,
  saving,
}: {
  form: NonNullable<OnboardingState["form"]>;
  onCancel: () => void;
  onFieldChange: <K extends keyof OnboardingFormValues>(
    field: K,
    value: OnboardingFormValues[K],
  ) => void;
  onSave: () => void;
  onTest: () => void;
  saving: boolean;
}) {
  const usesCredentials = isSaslAuthMode(form.values.authMode);
  const [passwordVisible, setPasswordVisible] = useState(false);

  return (
    <form
      className="environment-form"
      aria-label={`${form.mode === "add" ? "Add" : "Edit"} environment`}
      onSubmit={(event) => {
        event.preventDefault();
        onSave();
      }}
    >
      <header>
        <div className="rail-title">
          <span className="eyebrow">{form.mode === "add" ? "Add" : "Edit"}</span>
          <strong>{form.mode === "add" ? "New environment" : form.values.name}</strong>
          <small>Save keeps you in the chooser.</small>
        </div>
        <div className="chooser-actions">
          <button type="button" onClick={onTest} disabled={saving}>
            Test connection
          </button>
          <button type="button" onClick={onCancel}>
            Cancel
          </button>
          <button className="primary" type="submit" disabled={saving}>
            <Save aria-hidden="true" size={14} strokeWidth={2} />
            <span>Save</span>
          </button>
        </div>
      </header>

      <div className="environment-fields">
        <FieldError error={form.errors.name}>
          <label>
            <span>Name</span>
            <input
              aria-label="Environment name"
              type="text"
              spellCheck={false}
              autoCapitalize="none"
              autoCorrect="off"
              value={form.values.name}
              disabled={form.mode === "edit"}
              onChange={(event) => onFieldChange("name", event.currentTarget.value)}
            />
          </label>
        </FieldError>

        <FieldError error={form.errors.brokersText}>
          <label>
            <span>Brokers</span>
            <textarea
              aria-label="Kafka brokers"
              spellCheck={false}
              autoCapitalize="none"
              autoCorrect="off"
              value={form.values.brokersText}
              onChange={(event) =>
                onFieldChange("brokersText", event.currentTarget.value)
              }
            />
          </label>
        </FieldError>

        <label>
          <span>Auth mode</span>
          <select
            aria-label="Auth mode"
            value={form.values.authMode}
            onChange={(event) =>
              onFieldChange("authMode", event.currentTarget.value as EnvironmentAuthMode)
            }
          >
            <option value="plaintext">PLAINTEXT (no auth)</option>
            <option value="saslSslPlain">SASL_SSL PLAIN</option>
            <option value="saslSslScramSha512">SASL_SSL SCRAM-SHA-512</option>
          </select>
        </label>

        {usesCredentials ? (
          <div className="credential-grid">
            <FieldError error={form.errors.username}>
              <label>
                <span>Username</span>
                <input
                  aria-label="Kafka username"
                  type="text"
                  spellCheck={false}
                  autoCapitalize="none"
                  autoCorrect="off"
                  value={form.values.username}
                  onChange={(event) =>
                    onFieldChange("username", event.currentTarget.value)
                  }
                />
              </label>
            </FieldError>
            <FieldError error={form.errors.password}>
              <label>
                <span>Password</span>
                <div className="password-field">
                  <input
                    aria-label="Kafka password"
                    type={passwordVisible ? "text" : "password"}
                    spellCheck={false}
                    autoCapitalize="none"
                    autoCorrect="off"
                    value={form.values.password}
                    placeholder={form.mode === "edit" ? "Leave blank to keep" : ""}
                    onChange={(event) =>
                      onFieldChange("password", event.currentTarget.value)
                    }
                  />
                  <button
                    type="button"
                    className="icon-button password-preview-toggle"
                    aria-label={passwordVisible ? "Hide Kafka password" : "Show Kafka password"}
                    title={passwordVisible ? "Hide password" : "Show password"}
                    onClick={() => setPasswordVisible((visible) => !visible)}
                  >
                    {passwordVisible ? (
                      <EyeOff aria-hidden="true" size={14} strokeWidth={2} />
                    ) : (
                      <Eye aria-hidden="true" size={14} strokeWidth={2} />
                    )}
                  </button>
                </div>
              </label>
            </FieldError>
          </div>
        ) : null}

        <label>
          <span>Advanced properties</span>
          <textarea
            aria-label="Advanced Kafka properties"
            className="advanced-properties"
            spellCheck={false}
            autoCapitalize="none"
            autoCorrect="off"
            value={form.values.advancedPropertiesText}
            onChange={(event) =>
              onFieldChange("advancedPropertiesText", event.currentTarget.value)
            }
          />
        </label>
      </div>
      {form.testResult ? (
        <div
          className={`environment-test-result ${form.testResult.status}`}
          role="status"
        >
          {form.testResult.message}
        </div>
      ) : null}
    </form>
  );
}

function FieldError({
  children,
  error,
}: {
  children: ReactNode;
  error: string | undefined;
}) {
  return (
    <div className={error ? "field has-error" : "field"}>
      {children}
      {error ? <small>{error}</small> : null}
    </div>
  );
}

export function WorkspaceShell({
  activeRuntimeAuth = localDevRuntimeAuth,
  appearancePreference,
  autoLoadTopics = false,
  initialTopics = null,
  onAppearancePreferenceChange,
  onChangeEnvironment,
}: {
  activeRuntimeAuth?: RuntimeAuthConfig;
  appearancePreference?: AppearancePreference;
  autoLoadTopics?: boolean;
  initialTopics?: KafkaTopicMetadata[] | null;
  onAppearancePreferenceChange?: (preference: AppearancePreference) => void;
  onChangeEnvironment?: (environmentName: string) => void;
}) {
  const [localAppearancePreference, setLocalAppearancePreference] =
    useState(readAppearancePreference);
  const [appState, setAppState] = useState<AppState | null>(null);
  const [workspace, setWorkspace] = useState(createInitialWorkspaceState);
  const [topicRail, setTopicRail] = useState(() =>
    initialTopics === null
      ? createInitialTopicRailState(getTopicPinStore())
      : markTopicLoadSucceeded(
          createInitialTopicRailState(getTopicPinStore()),
          environmentKey(activeRuntimeAuth),
          initialTopics,
        ),
  );
  const [messageRenderPreferences, setMessageRenderPreferences] = useState(() =>
    readMessageRenderPreferences(getMessageRenderPreferenceStore()),
  );
  const [publisherState, setPublisherState] = useState(
    createInitialPublisherState,
  );
  const [activity, setActivity] = useState<GlobalActivityEntry[]>([]);
  const [topicMenu, setTopicMenu] = useState<TopicMenuState>(null);
  const [visibleColumns, setVisibleColumns] = useState<VisibleColumns>({
    topics: true,
    inspector: true,
  });
  const [railWidths, setRailWidths] = useState<RailWidths>(() => ({
    topics: RAIL_WIDTH_LIMITS.topics.default,
    inspector: RAIL_WIDTH_LIMITS.inspector.default,
  }));
  const requestedTopicLoads = useRef(new Set<string>());
  const workspaceRef = useRef(workspace);
  const workspaceGeneration = useRef(0);
  const pollingRuns = useRef(new Map<number, number>());
  const dockviewPlacementHint = useRef<DockviewPlacementHint>(null);
  const railResize = useRef<RailResizeState | null>(null);
  const currentAppearancePreference =
    appearancePreference ?? localAppearancePreference;
  const shellStyle = {
    "--topics-rail-width": `${railWidths.topics}px`,
    "--inspector-rail-width": `${railWidths.inspector}px`,
  } as CSSProperties;

  useEffect(() => {
    document.title = APP_SHELL_NAME;
  }, []);

  useAppliedAppearancePreference(
    currentAppearancePreference,
    appearancePreference === undefined,
  );

  useEffect(() => {
    loadAppState()
      .then(setAppState)
      .catch((cause: unknown) => {
        const message = errorMessage(cause, "Tauri runtime unavailable");
        recordGlobalError("app", "Tauri runtime unavailable", message);
      });
  }, []);

  useEffect(() => {
    workspaceRef.current = workspace;
  }, [workspace]);

  useEffect(() => {
    if (!topicMenu) {
      return;
    }

    function closeTopicMenu() {
      setTopicMenu(null);
    }

    window.addEventListener("click", closeTopicMenu);
    window.addEventListener("keydown", closeTopicMenu);
    return () => {
      window.removeEventListener("click", closeTopicMenu);
      window.removeEventListener("keydown", closeTopicMenu);
    };
  }, [topicMenu]);

  const activePane = useMemo(
    () => getSelectedPane(workspace),
    [workspace],
  );
  const activeTopic = activePane?.topic ?? null;
  const previewTopic = workspace.selectedTopic ?? activeTopic;
  const activeEnvironmentKey = environmentKey(activeRuntimeAuth);
  const topicRows = useMemo(() => visibleTopicRows(topicRail), [topicRail]);
  const topicPreview = useMemo<TopicPreviewModel | null>(() => {
    if (!workspace.selectedTopic || workspace.selectedTopic === activePane?.topic) {
      return null;
    }

    const metadata = topicRail.topics.find(
      (topic) => topic.name === workspace.selectedTopic,
    );

    return {
      name: workspace.selectedTopic,
      partitionCount: metadata?.partitionCount ?? null,
      renderMode: topicMessageRenderMode(
        messageRenderPreferences,
        activeEnvironmentKey,
        workspace.selectedTopic,
      ),
    };
  }, [
    activeEnvironmentKey,
    activePane?.topic,
    messageRenderPreferences,
    topicRail.topics,
    workspace.selectedTopic,
  ]);

  function selectTopic(topic: string) {
    updateWorkspace((current) => selectTopicPreview(current, topic));
  }

  async function loadTopicList(force: boolean) {
    const key = environmentKey(activeRuntimeAuth);
    if (!force && requestedTopicLoads.current.has(key)) {
      setTopicRail((current) => openTopicEnvironment(current, key));
      return;
    }

    requestedTopicLoads.current.add(key);
    setTopicRail((current) => markTopicLoadStarted(current, key));

    try {
      const topicList = await listKafkaTopics({ auth: activeRuntimeAuth });
      setTopicRail((current) =>
        markTopicLoadSucceeded(current, key, topicList.topics),
      );
    } catch (cause) {
      const message = errorMessage(cause, "Topic list refresh failed");
      setTopicRail((current) => markTopicLoadFailed(current, key, message));
      recordGlobalError("topics", "Topic list refresh failed", message);
    }
  }

  useEffect(() => {
    if (!autoLoadTopics) {
      return;
    }

    void loadTopicList(true);
  }, [autoLoadTopics, activeRuntimeAuth]);

  function refreshTopicList() {
    void loadTopicList(true);
  }

  function pinTopic(topic: string) {
    setTopicRail((current) =>
      toggleTopicPin(current, activeEnvironmentKey, topic, getTopicPinStore()),
    );
  }

  function setMessageRenderMode(topic: string, mode: MessageRenderMode) {
    setMessageRenderPreferences((current) =>
      setTopicMessageRenderMode(
        current,
        activeEnvironmentKey,
        topic,
        mode,
        getMessageRenderPreferenceStore(),
      ),
    );
  }

  function startPolling(paneId: number, topic: string) {
    const generation = workspaceGeneration.current;
    const run = nextPollingRun(paneId);
    setTopicMenu(null);
    return startPanePollingSession({
      paneId,
      topic,
      auth: activeRuntimeAuth,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      startConsumerSession: startKafkaConsumerSession,
      stopConsumerSession: stopKafkaConsumerSession,
      isCurrent: () =>
        workspaceGeneration.current === generation &&
        pollingRuns.current.get(paneId) === run,
      onEvent: (event) => {
        setActivity((current) =>
          appendBoundaryEventActivity(current, event, paneId),
        );
      },
      onError: (message) =>
        recordGlobalError("consumer", "Consumer session failed", message, paneId),
      onStopError: (message) => {
        if (workspaceGeneration.current === generation) {
          recordGlobalError("consumer", "Consumer cleanup failed", message, paneId);
        }
      },
    });
  }

  function moveTab(tabId: number, direction: TabMoveDirection) {
    const current = workspaceRef.current;
    const result = moveTabToAdjacentGroup(current, tabId, direction);
    if (result.status === "moved") {
      dockviewPlacementHint.current = {
        tabId,
        direction: direction === "bottom" ? "below" : "right",
      };
      workspaceRef.current = result.workspace;
      setWorkspace(result.workspace);
      return;
    }

    if (result.status === "tab-limit") {
      recordGlobalError(
        "topics",
        "Max tabs opened in group",
        "Max tabs opened in group",
      );
    } else if (result.status === "group-limit") {
      recordGlobalError(
        "topics",
        "Group limit reached",
        "Milena supports up to four open groups.",
      );
    }
  }

  function splitPane(paneId: number, direction: SplitDirection) {
    if (direction === "top") {
      updateWorkspace((current) => splitWorkspacePane(current, paneId, direction));
      return;
    }

    moveTab(paneId, direction);
  }

  function expandGroup(groupId: number) {
    updateWorkspace((current) => expandWorkspaceGroupState(current, groupId));
  }

  function expandPane(paneId: number) {
    updateWorkspace((current) => expandWorkspacePaneState(current, paneId));
  }

  function restoreGroup() {
    updateWorkspace(restoreExpandedGroup);
  }

  function restorePane() {
    updateWorkspace(restoreExpandedPane);
  }

  function openTopic(topic: string, placement: TopicOpenPlacement) {
    setTopicMenu(null);
    const current = workspaceRef.current;
    const result = openTopicInWorkspace(current, topic, placement);
    if (
      result.status === "opened" &&
      (placement === "right" || placement === "bottom")
    ) {
      dockviewPlacementHint.current = {
        tabId: result.workspace.selectedPaneId,
        direction: placement === "bottom" ? "below" : "right",
      };
    }
    workspaceRef.current = result.workspace;
    setWorkspace(result.workspace);

    if (result.status === "tab-limit") {
      recordGlobalError(
        "topics",
        "Max tabs opened in group",
        "Max tabs opened in group",
      );
    } else if (result.status === "pane-limit" || result.status === "group-limit") {
      recordGlobalError(
        "topics",
        "Group limit reached",
        "Milena supports up to four open groups.",
      );
    }
  }

  function stopPane(paneId: number) {
    const generation = workspaceGeneration.current;
    nextPollingRun(paneId);
    void stopPanePollingSession({
      paneId,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      stopConsumerSession: stopKafkaConsumerSession,
      onStopError: (message) => {
        if (workspaceGeneration.current === generation) {
          recordGlobalError("consumer", "Consumer cleanup failed", message, paneId);
        }
      },
    });
  }

  function clearPaneMessages(paneId: number) {
    updateWorkspace((current) => clearPaneActivity(current, paneId));
  }

  function closeWorkspacePane(paneId: number) {
    const generation = workspaceGeneration.current;
    nextPollingRun(paneId);
    void closePanePollingSession({
      paneId,
      getWorkspace: () => workspaceRef.current,
      updateWorkspace,
      stopConsumerSession: stopKafkaConsumerSession,
      onStopError: (message) => {
        if (workspaceGeneration.current === generation) {
          recordGlobalError("consumer", "Consumer cleanup failed", message, paneId);
        }
      },
    });
  }

  function updateWorkspace(update: (current: WorkspaceState) => WorkspaceState) {
    setWorkspace((current) => {
      const next = update(current);
      workspaceRef.current = next;
      return next;
    });
  }

  function updatePublisherState(
    update: (current: PublisherState) => PublisherState,
  ) {
    setPublisherState(update);
  }

  function changePublisherPayload(
    paneId: number,
    topic: string | null,
    payload: string,
  ) {
    setPublisherState((current) =>
      setPublisherPayload(current, paneId, payload, topic),
    );
  }

  function changePublisherKey(
    paneId: number,
    topic: string | null,
    key: string,
  ) {
    setPublisherState((current) => setPublisherKey(current, paneId, key, topic));
  }

  function formatPanePublisherPayload(paneId: number, topic: string | null) {
    setPublisherState((current) =>
      formatPublisherPayload(current, paneId, topic),
    );
  }

  function sendPanePublisherRecord(paneId: number) {
    const generation = workspaceGeneration.current;
    void sendPublisherRecord({
      paneId,
      auth: activeRuntimeAuth,
      getWorkspace: () => workspaceRef.current,
      getPublisherState: () => publisherState,
      updatePublisherState: (update) => {
        if (workspaceGeneration.current === generation) {
          updatePublisherState(update);
        }
      },
      publishRecord: publishKafkaRecord,
      onError: (message) => {
        if (workspaceGeneration.current === generation) {
          recordGlobalError("producer", "Publish failed", message, paneId);
        }
      },
    });
  }

  function nextPollingRun(paneId: number) {
    const next = (pollingRuns.current.get(paneId) ?? 0) + 1;
    pollingRuns.current.set(paneId, next);
    return next;
  }

  function openTopicMenu(event: MouseEvent<HTMLElement>, topic: string) {
    event.preventDefault();
    event.stopPropagation();
    setTopicMenu({ topic, x: event.clientX, y: event.clientY });
  }

  function recordGlobalError(
    source: GlobalActivitySource,
    message: string,
    detail: string,
    paneId: number | null = null,
  ) {
    setActivity((current) =>
      appendGlobalError(current, source, message, { detail, paneId }),
    );
  }

  function clearActivityLog() {
    setActivity(clearGlobalActivity());
  }

  function setColumnVisibility(column: keyof VisibleColumns, visible: boolean) {
    setVisibleColumns((current) => ({
      ...current,
      [column]: visible,
    }));
  }

  function setRailWidth(column: RailColumn, width: number) {
    const nextWidth = clampRailWidth(column, width);
    setRailWidths((current) =>
      current[column] === nextWidth
        ? current
        : {
            ...current,
            [column]: nextWidth,
          },
    );
  }

  function startRailResize(
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) {
    event.preventDefault();
    railResize.current = {
      column,
      pointerId: event.pointerId,
      startX: event.clientX,
      startWidth: railWidths[column],
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  }

  function updateRailResize(
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) {
    const current = railResize.current;
    if (
      !current ||
      current.column !== column ||
      current.pointerId !== event.pointerId
    ) {
      return;
    }

    const delta = event.clientX - current.startX;
    setRailWidth(
      column,
      column === "topics"
        ? current.startWidth + delta
        : current.startWidth - delta,
    );
  }

  function stopRailResize(
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) {
    const current = railResize.current;
    if (
      !current ||
      current.column !== column ||
      current.pointerId !== event.pointerId
    ) {
      return;
    }

    if (event.currentTarget.hasPointerCapture?.(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    railResize.current = null;
  }

  function resizeRailByKeyboard(
    column: RailColumn,
    event: ReactKeyboardEvent<HTMLElement>,
  ) {
    const step = event.shiftKey ? RAIL_RESIZE_LARGE_STEP : RAIL_RESIZE_STEP;
    const currentWidth = railWidths[column];
    let nextWidth: number | null = null;

    if (event.key === "ArrowRight") {
      nextWidth = column === "topics" ? currentWidth + step : currentWidth - step;
    } else if (event.key === "ArrowLeft") {
      nextWidth = column === "topics" ? currentWidth - step : currentWidth + step;
    } else if (event.key === "Home") {
      nextWidth = RAIL_WIDTH_LIMITS[column].min;
    } else if (event.key === "End") {
      nextWidth = RAIL_WIDTH_LIMITS[column].max;
    }

    if (nextWidth === null) {
      return;
    }

    event.preventDefault();
    setRailWidth(column, nextWidth);
  }

  function changeAppearancePreference(preference: AppearancePreference) {
    if (appearancePreference === undefined) {
      writeAppearancePreference(preference);
      setLocalAppearancePreference(preference);
    }
    onAppearancePreferenceChange?.(preference);
  }

  function changeActiveEnvironment() {
    const currentWorkspace = workspaceRef.current;
    const currentTabs = currentWorkspace.groups.flatMap((group) => group.tabs);
    const sessionIds = Array.from(
      new Set(
        currentTabs.flatMap((tab) => {
          nextPollingRun(tab.id);
          const sessionId = getPanePollingSessionId(currentWorkspace, tab.id);
          return sessionId ? [sessionId] : [];
        }),
      ),
    );
    workspaceGeneration.current += 1;
    pollingRuns.current.clear();
    dockviewPlacementHint.current = null;
    const resetWorkspace = createInitialWorkspaceState();
    workspaceRef.current = resetWorkspace;
    setWorkspace(resetWorkspace);
    setPublisherState(createInitialPublisherState());
    setActivity(clearGlobalActivity());
    setTopicMenu(null);

    for (const sessionId of sessionIds) {
      void stopKafkaConsumerSession({ sessionId }).catch(() => undefined);
    }

    onChangeEnvironment?.(activeRuntimeAuth.environment);
  }

  return (
    <main
      className={[
        "app-shell",
        visibleColumns.topics ? "" : "topics-hidden",
        visibleColumns.inspector ? "" : "inspector-hidden",
      ]
        .filter(Boolean)
        .join(" ")}
      style={shellStyle}
    >
      {visibleColumns.topics ? (
        <aside className="rail rail-left" aria-label="Kafka topics">
          <RailResizeHandle
            column="topics"
            width={railWidths.topics}
            onKeyDown={resizeRailByKeyboard}
            onPointerCancel={stopRailResize}
            onPointerDown={startRailResize}
            onPointerLostCapture={stopRailResize}
            onPointerMove={updateRailResize}
            onPointerUp={stopRailResize}
          />
          <header className="rail-header">
            <div className="rail-title">
              <strong>{APP_SHELL_NAME}</strong>
              <small>{activeRuntimeAuth.environment}</small>
            </div>
            <div className="rail-header-actions">
              <button
                className="rail-toggle-button"
                type="button"
                aria-label="Hide topic sidebar"
                title="Hide topic sidebar"
                onClick={() => setColumnVisibility("topics", false)}
              >
                <PanelLeftClose aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            </div>
          </header>
          <section className="environment-summary">
            <div className="environment-summary-heading">
              <span>Cluster</span>
              <div className="environment-summary-actions">
                {onChangeEnvironment ? (
                  <button
                    className="refresh-button secondary compact"
                    type="button"
                    aria-label="Change environment"
                    title="Change environment"
                    onClick={changeActiveEnvironment}
                  >
                    <ChevronDown aria-hidden="true" size={14} strokeWidth={2} />
                    <span>Change</span>
                  </button>
                ) : null}
                <button
                  className="refresh-button secondary compact"
                  type="button"
                  disabled={topicRail.status === "loading"}
                  onClick={refreshTopicList}
                >
                  <RefreshCw aria-hidden="true" size={14} strokeWidth={2} />
                  <span>Refresh</span>
                </button>
              </div>
            </div>
            <strong>{activeRuntimeAuth.brokers.join(", ")}</strong>
            <small>
              {topicRailStatus(topicRail.status, topicRail.topics.length)}
            </small>
          </section>
          <div className="topic-search">
            <input
              aria-label="Search topics"
              placeholder="Search topics"
              type="search"
              value={topicRail.searchQuery}
              onChange={(event) => {
                const value = event.currentTarget.value;
                setTopicRail((current) =>
                  setTopicSearch(current, value),
                );
              }}
            />
          </div>
          {topicRail.error ? (
            <div className="error-strip compact">{topicRail.error}</div>
          ) : null}
          <nav className="topic-list">
            {topicRows.length === 0 ? (
              <p className="topic-empty">
                {topicRail.status === "loading" ? "Loading topics" : "No topics"}
              </p>
            ) : (
              topicRows.map((topic) => {
                const selected = topic.name === previewTopic;
                const pinAction = topic.pinned ? "Unpin" : "Pin";
                const renderMode = topicMessageRenderMode(
                  messageRenderPreferences,
                  activeEnvironmentKey,
                  topic.name,
                );

                return (
                  <div
                    className={[
                      "topic",
                      selected ? "active" : "",
                      topic.pinned ? "pinned" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    data-pinned={topic.pinned}
                    data-selected={selected}
                    key={topic.name}
                    onContextMenu={(event) => openTopicMenu(event, topic.name)}
                  >
                    <button
                      className="topic-select"
                      type="button"
                      aria-current={selected ? "true" : undefined}
                      onClick={() => selectTopic(topic.name)}
                      title={topic.name}
                    >
                      <span className="topic-marker" />
                      <span className="topic-copy">
                        <strong>{topic.name}</strong>
                        <small>
                          {topic.partitionCount} partitions / {renderMode}
                        </small>
                      </span>
                    </button>
                    <button
                      className="topic-open"
                      type="button"
                      aria-label={`Open ${topic.name}`}
                      onClick={() => openTopic(topic.name, "selected")}
                    >
                      Open
                    </button>
                    <button
                      className="topic-pin"
                      type="button"
                      aria-label={`${pinAction} ${topic.name}`}
                      aria-pressed={topic.pinned}
                      onClick={() => pinTopic(topic.name)}
                      title={`${pinAction} ${topic.name}`}
                    >
                      {topic.pinned ? (
                        <PinOff aria-hidden="true" size={14} strokeWidth={1.9} />
                      ) : (
                        <Pin aria-hidden="true" size={14} strokeWidth={1.9} />
                      )}
                    </button>
                  </div>
                );
              })
            )}
          </nav>
        </aside>
      ) : null}

      <section className="workspace" aria-label="Milena workspace">
        {!visibleColumns.topics || !visibleColumns.inspector ? (
          <div className="workspace-rail-controls" aria-label="Hidden sidebars">
            {!visibleColumns.topics ? (
              <button
                className="rail-toggle-button workspace-rail-toggle is-left"
                type="button"
                aria-label="Show topic sidebar"
                title="Show topic sidebar"
                onClick={() => setColumnVisibility("topics", true)}
              >
                <PanelLeftOpen aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            ) : null}
            {!visibleColumns.inspector ? (
              <button
                className="rail-toggle-button workspace-rail-toggle is-right"
                type="button"
                aria-label="Show inspector column"
                title="Show inspector column"
                onClick={() => setColumnVisibility("inspector", true)}
              >
                <PanelRightOpen aria-hidden="true" size={15} strokeWidth={1.9} />
              </button>
            ) : null}
          </div>
        ) : null}
        <DockviewWorkspace
          workspace={workspace}
          placementHintRef={dockviewPlacementHint}
          previewTopic={previewTopic}
          publisherState={publisherState}
          environmentKey={activeEnvironmentKey}
          renderPreferences={messageRenderPreferences}
          onActivateTab={(tab) => {
            updateWorkspace((current) => ({
              ...selectWorkspacePane(current, tab.id),
              selectedTopic: tab.topic,
            }));
          }}
          onDockviewSnapshot={(snapshot) => {
            const result = reconcileDockviewSnapshot(
              workspaceRef.current,
              snapshot,
            );
            if (result.status === "applied") {
              workspaceRef.current = result.workspace;
              setWorkspace(result.workspace);
            }
            return result;
          }}
          onMoveTab={moveTab}
          onExpandGroup={expandGroup}
          onRestoreGroup={restoreGroup}
          onPoll={(tab) => tab.topic && startPolling(tab.id, tab.topic)}
          onPayloadChange={changePublisherPayload}
          onKeyChange={changePublisherKey}
          onFormatPayload={formatPanePublisherPayload}
          onSend={sendPanePublisherRecord}
          onRenderModeChange={setMessageRenderMode}
          onStop={stopPane}
          onClearMessages={clearPaneMessages}
          onClose={closeWorkspacePane}
        />
      </section>

      {visibleColumns.inspector ? (
        <RightRail
          activePane={activePane}
          activity={activity}
          paneCount={workspace.panes.length}
          workspaceContext={{
            activeGroupId:
              workspace.groups.find(
                (group) => group.id === workspace.focusedGroupId,
              )?.id ??
              workspace.groups[0]?.id ??
              null,
            groupCount: workspace.groups.length,
            tabCount: workspace.panes.length,
          }}
          topicPreview={topicPreview}
          appearancePreference={currentAppearancePreference}
          onAppearancePreferenceChange={changeAppearancePreference}
          onClearActivity={clearActivityLog}
          onHideInspector={() => setColumnVisibility("inspector", false)}
          resizeHandle={
            <RailResizeHandle
              column="inspector"
              width={railWidths.inspector}
              onKeyDown={resizeRailByKeyboard}
              onPointerCancel={stopRailResize}
              onPointerDown={startRailResize}
              onPointerLostCapture={stopRailResize}
              onPointerMove={updateRailResize}
              onPointerUp={stopRailResize}
            />
          }
        />
      ) : null}
      {topicMenu ? (
        <TopicContextMenu
          menu={topicMenu}
          panes={workspace.panes}
          onOpen={(placement) => openTopic(topicMenu.topic, placement)}
        />
      ) : null}
    </main>
  );
}

type RailResizeHandleProps = {
  column: RailColumn;
  width: number;
  onKeyDown: (
    column: RailColumn,
    event: ReactKeyboardEvent<HTMLElement>,
  ) => void;
  onPointerCancel: (
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) => void;
  onPointerDown: (
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) => void;
  onPointerLostCapture: (
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) => void;
  onPointerMove: (
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) => void;
  onPointerUp: (
    column: RailColumn,
    event: ReactPointerEvent<HTMLElement>,
  ) => void;
};

function RailResizeHandle({
  column,
  width,
  onKeyDown,
  onPointerCancel,
  onPointerDown,
  onPointerLostCapture,
  onPointerMove,
  onPointerUp,
}: RailResizeHandleProps) {
  const limit = RAIL_WIDTH_LIMITS[column];
  const label =
    column === "topics" ? "Resize topic sidebar" : "Resize inspector column";

  return (
    <div
      className={`rail-resize-handle is-${column}`}
      role="separator"
      aria-label={label}
      aria-orientation="vertical"
      aria-valuemin={limit.min}
      aria-valuemax={limit.max}
      aria-valuenow={width}
      tabIndex={0}
      title={label}
      onKeyDown={(event) => onKeyDown(column, event)}
      onLostPointerCapture={(event) => onPointerLostCapture(column, event)}
      onPointerCancel={(event) => onPointerCancel(column, event)}
      onPointerDown={(event) => onPointerDown(column, event)}
      onPointerMove={(event) => onPointerMove(column, event)}
      onPointerUp={(event) => onPointerUp(column, event)}
    />
  );
}

function clampRailWidth(column: RailColumn, width: number) {
  const limit = RAIL_WIDTH_LIMITS[column];
  return Math.min(limit.max, Math.max(limit.min, Math.round(width)));
}

type DockviewWorkspaceProps = {
  workspace: WorkspaceState;
  placementHintRef: MutableRefObject<DockviewPlacementHint>;
  previewTopic: string | null;
  publisherState: PublisherState;
  environmentKey: string;
  renderPreferences: MessageRenderPreferences;
  onActivateTab: (tab: WorkspacePane) => void;
  onDockviewSnapshot: (
    snapshot: DockviewWorkspaceSnapshot,
  ) => DockviewReconcileResult;
  onMoveTab: (tabId: number, direction: TabMoveDirection) => void;
  onExpandGroup: (groupId: number) => void;
  onRestoreGroup: () => void;
  onPoll: (tab: WorkspacePane) => void;
  onPayloadChange: (paneId: number, topic: string | null, payload: string) => void;
  onKeyChange: (paneId: number, topic: string | null, key: string) => void;
  onFormatPayload: (paneId: number, topic: string | null) => void;
  onSend: (paneId: number) => void;
  onRenderModeChange: (topic: string, mode: MessageRenderMode) => void;
  onStop: (paneId: number) => void;
  onClearMessages: (paneId: number) => void;
  onClose: (paneId: number) => void;
};

const DOCKVIEW_COMPONENTS = {
  [DOCKVIEW_TOPIC_PANEL_COMPONENT]: DockviewTopicPanel,
};

function DockviewWorkspace({
  workspace,
  placementHintRef,
  previewTopic,
  publisherState,
  environmentKey,
  renderPreferences,
  onActivateTab,
  onDockviewSnapshot,
  onMoveTab,
  onExpandGroup,
  onRestoreGroup,
  onPoll,
  onPayloadChange,
  onKeyChange,
  onFormatPayload,
  onSend,
  onRenderModeChange,
  onStop,
  onClearMessages,
  onClose,
}: DockviewWorkspaceProps) {
  const dockviewApi = useRef<DockviewApi | null>(null);
  const dockviewSubscriptions = useRef<{ dispose: () => void }[]>([]);
  const syncingDockview = useRef(false);
  const [dockviewReadyTick, setDockviewReadyTick] = useState(0);
  const dockviewCallbacks = useRef({ onClose, onDockviewSnapshot });
  useEffect(() => {
    dockviewCallbacks.current = { onClose, onDockviewSnapshot };
  }, [onClose, onDockviewSnapshot]);
  const options = useMemo(createDockviewWorkspaceOptions, []);
  const panelParams = useCallback(
    (pane: WorkspacePane): DockviewPanelParams => ({
      pane,
      active: pane.id === workspace.selectedPaneId,
      environmentKey,
      isExpanded: pane.id === workspace.expandedPaneId,
      publisher: getPublisherPaneState(publisherState, pane.id, pane.topic),
      renderPreferences,
      onActivate: () => onActivateTab(pane),
      onPoll: () => onPoll(pane),
      onPayloadChange: (payload) => onPayloadChange(pane.id, pane.topic, payload),
      onKeyChange: (key) => onKeyChange(pane.id, pane.topic, key),
      onFormatPayload: () => onFormatPayload(pane.id, pane.topic),
      onSend: () => onSend(pane.id),
      onRenderModeChange,
      onStop: () => onStop(pane.id),
      onClearMessages: () => onClearMessages(pane.id),
      onClose: () => onClose(pane.id),
    }),
    [
      environmentKey,
      onActivateTab,
      onClearMessages,
      onClose,
      onFormatPayload,
      onKeyChange,
      onPayloadChange,
      onPoll,
      onRenderModeChange,
      onSend,
      onStop,
      publisherState,
      renderPreferences,
      workspace.expandedPaneId,
      workspace.selectedPaneId,
    ],
  );

  const rightHeaderActionsComponent = useMemo(
    () =>
      function DockviewHeaderActions(props: IDockviewHeaderActionsProps) {
        return (
          <DockviewWorkspaceHeaderActions
            {...props}
            workspace={workspace}
            onMoveTab={onMoveTab}
            onExpandGroup={onExpandGroup}
            onRestoreGroup={onRestoreGroup}
            onClose={onClose}
          />
        );
      },
    [onClose, onExpandGroup, onMoveTab, onRestoreGroup, workspace],
  );

  const onReady = useCallback((event: DockviewReadyEvent) => {
    dockviewSubscriptions.current.forEach((disposable) => disposable.dispose());
    dockviewApi.current = event.api;
    const syncFromDockview = () => {
      if (syncingDockview.current) {
        return;
      }

      const result = dockviewCallbacks.current.onDockviewSnapshot(
        snapshotDockviewApi(event.api),
      );
      pruneRejectedDockviewSnapshot(event.api, result, syncingDockview);
    };
    dockviewSubscriptions.current = [
      event.api.onDidRemovePanel((panel) => {
        if (syncingDockview.current) {
          return;
        }

        const tabId = fromDockviewPanelId(panel.id);
        if (tabId !== null) {
          dockviewCallbacks.current.onClose(tabId);
        }
      }),
      event.api.onDidActivePanelChange(syncFromDockview),
      event.api.onDidActiveGroupChange(syncFromDockview),
      event.api.onDidMovePanel(syncFromDockview),
      event.api.onDidDrop(syncFromDockview),
      event.api.onDidMaximizedGroupChange(syncFromDockview),
    ];
    setDockviewReadyTick((tick) => tick + 1);
  }, []);

  useEffect(
    () => () => {
      dockviewSubscriptions.current.forEach((disposable) => disposable.dispose());
      dockviewSubscriptions.current = [];
    },
    [],
  );

  useLayoutEffect(() => {
    const api = dockviewApi.current;
    if (!api) {
      return;
    }

    syncingDockview.current = true;
    try {
      syncDockviewWorkspace({
        api,
        workspace,
        panelParams,
        placementHint: placementHintRef.current,
      });
      placementHintRef.current = null;
    } finally {
      syncingDockview.current = false;
    }
  }, [dockviewReadyTick, panelParams, placementHintRef, workspace]);

  if (workspace.groups.length === 0) {
    return (
      <section className="dockview-workspace is-empty">
        <p className="workspace-empty">
          {previewTopic
            ? "Topic selected for preview."
            : "Select a topic to preview it."}
        </p>
      </section>
    );
  }

  return (
    <section className="dockview-workspace" aria-label="Tabbed workspace">
      <DockviewReact
        className="dockview-theme-milena"
        components={DOCKVIEW_COMPONENTS}
        disableFloatingGroups={options.disableFloatingGroups}
        getTabContextMenuItems={options.getTabContextMenuItems}
        getTabGroupChipContextMenuItems={options.getTabGroupChipContextMenuItems}
        onReady={onReady}
        onWillDrop={(event: DockviewWillDropEvent) =>
          handleDockviewWillDrop(workspace, event)
        }
        rightHeaderActionsComponent={rightHeaderActionsComponent}
        singleTabMode="fullwidth"
      />
    </section>
  );
}

function pruneRejectedDockviewSnapshot(
  api: DockviewApi,
  result: DockviewReconcileResult,
  syncingDockview: MutableRefObject<boolean>,
) {
  if (
    result.prunePanelIds.length === 0 &&
    result.rejectedGroupIds.length === 0
  ) {
    return;
  }

  syncingDockview.current = true;
  try {
    for (const panelId of result.prunePanelIds) {
      const panel = api.getPanel(panelId);
      if (panel) {
        api.removePanel(panel);
      }
    }

    for (const groupId of result.rejectedGroupIds) {
      const group = api.getGroup(groupId);
      if (group) {
        api.removeGroup(group);
      }
    }
  } finally {
    syncingDockview.current = false;
  }
}

function DockviewTopicPanel({ params }: IDockviewPanelProps<DockviewPanelParams>) {
  return (
    <Pane
      pane={params.pane}
      active={params.active}
      canSplit={false}
      isExpanded={params.isExpanded}
      onActivate={params.onActivate}
      onPoll={params.onPoll}
      publisher={params.publisher}
      onPayloadChange={params.onPayloadChange}
      onKeyChange={params.onKeyChange}
      onFormatPayload={params.onFormatPayload}
      onSend={params.onSend}
      environmentKey={params.environmentKey}
      renderPreferences={params.renderPreferences}
      onRenderModeChange={params.onRenderModeChange}
      onSplit={() => undefined}
      onExpand={() => undefined}
      onRestore={() => undefined}
      onStop={params.onStop}
      onClearMessages={params.onClearMessages}
      onClose={params.onClose}
      showWorkspaceActions={false}
    />
  );
}

function DockviewWorkspaceHeaderActions({
  activePanel,
  api,
  group,
  workspace,
  onMoveTab,
  onExpandGroup,
  onRestoreGroup,
  onClose,
}: IDockviewHeaderActionsProps & {
  workspace: WorkspaceState;
  onMoveTab: (tabId: number, direction: TabMoveDirection) => void;
  onExpandGroup: (groupId: number) => void;
  onRestoreGroup: () => void;
  onClose: (paneId: number) => void;
}) {
  const activeTabId = activePanel ? fromDockviewPanelId(activePanel.id) : null;
  const groupId =
    fromDockviewGroupId(group.id) ??
    workspace.groups.find((candidate) =>
      candidate.tabs.some((tab) => tab.id === activeTabId),
    )?.id ??
    null;
  const moveDisabled =
    activeTabId === null ||
    (workspace.groups.length === 1 && workspace.groups[0]?.tabs.length === 1);
  const maximized = api.isMaximized();

  return (
    <div className="dockview-group-actions">
      <button
        className="pane-icon-button"
        type="button"
        aria-label={
          activeTabId === null
            ? "Move active tab right"
            : `Move tab ${activeTabId} right`
        }
        title="Move active tab right"
        disabled={moveDisabled}
        onClick={(event) => {
          event.stopPropagation();
          if (activeTabId !== null) {
            onMoveTab(activeTabId, "right");
          }
        }}
      >
        <SplitSquareHorizontal aria-hidden="true" size={14} strokeWidth={1.9} />
      </button>
      <button
        className="pane-icon-button"
        type="button"
        aria-label={
          activeTabId === null
            ? "Move active tab bottom"
            : `Move tab ${activeTabId} bottom`
        }
        title="Move active tab bottom"
        disabled={moveDisabled}
        onClick={(event) => {
          event.stopPropagation();
          if (activeTabId !== null) {
            onMoveTab(activeTabId, "bottom");
          }
        }}
      >
        <SplitSquareVertical aria-hidden="true" size={14} strokeWidth={1.9} />
      </button>
      <button
        className="pane-icon-button"
        type="button"
        aria-label={
          maximized
            ? `Restore group ${groupId ?? ""}`.trim()
            : `Maximize group ${groupId ?? ""}`.trim()
        }
        title={maximized ? "Restore group" : "Maximize group"}
        onClick={(event) => {
          event.stopPropagation();
          if (maximized) {
            api.exitMaximized();
            onRestoreGroup();
          } else {
            api.maximize();
            if (groupId !== null) {
              onExpandGroup(groupId);
            }
          }
        }}
      >
        {maximized ? (
          <Minimize2 aria-hidden="true" size={14} strokeWidth={1.9} />
        ) : (
          <Maximize2 aria-hidden="true" size={14} strokeWidth={1.9} />
        )}
      </button>
      <button
        className="pane-icon-button pane-close-button"
        type="button"
        aria-label={
          activeTabId === null ? "Close active tab" : `Close tab ${activeTabId}`
        }
        title="Close active tab"
        disabled={activeTabId === null}
        onClick={(event) => {
          event.stopPropagation();
          if (activeTabId !== null) {
            onClose(activeTabId);
          }
        }}
      >
        <X aria-hidden="true" size={14} strokeWidth={2.1} />
      </button>
    </div>
  );
}

export function syncDockviewWorkspace({
  api,
  workspace,
  panelParams,
  placementHint,
}: {
  api: DockviewApi;
  workspace: WorkspaceState;
  panelParams: (pane: WorkspacePane) => DockviewPanelParams;
  placementHint: DockviewPlacementHint;
}) {
  const mapping = mapWorkspaceToDockview(workspace);

  const canonicalPanelIds = new Set(mapping.panels.map((panel) => panel.id));
  for (const panel of [...api.panels]) {
    if (!canonicalPanelIds.has(panel.id)) {
      api.removePanel(panel);
    }
  }

  for (const group of workspace.groups) {
    const groupIndex = workspace.groups.findIndex(
      (candidate) => candidate.id === group.id,
    );

    for (const [tabIndex, tab] of group.tabs.entries()) {
      const panelId = toDockviewPanelId(tab.id);
      const existing = api.getPanel(panelId);
      const params = panelParams(tab);

      if (existing) {
        updateDockviewPanel(existing, tab, params);
        moveDockviewPanelIfNeeded({
          api,
          workspace,
          groupIndex,
          tabIndex,
          panel: existing,
          placementHint,
        });
        continue;
      }

      api.addPanel({
        id: panelId,
        title: tab.title,
        component: DOCKVIEW_TOPIC_PANEL_COMPONENT,
        params,
        position: dockviewPanelPosition({
          api,
          workspace,
          groupIndex,
          tabIndex,
          tabId: tab.id,
          placementHint,
        }),
      });
    }
  }

  if (api.width === 0 && api.height === 0) {
    api.layout(1_000, 700, true);
  }

  const activePanelId = toDockviewPanelId(workspace.selectedPaneId);
  api.getPanel(activePanelId)?.api.setActive();
  if (workspace.expandedGroupId === null && api.hasMaximizedGroup()) {
    api.exitMaximizedGroup();
  }
}

function moveDockviewPanelIfNeeded({
  api,
  workspace,
  groupIndex,
  tabIndex,
  panel,
  placementHint,
}: {
  api: DockviewApi;
  workspace: WorkspaceState;
  groupIndex: number;
  tabIndex: number;
  panel: IDockviewPanel;
  placementHint: DockviewPlacementHint;
}) {
  const group = workspace.groups[groupIndex];
  if (!group) {
    return;
  }

  const currentIndex = panel.group.panels.findIndex(
    (candidate) => candidate.id === panel.id,
  );
  const targetGroup = dockviewGroupForWorkspaceGroup(api, group, panel.id);
  if (targetGroup) {
    if (panel.group !== targetGroup || currentIndex !== tabIndex) {
      panel.api.moveTo({
        group: targetGroup,
        position: "center",
        index: tabIndex,
      });
    }
    return;
  }

  if (dockviewGroupMatchesWorkspaceGroup(panel.group, group)) {
    return;
  }

  const tabId = fromDockviewPanelId(panel.id);
  if (tabId === null || placementHint?.tabId !== tabId) {
    return;
  }

  const referenceGroup = dockviewReferenceGroup(api, workspace, groupIndex);
  if (!referenceGroup) {
    return;
  }

  panel.api.moveTo({
    group: referenceGroup,
    position: dockviewMovePosition(placementHint, panel.id),
    index: tabIndex,
  });
}

function updateDockviewPanel(
  panel: IDockviewPanel,
  tab: WorkspacePane,
  params: DockviewPanelParams,
) {
  if (panel.title !== tab.title) {
    panel.setTitle(tab.title ?? "");
  }
  panel.api.updateParameters(params);
}

function dockviewPanelPosition({
  api,
  workspace,
  groupIndex,
  tabIndex,
  tabId,
  placementHint,
}: {
  api: DockviewApi;
  workspace: WorkspaceState;
  groupIndex: number;
  tabIndex: number;
  tabId: number;
  placementHint: DockviewPlacementHint;
}): Parameters<DockviewApi["addPanel"]>[0]["position"] {
  const group = workspace.groups[groupIndex];
  const previousTab = group?.tabs[tabIndex - 1];
  if (previousTab) {
    return {
      referencePanel: toDockviewPanelId(previousTab.id),
      direction: "within",
      index: tabIndex,
    };
  }

  const targetGroup = group
    ? dockviewGroupForWorkspaceGroup(api, group, toDockviewPanelId(tabId))
    : undefined;
  if (targetGroup) {
    return {
      referenceGroup: targetGroup,
      direction: "within",
      index: tabIndex,
    };
  }

  const referenceGroup = dockviewReferenceGroup(api, workspace, groupIndex);

  if (referenceGroup) {
    return {
      referenceGroup,
      direction: dockviewAddPanelDirection(placementHint, tabId),
    };
  }

  return undefined;
}

function dockviewGroupForWorkspaceGroup(
  api: DockviewApi,
  group: WorkspaceState["groups"][number],
  movingPanelId: string,
): IDockviewPanel["group"] | undefined {
  const desiredPanelIds = new Set(
    group.tabs.map((tab) => toDockviewPanelId(tab.id)),
  );

  for (const tab of group.tabs) {
    const panelId = toDockviewPanelId(tab.id);
    if (panelId === movingPanelId) {
      continue;
    }

    const panel = api.getPanel(panelId);
    if (
      panel &&
      panel.group.panels.every((candidate) => desiredPanelIds.has(candidate.id))
    ) {
      return panel.group;
    }
  }

  return undefined;
}

function dockviewReferenceGroup(
  api: DockviewApi,
  workspace: WorkspaceState,
  groupIndex: number,
): IDockviewPanel["group"] | undefined {
  for (let index = groupIndex - 1; index >= 0; index -= 1) {
    const group = workspace.groups[index];
    const panel = group ? api.getPanel(toDockviewPanelId(group.activeTabId)) : null;
    if (panel) {
      return panel.group;
    }
  }

  for (let index = groupIndex + 1; index < workspace.groups.length; index += 1) {
    const group = workspace.groups[index];
    const panel = group ? api.getPanel(toDockviewPanelId(group.activeTabId)) : null;
    if (panel) {
      return panel.group;
    }
  }

  return undefined;
}

function dockviewGroupMatchesWorkspaceGroup(
  group: IDockviewPanel["group"],
  workspaceGroup: WorkspaceState["groups"][number],
): boolean {
  const actualPanelIds = group.panels.map((panel) => panel.id);
  const expectedPanelIds = workspaceGroup.tabs.map((tab) =>
    toDockviewPanelId(tab.id),
  );

  return (
    actualPanelIds.length === expectedPanelIds.length &&
    expectedPanelIds.every((panelId) => actualPanelIds.includes(panelId))
  );
}

function dockviewAddPanelDirection(
  placementHint: DockviewPlacementHint,
  tabId: number,
) {
  return placementHint?.tabId === tabId ? placementHint.direction : "right";
}

function dockviewMovePosition(
  placementHint: DockviewPlacementHint,
  panelId: string,
) {
  const tabId = fromDockviewPanelId(panelId);
  return tabId !== null && placementHint?.tabId === tabId
    ? placementHint.direction === "below"
      ? "bottom"
      : "right"
    : "right";
}

function AppearanceControl({
  value,
  onChange,
}: {
  value: AppearancePreference;
  onChange: (preference: AppearancePreference) => void;
}) {
  return (
    <div className="appearance-control" role="group" aria-label="Appearance">
      {APPEARANCE_OPTIONS.map((option) => {
        const Icon = option.icon;
        const selected = option.value === value;

        return (
          <button
            className="appearance-option"
            type="button"
            aria-label={option.ariaLabel}
            aria-pressed={selected}
            key={option.value}
            title={option.label}
            onClick={() => onChange(option.value)}
          >
            <Icon aria-hidden="true" size={14} strokeWidth={1.9} />
          </button>
        );
      })}
    </div>
  );
}

export function RightRail({
  activePane,
  activity,
  appearancePreference = "system",
  paneCount,
  workspaceContext,
  topicPreview,
  onAppearancePreferenceChange = () => undefined,
  onClearActivity,
  onHideInspector = () => undefined,
  resizeHandle = null,
}: {
  activePane: WorkspacePane | undefined;
  activity: GlobalActivityEntry[];
  appearancePreference?: AppearancePreference;
  paneCount: number;
  workspaceContext?: WorkspaceContext;
  topicPreview: TopicPreviewModel | null;
  onAppearancePreferenceChange?: (preference: AppearancePreference) => void;
  onClearActivity: () => void;
  onHideInspector?: () => void;
  resizeHandle?: ReactNode;
}) {
  const tabSummary = formatGroupSummary(paneCount);
  const groupSummary = workspaceContext
    ? formatWorkspaceContext(workspaceContext)
    : tabSummary;
  const statusClass = topicPreview ? "preview" : activePane?.status ?? "idle";
  const contextPill = topicPreview ? "preview" : activePane ? "selected" : "idle";
  const inspectorTitle = topicPreview
    ? "Topic preview"
    : activePane
      ? "Selected tab"
      : "No selection";
  const inspectorContext = topicPreview
    ? topicPreview.name
    : activePane
      ? `Tab ${activePane.id}`
      : groupSummary;

  return (
    <aside className="rail rail-right" aria-label="Activity log">
      {resizeHandle}
      <header className="rail-header">
        <div className="rail-title">
          <span className="eyebrow">Inspector</span>
          <strong>{inspectorTitle}</strong>
          <small>{inspectorContext}</small>
        </div>
        <div className="rail-header-actions">
          <AppearanceControl
            value={appearancePreference}
            onChange={onAppearancePreferenceChange}
          />
          <span className={`status-pill ${statusClass}`}>{contextPill}</span>
          <button
            className="rail-toggle-button"
            type="button"
            aria-label="Hide inspector column"
            title="Hide inspector column"
            onClick={onHideInspector}
          >
            <PanelRightClose aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
        </div>
      </header>
      {topicPreview ? (
        <section className="topic-preview">
          <div>
            <span>Topic</span>
            <strong>{topicPreview.name}</strong>
          </div>
          <div>
            <span>Partitions</span>
            <strong>
              {topicPreview.partitionCount === null
                ? "unknown"
                : `${topicPreview.partitionCount} partitions`}
            </strong>
          </div>
          <div>
            <span>Render</span>
            <strong>{topicPreview.renderMode}</strong>
          </div>
          <div className="topic-actions">
            <span>Open actions</span>
            <strong>
              <span>Poll</span>
              <span>Publish</span>
            </strong>
          </div>
        </section>
      ) : (
        <section className="pane-state">
          {activePane ? (
            <>
              <div>
                <span>Tab</span>
                <strong>Tab {activePane.id}</strong>
              </div>
              <div>
                <span>Status</span>
                <strong>{activePane.status}</strong>
              </div>
            </>
          ) : null}
          <div>
            <span>Topic</span>
            <strong>{activePane?.topic ?? "none"}</strong>
          </div>
          <div>
            <span>Group</span>
            <strong>{activePane?.consumerGroup ?? "none"}</strong>
          </div>
          <div>
            <span>Workspace</span>
            <strong>{tabSummary}</strong>
          </div>
          <div>
            <span>Active group</span>
            <strong>{groupSummary}</strong>
          </div>
        </section>
      )}
      <section className="activity-log">
        <header>
          <span className="eyebrow">Activity & errors</span>
          <button type="button" onClick={onClearActivity}>
            Clear
          </button>
        </header>
        {activity.length === 0 ? (
          <p>Inactive</p>
        ) : (
          activity.map((event) => (
            <p
              className={event.severity === "error" ? "activity-error" : ""}
              key={event.id}
            >
              <span>{event.time}</span>
              <strong>{formatActivityScope(event)}</strong>
              <span>
                {event.message}
                {event.detail ? <small>{event.detail}</small> : null}
              </span>
            </p>
          ))
        )}
      </section>
    </aside>
  );
}

type WorkspaceContext = {
  activeGroupId: number | null;
  groupCount: number;
  tabCount: number;
};

type PaneProps = {
  pane: WorkspacePane;
  active: boolean;
  canSplit: boolean;
  isExpanded: boolean;
  onActivate: () => void;
  onPoll: () => void;
  publisher: PublisherPaneState;
  onPayloadChange: (payload: string) => void;
  onKeyChange: (key: string) => void;
  onFormatPayload: () => void;
  onSend: () => void;
  environmentKey: string;
  renderPreferences: MessageRenderPreferences;
  onRenderModeChange: (topic: string, mode: MessageRenderMode) => void;
  onSplit: (direction: SplitDirection) => void;
  onExpand: () => void;
  onRestore: () => void;
  onStop: () => void;
  onClearMessages: () => void;
  onClose: () => void;
  showWorkspaceActions?: boolean;
};

export function Pane({
  pane,
  active,
  canSplit,
  isExpanded,
  onActivate,
  onPoll,
  publisher,
  onPayloadChange,
  onKeyChange,
  onFormatPayload,
  onSend,
  environmentKey,
  renderPreferences,
  onRenderModeChange,
  onSplit,
  onExpand,
  onRestore,
  onStop,
  onClearMessages,
  onClose,
  showWorkspaceActions = true,
}: PaneProps) {
  const empty = isPaneEmpty(pane);
  const canStart = canStartPaneSession(pane);
  const renderMode = topicMessageRenderMode(
    renderPreferences,
    environmentKey,
    pane.topic,
  );
  const [expandedRows, setExpandedRows] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [publisherExpanded, setPublisherExpanded] = useState(false);
  const [consumerFilter, setConsumerFilter] = useState("");
  useEffect(() => {
    setPublisherExpanded(false);
    setConsumerFilter("");
  }, [pane.id, pane.topic]);
  const recordEvents = pane.activity.filter(isKafkaRecordEvent);
  const visibleRecordEvents = recordEvents.filter((event) =>
    consumerRecordMatchesFilter(event.data.record, consumerFilter),
  );
  const payloadValidation = validatePublisherPayload(publisher.payload);
  const canSend = canSendPublisherRecord(pane, publisher);
  const publisherReadiness = publisherReadinessLabel(
    pane,
    publisher,
    payloadValidation.ok ? null : payloadValidation.error,
  );
  const sessionButton = paneSessionButtonState(pane);
  const compactStatus = compactPaneStatusLabel(pane);

  function toggleRow(identity: string) {
    setExpandedRows((current) => {
      const next = new Set(current);
      if (next.has(identity)) {
        next.delete(identity);
      } else {
        next.add(identity);
      }
      return next;
    });
  }

  function clearMessages() {
    setExpandedRows(new Set());
    onClearMessages();
  }

  return (
    <article
      className={[
        "pane",
        active ? "active" : "",
        empty ? "is-empty" : "",
        pane.tone === "error" ? "has-error" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      aria-label={`Tab ${pane.id}`}
      aria-current={active ? "true" : undefined}
      onClick={onActivate}
    >
      <header className="pane-header">
        <div className="pane-identity">
          <span className="eyebrow">Tab {pane.id} / {pane.mode}</span>
          <div className="pane-topic-row">
            <h2>{pane.topic ?? "Empty tab"}</h2>
            <div className="pane-session-actions">
              <button
                className="pane-session-button"
                type="button"
                aria-label={sessionButton.label}
                onClick={stopEvent(
                  sessionButton.action === "stop" ? onStop : onPoll,
                )}
                disabled={!canStart}
              >
                {sessionButton.action === "stop" ? (
                  <Square aria-hidden="true" size={11} fill="currentColor" />
                ) : (
                  <Play aria-hidden="true" size={12} fill="currentColor" />
                )}
                <span>{sessionButton.label}</span>
              </button>
            </div>
          </div>
          <small>{pane.consumerGroup ?? "No consumer group"}</small>
        </div>
        {showWorkspaceActions ? (
        <div className="pane-actions">
          <button
            className="pane-icon-button pane-expand-button"
            type="button"
            title={
                isExpanded ? `Restore tab ${pane.id}` : `Maximize tab ${pane.id}`
            }
            aria-label={
                isExpanded ? `Restore tab ${pane.id}` : `Maximize tab ${pane.id}`
            }
            onClick={stopEvent(isExpanded ? onRestore : onExpand)}
          >
            {isExpanded ? (
              <Minimize2 aria-hidden="true" size={15} strokeWidth={1.9} />
            ) : (
              <Maximize2 aria-hidden="true" size={15} strokeWidth={1.9} />
            )}
          </button>
          <button
            className="pane-icon-button pane-close-button"
            type="button"
            title={`Close tab ${pane.id}`}
            aria-label={`Close tab ${pane.id}`}
            onClick={stopEvent(onClose)}
          >
            <X aria-hidden="true" size={15} strokeWidth={2.1} />
          </button>
          <button
            className="pane-icon-button pane-split-button"
            type="button"
            aria-label={`Move tab ${pane.id} right`}
            title={`Move tab ${pane.id} right`}
            onClick={stopEvent(() => onSplit("right"))}
            disabled={!canSplit}
          >
            <SplitSquareHorizontal aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
          <button
            className="pane-icon-button pane-split-button"
            type="button"
            aria-label={`Move tab ${pane.id} bottom`}
            title={`Move tab ${pane.id} bottom`}
            onClick={stopEvent(() => onSplit("bottom"))}
            disabled={!canSplit}
          >
            <SplitSquareVertical aria-hidden="true" size={15} strokeWidth={1.9} />
          </button>
        </div>
        ) : null}
      </header>

      {pane.error ? (
        <div className="error-strip pane-error">{pane.error}</div>
      ) : null}

      <div className="session-grid">
        <div className="stat-cell">
          <span>Status</span>
          <strong>{pane.status}</strong>
        </div>
        <div className="stat-cell">
          <span>Session</span>
          <strong>{pane.session?.sessionId ?? "none"}</strong>
        </div>
        <div className="stat-cell">
          <span>Mode</span>
          <strong>{pane.mode}</strong>
        </div>
      </div>

      <div
        className={`pane-workbench ${
          publisherExpanded ? "has-expanded-publisher" : "has-collapsed-publisher"
        }`}
      >
        <section className="consumer" aria-label={`Consumer for tab ${pane.id}`}>
          <header>
            <span className="eyebrow">Consumer</span>
            <div className="consumer-controls">
              {pane.topic ? (
                <>
                  <label
                    className="consumer-filter-control"
                    onClick={(event) => event.stopPropagation()}
                  >
                    <Search aria-hidden="true" size={12} strokeWidth={2} />
                    <span>Filter</span>
                    <input
                      aria-label={`Filter records for tab ${pane.id}`}
                      type="search"
                      placeholder="Key or payload"
                      value={consumerFilter}
                      onChange={(event) =>
                        setConsumerFilter(event.currentTarget.value)
                      }
                    />
                  </label>
                  <label
                    className="render-mode-control"
                    onClick={(event) => event.stopPropagation()}
                  >
                    <span>Render</span>
                    <select
                      aria-label={`Render mode for ${pane.topic}`}
                      value={renderMode}
                      onChange={(event) =>
                        onRenderModeChange(
                          pane.topic ?? "",
                          event.currentTarget.value as MessageRenderMode,
                        )
                      }
                    >
                      <option value="json">JSON</option>
                      <option value="raw">Raw</option>
                    </select>
                  </label>
                </>
              ) : null}
              <button
                className="secondary compact consumer-clear-button"
                type="button"
                aria-label={`Clear messages for tab ${pane.id}`}
                title={`Clear messages for tab ${pane.id}`}
                onClick={stopEvent(clearMessages)}
                disabled={recordEvents.length === 0}
              >
                <Trash2 aria-hidden="true" size={13} strokeWidth={1.9} />
                <span>Clear</span>
              </button>
              <span className={`status-pill ${pane.status}`}>{compactStatus}</span>
            </div>
          </header>
          <div className="message-stream">
            {recordEvents.length === 0 ? (
              <p className="empty-state">Inactive</p>
            ) : visibleRecordEvents.length === 0 ? (
              <p className="empty-state">No matching records</p>
            ) : (
              visibleRecordEvents.map((event) => {
                const collapsed = renderKafkaRecord(event.data.record, {
                  mode: renderMode,
                });
                const rendered = renderKafkaRecord(event.data.record, {
                  mode: renderMode,
                  expanded: expandedRows.has(collapsed.identity),
                });

                return (
                  <MessageStreamRow
                    key={rendered.identity}
                    message={rendered}
                    filter={consumerFilter}
                    onToggle={() => toggleRow(rendered.identity)}
                  />
                );
              })
            )}
          </div>
        </section>

        <section
          className={`publisher ${publisherExpanded ? "expanded" : "collapsed"}`}
          aria-label={`Publisher for tab ${pane.id}`}
          onClick={(event) => event.stopPropagation()}
        >
          <header>
            <div className="publisher-heading">
              <span className="eyebrow">Publisher</span>
              <small>
                {publisherExpanded ? "Draft editor" : publisher.error ?? publisherReadiness}
              </small>
            </div>
            <button
              className="secondary compact"
              type="button"
              aria-controls={`publisher-panel-${pane.id}`}
              aria-expanded={publisherExpanded}
              aria-label={`${publisherExpanded ? "Collapse" : "Expand"} publisher for tab ${pane.id}`}
              onClick={stopEvent(() =>
                setPublisherExpanded((expanded) => !expanded),
              )}
            >
              <span>{publisherExpanded ? "Collapse" : "Expand"}</span>
              {publisherExpanded ? (
                <ChevronDown aria-hidden="true" size={14} strokeWidth={2} />
              ) : (
                <ChevronUp aria-hidden="true" size={14} strokeWidth={2} />
              )}
            </button>
          </header>
          {publisherExpanded ? (
            <div className="publisher-panel" id={`publisher-panel-${pane.id}`}>
              <div className="publisher-tools">
                <button
                  className="secondary compact"
                  type="button"
                  onClick={stopEvent(onFormatPayload)}
                >
                  Format JSON
                </button>
              </div>
              <div className="publisher-fields">
                <label>
                  <span>Key</span>
                  <input
                    aria-label={`Kafka key for tab ${pane.id}`}
                    placeholder="Optional key"
                    type="text"
                    value={publisher.key}
                    onChange={(event) => onKeyChange(event.currentTarget.value)}
                  />
                </label>
                <label>
                  <span>Payload</span>
                  <textarea
                    aria-label={`JSON payload for tab ${pane.id}`}
                    spellCheck={false}
                    value={publisher.payload}
                    onChange={(event) => onPayloadChange(event.currentTarget.value)}
                  />
                </label>
              </div>
              <section className={`producer-status ${publisher.status}`}>
                <div>
                  <span className="eyebrow">Producer</span>
                  <strong>{producerAckLabel(publisher.ack)}</strong>
                </div>
                <small>{publisher.error ?? publisherReadiness}</small>
              </section>
              <div className="publisher-footer">
                <span>{publisher.status === "sending" ? "sending" : "acks=all"}</span>
                <button
                  className="primary"
                  type="button"
                  onClick={stopEvent(onSend)}
                  disabled={!canSend}
                >
                  Send
                </button>
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </article>
  );
}

function MessageStreamRow({
  message,
  filter,
  onToggle,
}: {
  message: RenderedKafkaRecord;
  filter: string;
  onToggle: () => void;
}) {
  return (
    <article
      className={[
        "message-row",
        message.expanded ? "expanded" : "",
        message.payload.invalidJson ? "invalid-json" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <button
        className={`message-row-summary ${message.key ? "has-key" : "no-key"}`}
        type="button"
        aria-expanded={message.expanded}
        onClick={stopEvent(onToggle)}
      >
        <span className="message-toggle" aria-hidden="true">
          {message.expanded ? "-" : "+"}
        </span>
        <span className="message-meta">{message.receiveTime}</span>
        <span className="message-meta">
          p{message.partition} / {message.offset}
        </span>
        {message.key ? (
          <span className="message-key">
            key {renderHighlightedText(message.key, filter)}
          </span>
        ) : null}
        <span className="message-preview">
          {message.payload.marker ? (
            <span className="message-marker">{message.payload.marker}</span>
          ) : null}
          <code>{renderHighlightedText(message.payload.preview, filter)}</code>
          {message.payload.previewTruncated ? (
            <span className="message-marker">truncated</span>
          ) : null}
        </span>
      </button>

      {message.expanded ? (
        <div className="message-expanded">
          <pre>{renderHighlightedText(message.payload.content, filter)}</pre>
          {message.payload.contentTruncated ? (
            <span className="message-marker">payload truncated</span>
          ) : null}
          <section className="message-headers" aria-label="Kafka headers">
            <span className="eyebrow">Headers</span>
            {message.headers.length === 0 ? (
              <p>No headers</p>
            ) : (
              <dl>
                {message.headers.map((header, index) => (
                  <div key={`${header.key}-${index}`}>
                    <dt>{header.key}</dt>
                    <dd>{formatHeaderValue(header.value)}</dd>
                  </div>
                ))}
              </dl>
            )}
          </section>
        </div>
      ) : null}
    </article>
  );
}

function renderHighlightedText(text: string, filter: string): ReactNode {
  const query = filter.trim();
  if (!query) {
    return text;
  }

  const lowerText = text.toLocaleLowerCase();
  const lowerQuery = query.toLocaleLowerCase();
  const parts: ReactNode[] = [];
  let cursor = 0;
  let matchIndex = lowerText.indexOf(lowerQuery);

  while (matchIndex !== -1) {
    if (matchIndex > cursor) {
      parts.push(text.slice(cursor, matchIndex));
    }

    const matchEnd = matchIndex + query.length;
    parts.push(
      <mark className="message-match" key={`${matchIndex}-${matchEnd}`}>
        {text.slice(matchIndex, matchEnd)}
      </mark>,
    );
    cursor = matchEnd;
    matchIndex = lowerText.indexOf(lowerQuery, cursor);
  }

  if (cursor < text.length) {
    parts.push(text.slice(cursor));
  }

  return parts;
}

function TopicContextMenu({
  menu,
  panes,
  onOpen,
}: {
  menu: NonNullable<TopicMenuState>;
  panes: WorkspacePane[];
  onOpen: (placement: TopicOpenPlacement) => void;
}) {
  return (
    <div
      className="topic-menu"
      role="menu"
      style={{ left: menu.x, top: menu.y }}
      onClick={(event) => event.stopPropagation()}
    >
      <header>
        <span className="eyebrow">Topic actions</span>
        <strong>{menu.topic}</strong>
      </header>
      {panes.length === 0 ? (
        <div className="topic-menu-row" role="none">
          <span>First tab</span>
          <button type="button" role="menuitem" onClick={() => onOpen("selected")}>
            Open
          </button>
        </div>
      ) : (
        <>
          <div className="topic-menu-row" role="none">
            <span>Focused group</span>
            <button
              type="button"
              role="menuitem"
              onClick={() => onOpen("selected")}
            >
              Open in focused group
            </button>
          </div>
          <div className="topic-menu-row" role="none">
            <span>New group</span>
            <div className="topic-menu-actions">
              <button
                type="button"
                role="menuitem"
                onClick={() => onOpen("right")}
              >
                New group right
              </button>
              <button
                type="button"
                role="menuitem"
                onClick={() => onOpen("bottom")}
              >
                New group bottom
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function formatGroupSummary(tabCount: number) {
  if (tabCount === 0) {
    return "no tabs";
  }

  return tabCount === 1 ? "1 tab" : `${tabCount} tabs`;
}

function formatWorkspaceContext(context: WorkspaceContext) {
  const groupIdentity =
    context.activeGroupId === null ? "No group" : `Group ${context.activeGroupId}`;
  const groupCount =
    context.groupCount === 0
      ? "no groups"
      : context.groupCount === 1
        ? "1 group"
        : `${context.groupCount} groups`;

  return `${groupIdentity} / ${groupCount} / ${formatGroupSummary(context.tabCount)}`;
}

function formatActivityScope(event: GlobalActivityEntry) {
  return event.paneId ? `Tab ${event.paneId}` : event.source;
}

function topicRailStatus(status: string, topicCount: number) {
  if (status === "loading") {
    return "Listing topics";
  }

  if (status === "error") {
    return "Topic list unavailable";
  }

  return `${topicCount} topics / manual refresh`;
}

function isKafkaRecordEvent(
  event: MilenaBoundaryEvent,
): event is Extract<MilenaBoundaryEvent, { event: "kafkaRecord" }> {
  return event.event === "kafkaRecord";
}

function formatHeaderValue(value: string | null | undefined): string {
  return value ?? "(null)";
}

function publisherReadinessLabel(
  pane: WorkspacePane,
  publisher: PublisherPaneState,
  validationError: string | null,
) {
  if (publisher.error) {
    return publisher.error;
  }

  if (validationError) {
    return validationError;
  }

  if (!pane.topic) {
    return "Assign a topic before publishing";
  }

  if (pane.mode !== "poll" || pane.status !== "ready") {
    return "Start polling before publishing";
  }

  if (publisher.ack) {
    return `ack at ${new Date(publisher.ack.sentAt).toLocaleTimeString()}`;
  }

  return "Ready to publish";
}

function paneSessionButtonState(pane: WorkspacePane): {
  action: "poll" | "stop";
  label: "Poll" | "Stop";
} {
  if (pane.mode === "poll" && (pane.status === "loading" || pane.status === "ready")) {
    return { action: "stop", label: "Stop" };
  }

  return { action: "poll", label: "Poll" };
}

function compactPaneStatusLabel(pane: WorkspacePane): string {
  if (pane.mode === "poll" && pane.status === "loading") {
    return "Starting";
  }

  if (pane.mode === "poll" && pane.status === "ready") {
    return "Polling";
  }

  return pane.status;
}

function toOnboardingEnvironment(
  environment: SavedEnvironment,
): OnboardingEnvironment {
  return {
    name: environment.name,
    brokers: environment.brokers,
    authMode: environment.authMode,
    username: environment.username ?? "",
    advancedPropertiesText: environment.advancedProperties,
  };
}

function onboardingEnvironmentSignature(environment: OnboardingEnvironment): string {
  return JSON.stringify({
    name: environment.name,
    brokers: environment.brokers,
    authMode: environment.authMode,
    username: environment.username,
    advancedPropertiesText: environment.advancedPropertiesText,
  });
}

function connectionSuccessMessage(topicCount: number): string {
  return `Connection OK: ${pluralizeTopics(topicCount)}`;
}

function pluralizeTopics(topicCount: number): string {
  return `${topicCount} ${topicCount === 1 ? "topic" : "topics"}`;
}

function onboardingFormSignature(form: NonNullable<OnboardingState["form"]>): string {
  return JSON.stringify({
    mode: form.mode,
    originalName: form.originalName,
    originalAuthMode: form.originalAuthMode,
    values: form.values,
  });
}

function authModeLabel(authMode: EnvironmentAuthMode): string {
  if (authMode === "saslSslPlain") {
    return "SASL_SSL PLAIN";
  }
  return authMode === "saslSslScramSha512" ? "SASL_SSL SCRAM" : "PLAINTEXT";
}

function isSaslAuthMode(authMode: EnvironmentAuthMode): boolean {
  return authMode === "saslSslPlain" || authMode === "saslSslScramSha512";
}

function errorMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error) {
    return cause.message;
  }
  if (isMilenaCommandError(cause)) {
    return cause.message;
  }
  return fallback;
}

function isMilenaCommandError(cause: unknown): cause is MilenaCommandError {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "message" in cause &&
    typeof cause.message === "string"
  );
}

function isEditableShortcutTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) {
    return false;
  }

  return (
    target.isContentEditable ||
    target.tagName === "INPUT" ||
    target.tagName === "SELECT" ||
    target.tagName === "TEXTAREA"
  );
}

function readLastSelectedEnvironmentName(): string | null {
  if (typeof window === "undefined") {
    return null;
  }

  return window.localStorage.getItem(LAST_SELECTED_ENVIRONMENT_KEY);
}

function rememberLastSelectedEnvironment(environmentName: string) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(LAST_SELECTED_ENVIRONMENT_KEY, environmentName);
}

function forgetLastSelectedEnvironment() {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.removeItem(LAST_SELECTED_ENVIRONMENT_KEY);
}

function getTopicPinStore(): TopicPinStore | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

function getMessageRenderPreferenceStore():
  | MessageRenderPreferenceStore
  | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

function stopEvent(action: () => void) {
  return (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    action();
  };
}

export default App;
