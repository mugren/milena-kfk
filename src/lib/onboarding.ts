export type OnboardingMode = "list" | "add" | "edit";
export type EnvironmentAuthMode = "plaintext" | "saslSslScramSha512";

export type OnboardingEnvironment = {
  name: string;
  brokers: string[];
  authMode: EnvironmentAuthMode;
  username: string;
  advancedPropertiesText: string;
};

export type OnboardingFormValues = {
  name: string;
  brokersText: string;
  authMode: EnvironmentAuthMode;
  username: string;
  password: string;
  advancedPropertiesText: string;
};

export type OnboardingFormState = {
  mode: "add" | "edit";
  originalName: string | null;
  originalAuthMode: EnvironmentAuthMode | null;
  values: OnboardingFormValues;
  errors: Partial<Record<keyof OnboardingFormValues, string>>;
  testResult: OnboardingTestResult | null;
};

export type OnboardingTestResult = {
  status: "success" | "error";
  message: string;
};

export type DeleteConfirmationState = {
  environmentName: string;
};

export type OnboardingState = {
  mode: OnboardingMode;
  environments: OnboardingEnvironment[];
  selectedEnvironmentName: string | null;
  lastSelectedEnvironmentName: string | null;
  activeRuntimeAuth: null;
  form: OnboardingFormState | null;
  deleteConfirmation: DeleteConfirmationState | null;
};

export type CreateOnboardingStateOptions = {
  environments: OnboardingEnvironment[];
  lastSelectedEnvironmentName?: string | null;
};

export type SaveOnboardingFormResult =
  | {
      ok: true;
      state: OnboardingState;
      environment: OnboardingEnvironment;
    }
  | {
      ok: false;
      state: OnboardingState;
      errors: OnboardingFormState["errors"];
    };

export type BrokerValidationResult =
  | { ok: true }
  | { ok: false; error: string };

export type BrokerListValidationResult =
  | { ok: true; brokers: string[] }
  | { ok: false; errors: string[] };

export type OnboardingFormValidationResult =
  | { ok: true; environment: OnboardingEnvironment }
  | { ok: false; errors: OnboardingFormState["errors"] };

export function createOnboardingState({
  environments,
  lastSelectedEnvironmentName = null,
}: CreateOnboardingStateOptions): OnboardingState {
  const sortedEnvironments = sortEnvironments(environments);
  const selectedEnvironmentName = selectExistingName(
    sortedEnvironments,
    lastSelectedEnvironmentName,
  );
  const firstRun = sortedEnvironments.length === 0;

  return {
    mode: firstRun ? "add" : "list",
    environments: sortedEnvironments,
    selectedEnvironmentName,
    lastSelectedEnvironmentName: selectedEnvironmentName,
    activeRuntimeAuth: null,
    form: firstRun ? createAddForm() : null,
    deleteConfirmation: null,
  };
}

export function startAddEnvironment(state: OnboardingState): OnboardingState {
  return {
    ...state,
    mode: "add",
    form: createAddForm(),
    deleteConfirmation: null,
  };
}

export function startEditEnvironment(
  state: OnboardingState,
  environmentName: string = state.selectedEnvironmentName ?? "",
): OnboardingState {
  const environment = findEnvironment(state.environments, environmentName);
  if (!environment) {
    return state;
  }

  return {
    ...state,
    mode: "edit",
    selectedEnvironmentName: environment.name,
    lastSelectedEnvironmentName: environment.name,
    form: createEditForm(environment),
    deleteConfirmation: null,
  };
}

export function selectEnvironment(
  state: OnboardingState,
  environmentName: string,
): OnboardingState {
  const environment = findEnvironment(state.environments, environmentName);
  if (!environment) {
    return state;
  }

  return {
    ...state,
    selectedEnvironmentName: environment.name,
    lastSelectedEnvironmentName: environment.name,
    activeRuntimeAuth: null,
    deleteConfirmation: null,
  };
}

export function getSelectedEnvironment(
  state: OnboardingState,
): OnboardingEnvironment | null {
  if (!state.selectedEnvironmentName) {
    return null;
  }

  return (
    findEnvironment(state.environments, state.selectedEnvironmentName) ?? null
  );
}

export function setOnboardingFormField<K extends keyof OnboardingFormValues>(
  state: OnboardingState,
  field: K,
  value: OnboardingFormValues[K],
): OnboardingState {
  if (!state.form) {
    return state;
  }
  if (state.form.mode === "edit" && field === "name") {
    return state;
  }
  if (state.form.values[field] === value) {
    return state;
  }

  return {
    ...state,
    form: {
      ...state.form,
      values: {
        ...state.form.values,
        [field]: value,
      },
      errors: {
        ...state.form.errors,
        [field]: undefined,
      },
      testResult: null,
    },
  };
}

export function setOnboardingFormTestResult(
  state: OnboardingState,
  testResult: OnboardingTestResult,
): OnboardingState {
  if (!state.form) {
    return state;
  }

  return {
    ...state,
    form: {
      ...state.form,
      testResult,
    },
  };
}

export function requestDeleteEnvironment(
  state: OnboardingState,
  environmentName: string = state.selectedEnvironmentName ?? "",
): OnboardingState {
  const environment = findEnvironment(state.environments, environmentName);
  if (!environment) {
    return state;
  }

  return {
    ...state,
    mode: "list",
    selectedEnvironmentName: environment.name,
    lastSelectedEnvironmentName: environment.name,
    form: null,
    deleteConfirmation: {
      environmentName: environment.name,
    },
  };
}

export function cancelDeleteEnvironment(
  state: OnboardingState,
): OnboardingState {
  return {
    ...state,
    deleteConfirmation: null,
  };
}

export function confirmDeleteEnvironment(
  state: OnboardingState,
): OnboardingState {
  if (!state.deleteConfirmation) {
    return state;
  }

  const environments = state.environments.filter(
    (environment) =>
      normalizeName(environment.name) !==
      normalizeName(state.deleteConfirmation?.environmentName ?? ""),
  );
  const selectedEnvironmentName = selectExistingName(
    environments,
    state.lastSelectedEnvironmentName,
  );

  return {
    ...state,
    mode: "list",
    environments,
    selectedEnvironmentName,
    lastSelectedEnvironmentName: selectedEnvironmentName,
    activeRuntimeAuth: null,
    form: null,
    deleteConfirmation: null,
  };
}

export function cancelOnboardingForm(state: OnboardingState): OnboardingState {
  return {
    ...state,
    mode: "list",
    form: null,
  };
}

export function saveOnboardingForm(
  state: OnboardingState,
): SaveOnboardingFormResult {
  if (!state.form) {
    return { ok: false, state, errors: {} };
  }

  const validation = validateOnboardingForm(state.form, state.environments);
  if (!validation.ok) {
    const nextState = {
      ...state,
      form: {
        ...state.form,
        errors: validation.errors,
      },
    };

    return {
      ok: false,
      state: nextState,
      errors: validation.errors,
    };
  }

  const environment: OnboardingEnvironment = {
    name: validation.environment.name,
    brokers: validation.environment.brokers,
    authMode: validation.environment.authMode,
    username: validation.environment.username,
    advancedPropertiesText: validation.environment.advancedPropertiesText,
  };
  const environments =
    state.form.mode === "edit"
      ? state.environments.map((candidate) =>
          candidate.name === state.form?.originalName ? environment : candidate,
        )
      : [...state.environments, environment];
  const selectedEnvironmentName = environment.name;

  return {
    ok: true,
    environment,
    state: {
      ...state,
      mode: "list",
      environments: sortEnvironments(environments),
      selectedEnvironmentName,
      lastSelectedEnvironmentName: selectedEnvironmentName,
      activeRuntimeAuth: null,
      form: null,
      deleteConfirmation: null,
    },
  };
}

export function validateBrokerEntry(entry: string): BrokerValidationResult {
  const broker = entry.trim();
  if (!broker) {
    return { ok: false, error: "Broker is required" };
  }
  if (broker.includes("://")) {
    return { ok: false, error: "Broker must not include a URL scheme" };
  }
  if (/[/?#]/.test(broker)) {
    return { ok: false, error: "Broker must not include a path" };
  }
  if (/\s/.test(broker)) {
    return { ok: false, error: "Broker must not include whitespace" };
  }

  const bracketedIpv6 = broker.match(/^\[([^\]]+)\]:(.+)$/);
  if (bracketedIpv6) {
    if (!isValidIpv6Address(bracketedIpv6[1])) {
      return { ok: false, error: "Broker IPv6 host is invalid" };
    }
    return validateBrokerPort(bracketedIpv6[2]);
  }

  if (broker.startsWith("[") || broker.includes("]")) {
    return { ok: false, error: "IPv6 brokers must be bracketed with a port" };
  }

  const separator = broker.lastIndexOf(":");
  if (separator === -1) {
    return { ok: false, error: "Broker port is required" };
  }
  const host = broker.slice(0, separator);
  const port = broker.slice(separator + 1);
  if (!host || host.includes(":")) {
    return { ok: false, error: "Broker host is invalid" };
  }
  if (!/^[A-Za-z0-9.-]+$/.test(host)) {
    return { ok: false, error: "Broker host is invalid" };
  }

  return validateBrokerPort(port);
}

export function validateBrokerList(
  brokersText: string,
): BrokerListValidationResult {
  const brokers = parseBrokerList(brokersText);
  if (brokers.length === 0) {
    return { ok: false, errors: ["At least one broker is required"] };
  }

  const errors = brokers.flatMap((broker) => {
    const validation = validateBrokerEntry(broker);
    return validation.ok ? [] : [`${broker}: ${validation.error}`];
  });

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  return { ok: true, brokers };
}

function createAddForm(): OnboardingFormState {
  return {
    mode: "add",
    originalName: null,
    originalAuthMode: null,
    values: {
      name: "",
      brokersText: "",
      authMode: "plaintext",
      username: "",
      password: "",
      advancedPropertiesText: "",
    },
    errors: {},
    testResult: null,
  };
}

function createEditForm(
  environment: OnboardingEnvironment,
): OnboardingFormState {
  return {
    mode: "edit",
    originalName: environment.name,
    originalAuthMode: environment.authMode,
    values: {
      name: environment.name,
      brokersText: environment.brokers.join(", "),
      authMode: environment.authMode,
      username: environment.username,
      password: "",
      advancedPropertiesText: environment.advancedPropertiesText,
    },
    errors: {},
    testResult: null,
  };
}

export function validateOnboardingForm(
  form: OnboardingFormState,
  environments: OnboardingEnvironment[],
): OnboardingFormValidationResult {
  const errors: OnboardingFormState["errors"] = {};
  const name =
    form.mode === "edit" && form.originalName
      ? form.originalName
      : form.values.name.trim();
  const brokers = validateBrokerList(form.values.brokersText);

  if (!name) {
    errors.name = "Environment name is required";
  }

  if (!brokers.ok) {
    errors.brokersText = brokers.errors[0];
  }

  if (
    form.mode === "add" &&
    environments.some(
      (environment) => normalizeName(environment.name) === normalizeName(name),
    )
  ) {
    errors.name = "Environment name already exists";
  }

  if (form.values.authMode === "saslSslScramSha512") {
    if (!form.values.username.trim()) {
      errors.username = "Username is required";
    }

    const keepsExistingScramPassword =
      form.mode === "edit" && form.originalAuthMode === "saslSslScramSha512";
    if (!keepsExistingScramPassword && !form.values.password) {
      errors.password = "Password is required";
    }
  }

  if (Object.keys(errors).length > 0) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    environment: {
      name,
      brokers: brokers.ok ? brokers.brokers : [],
      authMode: form.values.authMode,
      username: form.values.username.trim(),
      advancedPropertiesText: form.values.advancedPropertiesText,
    },
  };
}

function parseBrokerList(value: string): string[] {
  return value
    .split(/[,\n]/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function validateBrokerPort(port: string): BrokerValidationResult {
  if (!/^\d+$/.test(port)) {
    return { ok: false, error: "Broker port must be numeric" };
  }

  const numericPort = Number(port);
  if (numericPort < 1 || numericPort > 65_535) {
    return { ok: false, error: "Broker port must be between 1 and 65535" };
  }

  return { ok: true };
}

function isValidIpv6Address(value: string): boolean {
  if (!value.includes(":")) {
    return false;
  }
  if (!/^[0-9A-Fa-f:]+$/.test(value)) {
    return false;
  }
  if (value.includes(":::")) {
    return false;
  }
  if ((value.match(/::/g) ?? []).length > 1) {
    return false;
  }

  const hasCompressedZeroes = value.includes("::");
  const segments = value.split(":").filter((segment) => segment.length > 0);
  if (segments.some((segment) => !/^[0-9A-Fa-f]{1,4}$/.test(segment))) {
    return false;
  }

  return hasCompressedZeroes ? segments.length < 8 : segments.length === 8;
}

function sortEnvironments(
  environments: OnboardingEnvironment[],
): OnboardingEnvironment[] {
  return [...environments].sort((left, right) =>
    left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
  );
}

function selectExistingName(
  environments: OnboardingEnvironment[],
  preferredName: string | null,
): string | null {
  if (preferredName) {
    const preferred = environments.find(
      (environment) =>
        normalizeName(environment.name) === normalizeName(preferredName),
    );
    if (preferred) {
      return preferred.name;
    }
  }

  return environments[0]?.name ?? null;
}

function findEnvironment(
  environments: OnboardingEnvironment[],
  environmentName: string,
): OnboardingEnvironment | undefined {
  return environments.find(
    (environment) =>
      normalizeName(environment.name) === normalizeName(environmentName),
  );
}

function normalizeName(name: string): string {
  return name.trim().toLocaleLowerCase();
}
