import { readFileSync, writeFileSync } from "node:fs";

const VERSION_FILES = {
  packageJson: "package.json",
  packageLock: "package-lock.json",
  tauriConfig: "src-tauri/tauri.conf.json",
  cargoToml: "src-tauri/Cargo.toml",
};

function parseArgs(argv) {
  const options = {};

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--event" && next) {
      options.eventPath = next;
      index += 1;
    } else if (arg === "--bump" && next) {
      options.bump = next.toLowerCase();
      index += 1;
    } else {
      throw new Error("Usage: node scripts/bump-version.mjs [--event path] [--bump major|minor|patch]");
    }
  }

  return options;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function labelsFromEvent(path) {
  const event = readJson(path);
  return (event.pull_request?.labels ?? [])
    .map((label) => label.name)
    .filter((name) => typeof name === "string");
}

function bumpTypeForLabels(labels) {
  const normalized = new Set(labels.map((label) => label.trim().toLowerCase()));

  if (normalized.has("major")) {
    return "major";
  }

  if (normalized.has("patch") || normalized.has("hotfix") || normalized.has("patch/hotfix")) {
    return "patch";
  }

  return "minor";
}

function nextVersion(version, bump) {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);

  if (!match) {
    throw new Error(`Expected package version to use x.y.z format, got "${version}"`);
  }

  const [, majorText, minorText, patchText] = match;
  const major = Number(majorText);
  const minor = Number(minorText);
  const patch = Number(patchText);

  if (bump === "major") {
    return `${major + 1}.0.0`;
  }

  if (bump === "minor") {
    return `${major}.${minor + 1}.0`;
  }

  if (bump === "patch") {
    return `${major}.${minor}.${patch + 1}`;
  }

  throw new Error(`Unsupported bump type "${bump}"`);
}

function updateCargoTomlVersion(path, version) {
  const source = readFileSync(path, "utf8");
  const updated = source.replace(
    /(^\[package\][\s\S]*?^version\s*=\s*)"[^"]+"/m,
    `$1"${version}"`,
  );

  if (updated === source) {
    throw new Error(`Could not find [package] version in ${path}`);
  }

  writeFileSync(path, updated);
}

function bumpVersionFiles(options) {
  const packageJson = readJson(VERSION_FILES.packageJson);
  const bump =
    options.bump ??
    bumpTypeForLabels(labelsFromEvent(options.eventPath ?? process.env.GITHUB_EVENT_PATH ?? ""));
  const version = nextVersion(packageJson.version, bump);

  packageJson.version = version;
  writeJson(VERSION_FILES.packageJson, packageJson);

  const packageLock = readJson(VERSION_FILES.packageLock);
  packageLock.version = version;
  if (packageLock.packages?.[""]) {
    packageLock.packages[""].version = version;
  }
  writeJson(VERSION_FILES.packageLock, packageLock);

  const tauriConfig = readJson(VERSION_FILES.tauriConfig);
  tauriConfig.version = version;
  writeJson(VERSION_FILES.tauriConfig, tauriConfig);

  updateCargoTomlVersion(VERSION_FILES.cargoToml, version);

  return { bump, version };
}

try {
  const result = bumpVersionFiles(parseArgs(process.argv.slice(2)));
  console.log(`Bumped ${result.bump} version to ${result.version}`);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
