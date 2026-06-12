#!/usr/bin/env node

import { spawn } from "node:child_process";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const scenarioDir = path.join(rootDir, "scenarios", "e2e");
const actualDir = path.join(rootDir, ".tmp", "e2e-scenarios");

const command = process.argv[2] ?? "list";
const selector = process.argv[3] ?? "all";

const usage = `Usage:
  npm run e2e:scenarios -- list
  npm run e2e:scenarios -- show <scenario-id>
  npm run e2e:scenarios -- run <scenario-id|all>

Approved scenario files live in scenarios/e2e/*.approved.md.
Actual run reports are written to .tmp/e2e-scenarios/*.actual.md.`;

async function main() {
  const scenarios = await loadScenarios();

  if (command === "list") {
    for (const scenario of scenarios) {
      console.log(`${scenario.id}\t${scenario.summary}`);
    }
    return;
  }

  if (command === "show") {
    const scenario = findScenario(scenarios, selector);
    console.log(scenario.markdown);
    return;
  }

  if (command === "run") {
    const selected =
      selector === "all" ? scenarios : [findScenario(scenarios, selector)];
    let failed = false;
    await mkdir(actualDir, { recursive: true });

    for (const scenario of selected) {
      const result = await runScenario(scenario);
      if (!result.ok) {
        failed = true;
      }
    }

    if (failed) {
      process.exitCode = 1;
    }
    return;
  }

  console.error(usage);
  process.exitCode = 1;
}

async function loadScenarios() {
  const entries = await readdir(scenarioDir, { withFileTypes: true });
  const files = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".approved.md"))
    .map((entry) => path.join(scenarioDir, entry.name))
    .sort();

  const scenarios = [];
  for (const filePath of files) {
    const markdown = await readFile(filePath, "utf8");
    const config = parseScenarioConfig(markdown, filePath);
    const expectedId = path.basename(filePath, ".approved.md");
    if (config.id !== expectedId) {
      throw new Error(
        `${relative(filePath)} declares id '${config.id}', expected '${expectedId}'`,
      );
    }
    scenarios.push({
      ...config,
      filePath,
      markdown,
      commands: config.commands ?? [],
    });
  }

  return scenarios;
}

function parseScenarioConfig(markdown, filePath) {
  const match = markdown.match(
    /<!--\s*approved-scenario\s*([\s\S]*?)\s*-->/,
  );
  if (!match) {
    throw new Error(`${relative(filePath)} is missing approved-scenario metadata`);
  }

  try {
    return JSON.parse(match[1]);
  } catch (error) {
    throw new Error(
      `${relative(filePath)} has invalid approved-scenario JSON: ${error.message}`,
    );
  }
}

function findScenario(scenarios, id) {
  const scenario = scenarios.find((candidate) => candidate.id === id);
  if (!scenario) {
    const ids = scenarios.map((candidate) => candidate.id).join(", ");
    throw new Error(`unknown scenario '${id}'. Available scenarios: ${ids}`);
  }
  return scenario;
}

async function runScenario(scenario) {
  console.log(`\n==> Running approved scenario ${scenario.id}`);
  console.log(`    ${scenario.summary}`);

  if (scenario.commands.length === 0) {
    const report = renderManualReport(scenario);
    const actualPath = path.join(actualDir, `${scenario.id}.actual.md`);
    await writeFile(actualPath, report);
    console.log(`    manual scenario only; review ${relative(actualPath)}`);
    return { ok: true };
  }

  const commandResults = [];
  let ok = true;

  for (const step of scenario.commands) {
    const result = await runCommand(step);
    commandResults.push(result);

    if (result.exitCode !== 0 || result.missingMarkers.length > 0) {
      ok = false;
      break;
    }
  }

  const report = renderRunReport(scenario, commandResults, ok);
  const actualPath = path.join(actualDir, `${scenario.id}.actual.md`);
  await writeFile(actualPath, report);

  if (ok) {
    console.log(`==> Scenario ${scenario.id} passed`);
  } else {
    console.error(`==> Scenario ${scenario.id} failed`);
  }
  console.log(`    actual report: ${relative(actualPath)}`);

  return { ok };
}

async function runCommand(step) {
  console.log(`\n--> ${step.name}`);
  console.log(`    ${formatArgv(step.argv)}`);

  const startedAt = new Date();
  const outputChunks = [];
  const child = spawn(step.argv[0], step.argv.slice(1), {
    cwd: rootDir,
    env: { ...process.env, ...(step.env ?? {}) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(chunk);
    outputChunks.push(chunk);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(chunk);
    outputChunks.push(chunk);
  });

  const exitCode = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", resolve);
  });
  const finishedAt = new Date();
  const output = Buffer.concat(outputChunks).toString("utf8");
  const markers = step.expectedMarkers ?? [];
  const missingMarkers = markers.filter((marker) => !output.includes(marker));

  if (exitCode !== 0) {
    console.error(`    command exited with ${exitCode}`);
  }
  for (const marker of missingMarkers) {
    console.error(`    missing expected marker: ${marker}`);
  }

  return {
    ...step,
    exitCode,
    output,
    missingMarkers,
    startedAt,
    finishedAt,
  };
}

function renderManualReport(scenario) {
  return `# ${scenario.id} Actual

Approved scenario: \`${relative(scenario.filePath)}\`
Generated at: ${new Date().toISOString()}

This is a manual scenario. Review the approved file's validation rules and
record the checked result in your issue or PR notes.
`;
}

function renderRunReport(scenario, commandResults, ok) {
  const commandSections = commandResults
    .map((result) => {
      const markers = result.expectedMarkers ?? [];
      const markerLines =
        markers.length === 0
          ? "- No output markers declared."
          : markers
              .map((marker) => {
                const status = result.missingMarkers.includes(marker)
                  ? "FAIL"
                  : "PASS";
                return `- ${status}: \`${marker}\``;
              })
              .join("\n");

      return `## ${result.name}

Command: \`${formatArgv(result.argv)}\`
Started: ${result.startedAt.toISOString()}
Finished: ${result.finishedAt.toISOString()}
Exit code: ${result.exitCode}

### Marker Checks

${markerLines}

### Output

\`\`\`text
${trimOutput(result.output)}
\`\`\`
`;
    })
    .join("\n");

  return `# ${scenario.id} Actual

Approved scenario: \`${relative(scenario.filePath)}\`
Generated at: ${new Date().toISOString()}
Status: ${ok ? "PASS" : "FAIL"}

${commandSections}
`;
}

function trimOutput(output) {
  return output.trimEnd();
}

function formatArgv(argv) {
  return argv.map(shellQuote).join(" ");
}

function shellQuote(value) {
  if (/^[A-Za-z0-9_./:=@+-]+$/.test(value)) {
    return value;
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function relative(filePath) {
  return path.relative(rootDir, filePath);
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
