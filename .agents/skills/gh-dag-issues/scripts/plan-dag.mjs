#!/usr/bin/env node

import fs from "node:fs";

function usage() {
  console.error("Usage: node plan-dag.mjs <plan.json> [--markdown]");
  process.exit(2);
}

const file = process.argv[2];
const markdown = process.argv.includes("--markdown");
if (!file || file === "--markdown") usage();

let plan;
try {
  plan = JSON.parse(fs.readFileSync(file, "utf8"));
} catch (error) {
  console.error(`Failed to read JSON plan: ${error.message}`);
  process.exit(1);
}

const tasks = Array.isArray(plan.tasks) ? plan.tasks : null;
if (!tasks) {
  console.error('Plan must be an object with a "tasks" array.');
  process.exit(1);
}

const byId = new Map();
const errors = [];

for (const task of tasks) {
  if (!task || typeof task !== "object") {
    errors.push("Each task must be an object.");
    continue;
  }

  if (!task.id || typeof task.id !== "string") {
    errors.push(`Task "${task.title || "(untitled)"}" is missing a string id.`);
    continue;
  }

  if (byId.has(task.id)) errors.push(`Duplicate task id: ${task.id}`);
  byId.set(task.id, task);

  if (!task.title || typeof task.title !== "string") {
    errors.push(`Task "${task.id}" is missing a string title.`);
  }

  if (task.blocked_by && !Array.isArray(task.blocked_by)) {
    errors.push(`Task "${task.id}" blocked_by must be an array.`);
  }
}

for (const task of tasks) {
  if (!task || !task.id) continue;
  for (const dep of task.blocked_by || []) {
    if (dep === task.id) errors.push(`Task "${task.id}" depends on itself.`);
    if (!byId.has(dep)) errors.push(`Task "${task.id}" depends on missing task "${dep}".`);
  }
}

if (errors.length) {
  console.error(errors.map((error) => `- ${error}`).join("\n"));
  process.exit(1);
}

const visiting = new Set();
const visited = new Set();
const order = [];

function visit(id, stack) {
  if (visited.has(id)) return;
  if (visiting.has(id)) {
    const cycle = stack.slice(stack.indexOf(id)).join(" -> ");
    throw new Error(`Cycle detected: ${cycle}`);
  }

  visiting.add(id);
  const task = byId.get(id);
  for (const dep of task.blocked_by || []) visit(dep, [...stack, dep]);
  visiting.delete(id);
  visited.add(id);
  order.push(id);
}

try {
  for (const task of tasks) visit(task.id, [task.id]);
} catch (error) {
  console.error(error.message);
  process.exit(1);
}

const unblocks = new Map([...byId.keys()].map((id) => [id, []]));
for (const task of tasks) {
  for (const dep of task.blocked_by || []) unblocks.get(dep).push(task.id);
}

function listOrNone(values) {
  return values.length ? values.map((value) => `\`${value}\``).join(", ") : "None";
}

function acceptance(task) {
  const items = Array.isArray(task.acceptance) ? task.acceptance : [];
  return items.length ? items.map((item) => `- [ ] ${item}`).join("\n") : "- [ ] Outcome is verified.";
}

if (markdown) {
  for (const id of order) {
    const task = byId.get(id);
    console.log(`---\n# ${task.title}\n`);
    console.log("## Task\n");
    console.log(`${task.body || "Describe the completed behavior."}\n`);
    console.log("## Acceptance criteria\n");
    console.log(`${acceptance(task)}\n`);
    console.log("## DAG\n");
    console.log(`Task ID: \`${id}\``);
    console.log(`Type: ${task.type || "AFK"}`);
    console.log(`Source: ${task.source || "Unspecified"}`);
    console.log(`Blocked by: ${listOrNone(task.blocked_by || [])}`);
    console.log(`Unblocks: ${listOrNone(unblocks.get(id))}\n`);
    console.log("## Notes\n");
    console.log(`${task.notes || "None"}\n`);
  }
} else {
  console.log("DAG ok. Creation order:");
  order.forEach((id, index) => {
    const task = byId.get(id);
    console.log(`${index + 1}. ${id} - ${task.title}`);
  });
}
