---
name: gh-dag-issues
description: Turn a prompt, PRD, or to-issues breakdown into DAG todos that can be published as GitHub issues with explicit dependency metadata. Use when the user wants GitHub issues for tasks, todos, blockers, unblockers, dependency graphs, topological execution plans, PRD implementation plans, or DAG-based project tracking.
---

# GitHub DAG Issues

Turn a source request into GitHub-ready DAG todos. Each todo should be independently actionable, dependency-aware, and publishable as an issue once the graph is approved.

## Quick start

1. Identify the source: user prompt, PRD/path, or `to-issues` output.
2. Convert it into a JSON DAG todo plan with stable local IDs.
3. Validate: `node .agents/skills/gh-dag-issues/scripts/plan-dag.mjs plan.json`.
4. Review the topological order with the user when the graph is non-trivial.
5. Publish blockers first with `gh issue create`, then replace local IDs with real `#123` refs.

## Accepted inputs

- **Prompt**: ask only for missing goals, constraints, or repo/project target; then draft DAG todos directly.
- **PRD/path**: read the full PRD, extract user stories, milestones, constraints, and acceptance criteria.
- **to-issues output**: treat each approved vertical slice as a todo node, preserve its HITL/AFK type, and carry over `Blocked by` relationships.

If the user passes a PRD such as `docs/prd-milena-mvp.md`, read it before drafting. With no structured source, create the smallest useful DAG and mark uncertain nodes as HITL.

## Task plan format

Use JSON for deterministic validation:

```json
{
  "tasks": [
    {
      "id": "auth-session-model",
      "title": "Model authenticated sessions",
      "body": "Create the smallest end-to-end task.",
      "acceptance": ["Session state is persisted", "Regression tests cover expiry"],
      "blocked_by": [],
      "labels": ["task"],
      "source": "PRD user story 2",
      "type": "AFK"
    }
  ]
}
```

`id` is a stable local slug used until issue numbers exist. `blocked_by` points to prerequisite task IDs. `source` records prompt, PRD section, or `to-issues` slice.

## Issue body template

Generated issue bodies must include `## Task`, `## Acceptance criteria`, `## DAG`, and `## Notes`. The DAG section must include `Task ID`, `Type`, `Source`, `Blocked by`, and `Unblocks`.

## Workflow

### 1. Extract

- From a prompt, infer todos and ask only when dependency or scope ambiguity would cause rework.
- From a PRD, map goals/user stories to vertical todos with acceptance criteria.
- From `to-issues`, reuse the approved slice list instead of re-breaking the plan.

### 2. Normalize

- Give every todo one clear outcome and a lowercase kebab-case `id`.
- Prefer small vertical tasks that can be verified independently.
- Record dependencies only when one task cannot start until another is complete.
- Preserve HITL/AFK markers in `type`; use HITL for decisions, design review, or missing product calls.

### 3. Validate

- Run `plan-dag.mjs` before publishing.
- Fix missing dependencies, duplicate IDs, self-dependencies, and cycles.
- Treat cycles as a design problem: split tasks, invert the dependency, or introduce a decision task.

### 4. Review

For any graph with more than three nodes, show the user:
- creation order
- each task title
- `blocked_by` IDs
- any HITL or decision tasks
Ask only for corrections to scope, granularity, or dependencies.

### 5. Publish

Use current `gh` issue commands. Create blockers first: `gh issue create --title "<title>" --body-file <body-file> --label task`.
Add assignees, milestones, project, and extra labels when the user or repo conventions call for them. Use `gh issue edit` to replace local IDs with real issue references after all issues are created.

### 6. Maintain

- When adding a new task, validate the updated plan before creating it.
- When closing a blocker, comment on or update directly unblocked issues if useful.
- Do not close parent/planning issues unless the user explicitly asks.
- Keep `Blocked by` and `Unblocks` symmetric whenever possible.

## Helper script

`scripts/plan-dag.mjs` validates a JSON task plan, detects cycles, and prints dependency-first creation order. Use `--markdown` to print draft issue bodies.
