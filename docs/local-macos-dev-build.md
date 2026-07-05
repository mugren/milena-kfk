# Local macOS Developer Build

Milena's MVP build path is a local, unsigned macOS developer build. It is meant
for contributors validating the narrow Kafka workflow on their own machine, not
for distribution.

## Prerequisites

- macOS.
- Node.js 24 LTS and npm.
- Rust stable toolchain with Cargo.
- Xcode Command Line Tools.
- Docker Desktop or another Docker Compose runtime for Kafka smoke checks.

Install project dependencies from the repository root:

```sh
npm install
```

## Run The Developer App

Start Milena in Tauri development mode:

```sh
npm run tauri:dev
```

This starts the Vite dev server through Tauri and opens the local desktop app.
Use this path for normal UI and Kafka workflow validation while developing.

## Build The Unsigned App Bundle

Build the frontend, Rust backend, and local macOS `.app` bundle:

```sh
npm run tauri:build
```

The script runs `tauri build --bundles app`. The expected local artifact is:

```text
src-tauri/target/release/bundle/macos/Milena.app
```

Run that app locally from Finder or with:

```sh
open src-tauri/target/release/bundle/macos/Milena.app
```

If macOS blocks the app because it is unsigned, open it from Finder with the
normal local developer override. Do not add signing or notarization to the MVP
build path.

## Fast Verification Commands

Run focused checks before a manual smoke pass:

```sh
npm run test
npm run build
cd src-tauri
cargo test
```

Use the local Kafka broker only when validating broker behavior:

```sh
npm run kafka:up
npm run kafka:check
```

See [kafka-integration.md](kafka-integration.md) for auth profiles, broker
cleanup, opt-in Rust integration tests, and approved E2E scenarios.

## MVP Smoke Checklist

Use this checklist against `npm run tauri:dev` first, then repeat against the
built `Milena.app` when validating the local bundle.

- Open one local Kafka environment that uses the local broker at
  `localhost:19092`.
- List topics after opening the environment, then manually refresh topics.
- Search for a deterministic topic such as `milena.issue14.records` and pin it.
- Start an explicit poll session in one pane and confirm the pane starts empty.
- Publish validated JSON from `scripts/kafka-fixtures/order-created.json`
  without a key.
- Publish validated JSON from `scripts/kafka-fixtures/order-updated.json` with
  a key such as `issue-15-smoke-1`.
- Confirm producer acknowledgement shows topic, partition, offset, and delivered
  status separately from consumed records.
- Confirm records produced after polling starts appear in the polling pane.
- Split the workspace to two panes and four panes; confirm new panes are empty
  and do not inherit active polling.
- Stop polling and confirm the stop or cleanup result is visible in pane or
  activity feedback.
- Enter a bad password or broker port and confirm the error appears at the
  relevant pane or global activity surface without blocking other panes.

## Out Of Scope

The MVP local build path intentionally excludes:

- Signed and notarized macOS releases.
- DMG, PKG, App Store, auto-update, or installer distribution.
- Windows or Linux builds.
- Kafka CLI as Milena's primary implementation path.
- Schema registry, Avro, Protobuf, publish headers, lag views, partition
  explorers, topic config inspection, automatic topic refresh, automatic session
  restore, and multi-environment workspaces.
