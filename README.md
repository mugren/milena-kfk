# Milena

Milena is a local macOS developer desktop app for tactical Kafka workflows. The
MVP focuses on opening one environment, listing and pinning topics, explicitly
polling records produced after a session starts, publishing validated JSON, and
keeping pane-level and global feedback visible.

## Local Development

Prerequisites:

- Node.js and npm
- Rust stable toolchain
- Xcode Command Line Tools
- Docker Compose for local Kafka smoke checks
- macOS for the local desktop developer build

Install dependencies:

```sh
npm install
```

Run the web frontend only:

```sh
npm run dev
```

Run the local Tauri developer app:

```sh
npm run tauri:dev
```

Build the unsigned local macOS app bundle:

```sh
npm run tauri:build
```

The expected artifact is
`src-tauri/target/release/bundle/macos/Milena.app`.

Build the frontend:

```sh
npm run build
```

Run Rust tests for the backend boundary:

```sh
cd src-tauri
cargo test
```

Run the local Kafka integration broker and issue #14 HITL verification checks:

```sh
npm run kafka:up
npm run kafka:check
```

Run approved E2E scenarios and review the generated actual Markdown:

```sh
npm run e2e:scenarios -- list
npm run e2e:scenarios -- run kafka-all
```

See [docs/kafka-integration.md](docs/kafka-integration.md) for the full local
broker runbook, auth profiles, Rust integration test env vars, and cleanup
commands.

See [docs/local-macos-dev-build.md](docs/local-macos-dev-build.md) for the
complete unsigned macOS build path, runtime prerequisites, and MVP smoke
checklist.

## MVP Scope Boundaries

The MVP local build intentionally excludes signing, notarization, DMG or PKG
distribution, App Store distribution, auto-update, Windows builds, and Linux
builds. Kafka CLI behavior is useful for local verification, but Milena's app
path uses the native Rust Kafka client behind the Tauri command/event boundary.

Other deferred product areas include schema registry, Avro, Protobuf, publish
headers, lag views, partition explorers, topic config inspection, automatic
topic refresh, automatic session restore, and multi-environment workspaces.
