# Milena

Milena is a local macOS developer desktop app for Kafka workflows. The initial
scaffold is a Tauri v2 application with a React and TypeScript frontend and a
Rust backend command/event boundary.

## Local Development

Prerequisites:

- Node.js and npm
- Rust stable toolchain
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

This scaffold intentionally does not include signing, notarization, Windows, or
Linux packaging work. The Rust command boundary currently exposes a minimal
typed preview session and event channel only; Kafka behavior belongs to later
feature issues.
