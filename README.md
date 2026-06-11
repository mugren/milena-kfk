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

This scaffold intentionally does not include signing, notarization, Windows, or
Linux packaging work. The Rust command boundary currently exposes a minimal
typed preview session and event channel only; Kafka behavior belongs to later
feature issues.
