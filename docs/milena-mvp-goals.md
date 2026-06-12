# Milena MVP Goals

## Purpose

Milena MVP is a focused macOS developer desktop app for tactical Kafka work. It
replaces the repeated local workflow of shell scripts, manually wired auth
files, and separate terminal windows with one native workspace for connecting to
a Kafka environment, finding a topic, polling new messages, publishing validated
JSON, and seeing feedback in context.

The MVP should prove that Milena can make the common Kafka debugging loop fast
and predictable while avoiding surprising broker activity.

## MVP Outcome

The first usable version should let a Kafka developer:

- Save or import one Kafka environment and reconnect without retyping broker and
  auth details.
- Use `auth.properties`-style configuration with `$KAFKA_USER` and
  `$KAFKA_PASS` substitution.
- Store secrets outside plain text app config.
- Open one environment at a time and list topics on demand.
- Search, filter, and pin topics per environment.
- Start explicit polling sessions that show only records produced after the
  session starts.
- Publish validated JSON with an optional Kafka message key.
- See producer acknowledgements separately from consumer feedback.
- Work across one, two, or four panes without automatic session restoration or
  implicit consumers.
- Diagnose local pane failures and repeated global failures from visible error
  and activity surfaces.

## Product Principles

- Keep the product boundary narrow: environment, topics, polling, publishing,
  panes, and visible feedback.
- Avoid surprising Kafka activity. Consumers start only after explicit user
  action, split panes are empty, and sessions reset after restart.
- Preserve useful behavior from the existing `kafka-shell` reference where it
  affects environment loading, auth properties, and variable substitution.
- Prefer native app behavior over shelling out. Kafka CLI behavior is a
  reference, not the primary implementation path.
- Make failures local and recoverable. One failed pane should not block other
  panes.

## Platform And Architecture

- Milena MVP targets macOS local developer builds only.
- The app uses Tauri v2 with a React and TypeScript frontend and a Rust backend.
- Kafka operations use a native Rust Kafka client.
- The Rust command/event boundary is the main contract between frontend state
  and backend Kafka behavior.
- Environment metadata and secrets are stored separately.
- Passwords use macOS Keychain for the MVP.
- The environment model should remain portable enough to support Windows
  Credential Manager and Linux Secret Service later.

## Environment Goals

Milena should support creating, saving, loading, and importing Kafka
environments. Environment metadata includes:

- Name.
- Brokers.
- Username.
- Raw `auth.properties` template.

Runtime auth config is produced by:

1. Loading structured environment metadata.
2. Fetching the saved password.
3. Substituting `$KAFKA_USER` and `$KAFKA_PASS`.
4. Parsing properties.
5. Applying supported properties to the native Kafka client.

First-class MVP auth support is SASL_SSL with PLAIN and SCRAM. Other
property-based auth is advanced and unverified.

Milena uses its own app config format as source of truth. A one-time importer
for `kafka-shell`-style environment files is part of the MVP migration path.

## Topic Goals

Opening an environment lists topics once. Topic refresh after that is manual so
Milena does not perform background broker calls without user intent.

Topic behavior:

- Search and filtering happen client-side.
- Pinned topics persist per environment.
- Pinned topics appear at the top of the sidebar.
- Topic render preferences persist per environment and topic.
- Default message render mode is JSON.
- Raw render mode is available for non-JSON topics.

## Workspace Goals

The main workspace supports one, two, or four panes. A pane can be split right
or top. Newly split panes start empty and must not inherit Kafka activity from
the original pane.

Active sessions reset after restart and are not auto-reconnected. Empty panes
should clearly communicate that no Kafka consumer is active.

## Polling Goals

Polling sessions are explicit, pane-scoped Kafka consumers.

Polling behavior:

- Each session uses a unique consumer group.
- Temporary consumer groups use a recognizable Milena prefix.
- Sessions consume only records produced after the consumer starts.
- Consumer group cleanup is best effort on normal stop and is not required for
  correctness.
- Each polling pane retains the latest 1,000 messages.
- JSON parse failures render inline as raw text with an invalid JSON marker.
- Large payloads are capped and marked as truncated.

Message rows should show:

- Receive time.
- Topic.
- Partition.
- Offset.
- Optional key.
- Compact payload preview.

Expanded message rows should show the full rendered payload, within the payload
cap, and headers.

## Publishing Goals

Publishing opens a combined poll and publish pane by default. The poll session
starts before send is enabled so the user can often see the feedback record
after publishing.

Publisher behavior:

- Default publishing mode is JSON.
- Format JSON is available as a draft action before sending.
- Invalid JSON is blocked before send.
- Optional Kafka message keys are supported.
- Producer acknowledgement and status are shown separately from consumer
  feedback.

Publisher headers and raw publishing mode are deferred.

## Error And Activity Goals

Milena should show errors at the level where action is possible:

- Pane-level errors for failed pane actions.
- Producer status for publish failures and acknowledgements.
- Clearable global activity and error log for repeated auth, broker, or client
  failures.

A failing pane must not block unaffected sessions or panes.

## Testing Goals

Tests should verify external behavior at product seams rather than private
implementation details.

Expected coverage:

- Environment creation, loading, keychain password reference, auth template
  substitution, and import behavior.
- Kafka adapter config parsing, supported auth mapping, topic listing,
  publishing, consumer lifecycle, and cleanup attempts.
- Consumer latest-only startup semantics, unique group IDs, stop behavior,
  bounded message retention, and non-fatal render failures.
- Publisher JSON validation, formatting, optional key handling, and visible
  acknowledgement/error reporting.
- Workspace pane layout changes, empty split behavior, session reset on restart,
  topic pinning, topic render preferences, and topic filtering.
- UI behavior for topic actions, combined publish and poll flow, local pane
  errors, and global log entries.

Manual integration testing should use a real or containerized Kafka broker with
SASL_SSL SCRAM/PLAIN before the MVP is considered usable. The highest-value
test seam is the Rust command/event boundary because it exercises the app-level
contract without over-coupling tests to UI internals or Kafka client internals.

## Out Of Scope

- Windows and Linux builds.
- Signed and notarized macOS releases.
- Kafka CLI execution as the primary implementation.
- Consumer group browser.
- Lag view.
- Partition and offset explorer.
- Topic config inspection.
- Schema registry integration.
- Avro, Protobuf, or schema-aware decoding.
- Publish headers.
- Raw publishing mode.
- Auto-refreshing topic list.
- Auto-restoring active sessions after app restart.
- Guaranteed support for SSL cert files, OAuth/OIDC, IAM, or other advanced auth
  flows.
- Multi-environment workspaces in the same window.
