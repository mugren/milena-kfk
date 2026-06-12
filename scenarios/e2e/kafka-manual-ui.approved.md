<!-- approved-scenario
{
  "id": "kafka-manual-ui",
  "summary": "Manual desktop UI validation for issue #14 pane and global feedback surfaces.",
  "commands": []
}
-->

# Kafka Manual UI E2E

## Input Data

- Local broker is running after `npm run kafka:up`.
- PLAIN and SCRAM smoke checks have passed.
- Desktop app is running with `npm run tauri:dev`.
- Environment uses `localhost:19092` and the local CA at
  `docker/kafka/generated/ssl/ca.crt`.

## Expected Output

- Loading the PLAIN environment lists deterministic issue #14 topics.
- Polling `milena.issue14.records` starts only after explicit user action and
  the pane is empty until records are produced.
- Publishing `scripts/kafka-fixtures/order-created.json` without a key reports
  delivered status separately from consumer feedback.
- Publishing `scripts/kafka-fixtures/order-updated.json` with a key reports
  delivered status with topic, partition, and offset.
- Produced records appear in the active polling pane when they are created after
  the session starts.
- Stopping polling shows the stop lifecycle and cleanup attempt.
- Repeating the environment with SCRAM credentials lists topics and supports the
  same publish/poll workflow.
- A wrong password, wrong broker port, or bad CA path surfaces an error in the
  relevant pane or global activity surface without making the app look active.

## Validation Rules

- Run `npm run e2e:scenarios -- show kafka-manual-ui` before testing.
- Follow the expected output list as a checklist.
- Record manual pass/fail notes in the issue or PR.
- No generated actual file is required beyond the runner's manual placeholder.
