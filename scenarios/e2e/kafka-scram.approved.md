<!-- approved-scenario
{
  "id": "kafka-scram",
  "summary": "Validate Milena's Kafka command boundary against the local SASL_SSL SCRAM broker profile.",
  "commands": [
    {
      "name": "Start local Kafka broker and deterministic topics",
      "argv": ["npm", "run", "kafka:up"]
    },
    {
      "name": "Smoke check SCRAM topic list, produce, consume, and group cleanup",
      "argv": ["npm", "run", "kafka:smoke", "--", "scram"],
      "expectedMarkers": ["Kafka smoke check passed for SCRAM-SHA-512"]
    },
    {
      "name": "Run Rust command-boundary integration tests with SCRAM auth",
      "argv": ["npm", "run", "kafka:test", "--", "scram"],
      "expectedMarkers": [
        "running Rust Kafka integration tests against localhost:19092 using SCRAM-SHA-512",
        "test result: ok"
      ]
    }
  ]
}
-->

# Kafka SCRAM E2E

## Input Data

- Local Docker Compose broker from `docker-compose.kafka.yml`.
- Deterministic topics from `docker/kafka/scripts/create-topics.sh`.
- Host bootstrap server: `localhost:19092`.
- Auth profile: SASL_SSL with SCRAM-SHA-512 user `milena_scram`.
- Fixture payloads under `scripts/kafka-fixtures/` and
  `src-tauri/tests/fixtures/kafka/`.

## Expected Output

- Topic listing succeeds and excludes Kafka internal topics from Milena
  metadata.
- The smoke step creates/verifies the focused topic, produces one JSON fixture,
  consumes it with a temporary group, and attempts group deletion.
- Publishing valid JSON without a key and with a key returns delivered ack
  status with topic, partition, and offset.
- Polling starts at latest and receives records produced after session start,
  while ignoring pre-session records.
- Stop-session output includes the lifecycle stop event and cleanup attempt.
- Bad credentials, unavailable broker, bad CA path, blank topic, empty payload,
  blank consumer topic, and non-existent topic publish all surface command
  errors without hanging.
- CLI-produced records are visible to Milena through the native adapter and
  public command boundary.

## Validation Rules

- Run `npm run e2e:scenarios -- run kafka-scram`.
- The command exits zero.
- Review `.tmp/e2e-scenarios/kafka-scram.actual.md`.
- Confirm each command exit code is `0`.
- Confirm all declared marker checks are `PASS`.
- Keep the generated actual file out of git; it is a local review artifact.
