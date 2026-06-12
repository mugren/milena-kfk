<!-- approved-scenario
{
  "id": "kafka-all",
  "summary": "Run the full issue #14 Kafka validation flow for PLAIN and SCRAM.",
  "commands": [
    {
      "name": "Start local Kafka broker and deterministic topics",
      "argv": ["npm", "run", "kafka:up"]
    },
    {
      "name": "Run all Kafka smoke and Rust integration checks",
      "argv": ["npm", "run", "kafka:check"],
      "expectedMarkers": [
        "Kafka smoke check passed for PLAIN",
        "Kafka smoke check passed for SCRAM-SHA-512",
        "running Rust Kafka integration tests against localhost:19092 using PLAIN",
        "running Rust Kafka integration tests against localhost:19092 using SCRAM-SHA-512",
        "test result: ok"
      ]
    }
  ]
}
-->

# Kafka All Profiles E2E

## Input Data

- Local Docker Compose broker from `docker-compose.kafka.yml`.
- Deterministic issue #14 topics from `docker/kafka/scripts/create-topics.sh`.
- PLAIN and SCRAM local development credentials from
  `docs/kafka-integration.md`.
- The public Rust command/event boundary exercised by
  `src-tauri/tests/kafka_integration.rs`.

## Expected Output

- PLAIN smoke check passes.
- SCRAM smoke check passes.
- PLAIN Rust integration tests pass.
- SCRAM Rust integration tests pass.
- The combined flow covers topic listing, latest-only polling, validated JSON
  publishing with and without keys, temporary consumer group cleanup reporting,
  command-boundary error surfaces, and CLI-to-Milena interop.

## Validation Rules

- Run `npm run e2e:scenarios -- run kafka-all`.
- The command exits zero.
- Review `.tmp/e2e-scenarios/kafka-all.actual.md`.
- Confirm each command exit code is `0`.
- Confirm all declared marker checks are `PASS`.
- Use this scenario before marking issue #14 complete locally.
