# Kafka Integration Broker

This runbook covers the local-only Kafka broker and verification commands for
issue #14: validating Milena against a real or containerized Kafka broker.

The scripts expect Kafka compose infra at `docker-compose.kafka.yml` with
supporting files under `docker/kafka/**`. They do not create or modify that
infra. Override any assumption with the environment variables below if the
compose service names or ports differ.

## Commands

```sh
npm run kafka:up
npm run kafka:smoke
npm run kafka:smoke -- scram
npm run kafka:test
npm run kafka:down
```

Use `npm run kafka:reset` when you need a clean broker and empty compose
volumes. It deletes local Kafka compose volumes before starting the broker
again.

`npm run kafka:check` runs the PLAIN smoke check, the SCRAM smoke check, then
the Rust Kafka integration test command. It is intended for local HITL
verification, not normal CI.

## Script Defaults

| Variable | Default | Purpose |
| --- | --- | --- |
| `MILENA_KAFKA_COMPOSE_FILE` | `docker-compose.kafka.yml` | Compose file for the local broker |
| `MILENA_KAFKA_PROJECT_NAME` | `milena-kafka` | Compose project name |
| `MILENA_KAFKA_SERVICE` | `kafka` | Service that has Kafka CLI tools installed |
| `MILENA_KAFKA_SMOKE_BROKERS` | `kafka:9092` | Broker address used inside the compose network |
| `MILENA_KAFKA_BROKERS` | `localhost:19092` | Broker address Milena and Rust tests use from the host |
| `MILENA_KAFKA_TOPIC` | `milena.issue14.records` | Topic used by focused smoke and integration checks |
| `MILENA_KAFKA_FIXTURE` | `scripts/kafka-fixtures/order-created.json` | Payload produced by the smoke test |
| `MILENA_KAFKA_CLIENT_CONFIG` | profile-specific file under `/milena-kafka/config/clients` | Container path used by focused smoke checks |

`npm run kafka:smoke` uses the compose-side `kafka-smoke` service and verifies
both PLAIN and SCRAM. `npm run kafka:smoke -- plain` and
`npm run kafka:smoke -- scram` run a focused topic create, produce, consume, and
temporary group cleanup check from this support layer using the client property
files mounted into the Kafka container.

## Local Fixture Credentials

These are local development credentials only. Do not reuse them outside the
compose broker.

### PLAIN

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=milena_plain
sasl.password=milena-plain-secret
ssl.endpoint.identification.algorithm=
ssl.ca.location=/absolute/path/to/docker/kafka/generated/ssl/ca.crt
```

Run:

```sh
npm run kafka:smoke -- plain
```

### SCRAM

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=milena_scram
sasl.password=milena-scram-secret
ssl.endpoint.identification.algorithm=
ssl.ca.location=/absolute/path/to/docker/kafka/generated/ssl/ca.crt
```

Run:

```sh
npm run kafka:smoke -- scram
```

Milena and the Rust tests run from the host, so `ssl.ca.location` should be a
host path. Kafka CLI smoke checks run inside the compose service and use the
container-side client property files under `/milena-kafka/config/clients`.

## Broker Startup

Start the broker:

```sh
npm run kafka:up
```

Verify PLAIN auth, topic creation, produce, consume, and temporary group
cleanup:

```sh
npm run kafka:smoke -- plain
```

Verify SCRAM auth:

```sh
npm run kafka:smoke -- scram
```

The smoke check creates `MILENA_KAFKA_TOPIC` if needed, produces one JSON
fixture payload, consumes one record with a `milena-smoke-*` group, and attempts
to delete that temporary consumer group.

## Rust Integration Tests

Real broker tests should opt in and skip by default. Use these environment
variables for issue #14 tests:

```sh
export MILENA_KAFKA_INTEGRATION=1
export MILENA_KAFKA_BROKERS=localhost:19092
export MILENA_KAFKA_TOPIC=milena.issue14.records
export MILENA_KAFKA_SECURITY_PROTOCOL=SASL_SSL
export MILENA_KAFKA_SASL_MECHANISM=PLAIN
export MILENA_KAFKA_USERNAME=milena_plain
export MILENA_KAFKA_PASSWORD=milena-plain-secret
export MILENA_KAFKA_SSL_CA_LOCATION="$PWD/docker/kafka/generated/ssl/ca.crt"
```

Then run:

```sh
npm run kafka:test
```

Use SCRAM by overriding the auth values:

```sh
MILENA_KAFKA_SASL_MECHANISM=SCRAM-SHA-512 \
MILENA_KAFKA_USERNAME=milena_scram \
MILENA_KAFKA_PASSWORD=milena-scram-secret \
npm run kafka:test -- scram
```

Expected coverage for issue #14:

- Topic listing returns the configured `MILENA_KAFKA_TOPIC`.
- Polling starts with `auto.offset.reset=latest`; only records produced after
  session start are emitted.
- Publishing valid JSON without a key succeeds and reports topic, partition,
  offset, and `delivered` status.
- Publishing valid JSON with a key succeeds and reports the same ack fields.
- Stopping a poll session reports the temporary consumer group cleanup attempt.
- Bad credentials, unavailable broker, invalid topic, and invalid payload errors
  appear in the pane or global error surface.

## Manual Milena Checks

1. Start the broker with `npm run kafka:up`.
2. Run `npm run kafka:smoke -- plain` and `npm run kafka:smoke -- scram`.
3. Start the desktop app with `npm run tauri:dev`.
4. Create or select a local environment that uses `localhost:19092`.
5. Enter the PLAIN properties above and verify topic listing.
6. Start polling `milena.issue14.records`.
7. Publish `scripts/kafka-fixtures/order-created.json` without a key.
8. Publish `scripts/kafka-fixtures/order-updated.json` with a key such as
   `issue-14-smoke-1`.
9. Verify the publish status includes topic, partition, offset, and delivered
   status.
10. Stop polling and verify the cleanup attempt is visible.
11. Repeat auth setup with the SCRAM properties.
12. Change the password or broker port and verify the resulting error is visible
    in the relevant pane or global error surface.

## Cleanup

Stop the local broker without deleting data:

```sh
npm run kafka:down
```

Delete local Kafka compose volumes and restart with a clean broker:

```sh
npm run kafka:reset
```
