# Command and Event Harness

`command_event_harness.rs` exercises the app boundary through public request,
response, error, and event payload contracts. It uses a recording
`BoundaryEventEmitter`, so the default test path does not require a Kafka broker
or a running Tauri window.

Real broker integration tests should live in this `src-tauri/tests/` directory
beside the harness once Kafka-backed environment, publisher, or workspace
features are added. Those tests should opt in explicitly through an environment
variable such as `MILENA_KAFKA_INTEGRATION=1` and skip by default when no broker
is configured.

## Kafka broker integration

`kafka_integration.rs` is skipped unless `MILENA_KAFKA_INTEGRATION=1` is set.
It exercises the public command boundary with `NativeKafkaAdapter` against a
real SASL_SSL broker:

- topic listing
- valid JSON publish without a key
- valid JSON publish with a key
- latest-only consumer sessions receiving records produced after start
- stop-session cleanup attempt reporting
- bad auth and unavailable broker command errors

Run it from `src-tauri`:

```sh
MILENA_KAFKA_INTEGRATION=1 cargo test --test kafka_integration -- --nocapture
```

Defaults target the local issue #14 fixtures:

```text
MILENA_KAFKA_BROKERS=localhost:19092
MILENA_KAFKA_ENVIRONMENT=local-issue14
MILENA_KAFKA_TOPIC=milena.issue14.records
MILENA_KAFKA_USERNAME=milena_plain
MILENA_KAFKA_PASSWORD=milena-plain-secret
MILENA_KAFKA_SASL_MECHANISM=PLAIN
MILENA_KAFKA_UNAVAILABLE_BROKERS=127.0.0.1:1
```

If the broker uses a generated local CA, set
`MILENA_KAFKA_SSL_CA_LOCATION` to the CA PEM path. If the local certificate does
not include a `localhost` SAN, set
`MILENA_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=` for this local-only test
run.

An example environment file lives at
`src-tauri/tests/fixtures/kafka/kafka.integration.env.example`.
