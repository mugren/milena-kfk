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
