# Milena Local Kafka

This directory supports the local Docker Compose broker in
`docker-compose.kafka.yml`. It is for issue #14 integration validation only and
is not production-grade Kafka configuration.

## Broker

Start the broker and deterministic topics from the repo root:

```sh
docker compose -f docker-compose.kafka.yml up kafka-topics
```

Run the compose-side smoke check:

```sh
docker compose -f docker-compose.kafka.yml --profile tools run --rm kafka-smoke
```

Reset all broker data and generated certs:

```sh
docker compose -f docker-compose.kafka.yml down -v
rm -rf docker/kafka/generated
```

## Host Connection

Milena should connect to:

```properties
bootstrap.servers=localhost:19092
security.protocol=SASL_SSL
ssl.ca.location=docker/kafka/generated/ssl/ca.crt
```

PLAIN user:

```properties
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="milena_plain" password="milena-plain-secret";
```

SCRAM user:

```properties
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="milena_scram" password="milena-scram-secret";
```

The generated CA lives at `docker/kafka/generated/ssl/ca.crt` after compose
starts. Java CLI client property examples are checked in under
`docker/kafka/config/clients`.

## Deterministic Topics

- `milena.issue14.records`
- `milena.issue14.polling`
- `milena.issue14.publish`
- `milena.issue14.keyed`
- `milena.issue14.errors`
- `milena.issue14.cleanup`

JSON payload fixtures live under `docker/kafka/fixtures`.
