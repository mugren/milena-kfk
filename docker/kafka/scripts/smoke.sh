#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
PLAIN_CONFIG=/milena-kafka/config/clients/milena.plain.container.properties
SCRAM_CONFIG=/milena-kafka/config/clients/milena.scram.container.properties
NO_KEY_FIXTURE=/milena-kafka/fixtures/publish-no-key.json
KEYED_FIXTURE=/milena-kafka/fixtures/publish-with-key.json

echo "Listing topics with PLAIN user"
"${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --command-config "${PLAIN_CONFIG}" \
  --list

echo "Listing topics with SCRAM-SHA-512 user"
"${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --command-config "${SCRAM_CONFIG}" \
  --list

echo "Producing no-key JSON fixture"
{
  tr -d '\n' < "${NO_KEY_FIXTURE}"
  printf '\n'
} | "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --producer.config "${PLAIN_CONFIG}" \
  --topic milena.issue14.publish

echo "Producing keyed JSON fixture"
{
  printf 'issue14-key|'
  tr -d '\n' < "${KEYED_FIXTURE}"
  printf '\n'
} | "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --producer.config "${SCRAM_CONFIG}" \
  --topic milena.issue14.keyed \
  --property parse.key=true \
  --property key.separator='|'

echo "Kafka local infra smoke check passed"
