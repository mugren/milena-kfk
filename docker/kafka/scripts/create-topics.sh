#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
CONFIG=${KAFKA_CLIENT_CONFIG:-/milena-kafka/config/clients/admin.plain.container.properties}

TOPICS=(
  milena.issue14.records
  milena.issue14.polling
  milena.issue14.publish
  milena.issue14.keyed
  milena.issue14.errors
  milena.issue14.cleanup
  milena.issue14.interop
)

for topic in "${TOPICS[@]}"; do
  "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --command-config "${CONFIG}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000
done

"${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --command-config "${CONFIG}" \
  --list
