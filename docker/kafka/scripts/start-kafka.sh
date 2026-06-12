#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
CONFIG_DIR=${KAFKA_CONFIG_DIR:-/milena-kafka/generated/runtime}
CONFIG_FILE="${CONFIG_DIR}/server.properties"
DATA_DIR=${KAFKA_DATA_DIR:-/var/lib/kafka/data}
CLUSTER_ID=${KAFKA_CLUSTER_ID:-KBlv4duVSf2k9QjoE9R4xA}

ADMIN_PASSWORD=${KAFKA_ADMIN_PASSWORD:-admin-secret}
PLAIN_PASSWORD=${KAFKA_MILENA_PLAIN_PASSWORD:-milena-plain-secret}
SCRAM_PASSWORD=${KAFKA_MILENA_SCRAM_PASSWORD:-milena-scram-secret}
KEYSTORE_PASSWORD=${KAFKA_SSL_KEYSTORE_PASSWORD:-milena-local-keystore}
TRUSTSTORE_PASSWORD=${KAFKA_SSL_TRUSTSTORE_PASSWORD:-milena-local-truststore}

SSL_DIR=/milena-kafka/generated/ssl
SERVER_KEYSTORE="${SSL_DIR}/kafka.server.keystore.p12"
CLIENT_TRUSTSTORE="${SSL_DIR}/kafka.client.truststore.p12"

if [[ ! -s "${SERVER_KEYSTORE}" || ! -s "${CLIENT_TRUSTSTORE}" ]]; then
  echo "Kafka SSL material is missing. Run the kafka-certs compose service first." >&2
  exit 1
fi

mkdir -p "${CONFIG_DIR}" "${DATA_DIR}"

cat > "${CONFIG_FILE}" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka:9094
controller.listener.names=CONTROLLER

listeners=SASL_SSL://0.0.0.0:9092,SASL_SSL_HOST://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094
advertised.listeners=SASL_SSL://kafka:9092,SASL_SSL_HOST://localhost:19092
listener.security.protocol.map=SASL_SSL:SASL_SSL,SASL_SSL_HOST:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SASL_SSL

sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${ADMIN_PASSWORD}" user_admin="${ADMIN_PASSWORD}" user_milena_plain="${PLAIN_PASSWORD}";
listener.name.sasl_ssl_host.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${ADMIN_PASSWORD}" user_admin="${ADMIN_PASSWORD}" user_milena_plain="${PLAIN_PASSWORD}";
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${ADMIN_PASSWORD}";
listener.name.sasl_ssl_host.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${ADMIN_PASSWORD}";
super.users=User:admin

ssl.keystore.location=${SERVER_KEYSTORE}
ssl.keystore.password=${KEYSTORE_PASSWORD}
ssl.key.password=${KEYSTORE_PASSWORD}
ssl.keystore.type=PKCS12
ssl.truststore.location=${CLIENT_TRUSTSTORE}
ssl.truststore.password=${TRUSTSTORE_PASSWORD}
ssl.truststore.type=PKCS12
ssl.client.auth=none

log.dirs=${DATA_DIR}
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
delete.topic.enable=true
EOF

if [[ ! -f "${DATA_DIR}/meta.properties" ]]; then
  "${KAFKA_HOME}/bin/kafka-storage.sh" format \
    --ignore-formatted \
    --cluster-id "${CLUSTER_ID}" \
    --config "${CONFIG_FILE}" \
    --add-scram "SCRAM-SHA-512=[name=admin,password=${ADMIN_PASSWORD}]" \
    --add-scram "SCRAM-SHA-512=[name=milena_scram,password=${SCRAM_PASSWORD}]"
fi

exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONFIG_FILE}"
