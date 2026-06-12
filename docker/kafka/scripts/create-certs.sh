#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR=${KAFKA_SSL_OUTPUT_DIR:-/milena-kafka/generated/ssl}
CA_PASSWORD=${KAFKA_SSL_CA_PASSWORD:-milena-local-ca}
KEYSTORE_PASSWORD=${KAFKA_SSL_KEYSTORE_PASSWORD:-milena-local-keystore}
TRUSTSTORE_PASSWORD=${KAFKA_SSL_TRUSTSTORE_PASSWORD:-milena-local-truststore}
SERVER_ALIAS=${KAFKA_SSL_SERVER_ALIAS:-kafka}
SERVER_DNAME=${KAFKA_SSL_SERVER_DNAME:-CN=localhost,OU=Milena,O=Local Dev,L=Skopje,ST=Skopje,C=MK}
CA_DNAME=${KAFKA_SSL_CA_DNAME:-CN=Milena Local Kafka CA,OU=Milena,O=Local Dev,L=Skopje,ST=Skopje,C=MK}
SAN=${KAFKA_SSL_SAN:-dns:localhost,dns:kafka,ip:127.0.0.1}
VALIDITY_DAYS=${KAFKA_SSL_VALIDITY_DAYS:-3650}

CA_STORE="${OUTPUT_DIR}/kafka.ca.p12"
CA_CERT="${OUTPUT_DIR}/ca.crt"
SERVER_STORE="${OUTPUT_DIR}/kafka.server.keystore.p12"
CLIENT_TRUSTSTORE="${OUTPUT_DIR}/kafka.client.truststore.p12"

if [[ -s "${CA_CERT}" && -s "${SERVER_STORE}" && -s "${CLIENT_TRUSTSTORE}" ]]; then
  echo "Kafka SSL material already exists in ${OUTPUT_DIR}"
  exit 0
fi

umask 077
mkdir -p "${OUTPUT_DIR}"
TMP_DIR="${OUTPUT_DIR}/.tmp.$$"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

keytool -genkeypair \
  -alias ca \
  -keyalg RSA \
  -keysize 4096 \
  -validity "${VALIDITY_DAYS}" \
  -dname "${CA_DNAME}" \
  -ext bc=ca:true \
  -keystore "${TMP_DIR}/kafka.ca.p12" \
  -storetype PKCS12 \
  -storepass "${CA_PASSWORD}" \
  -keypass "${CA_PASSWORD}" \
  -noprompt

keytool -exportcert \
  -alias ca \
  -rfc \
  -keystore "${TMP_DIR}/kafka.ca.p12" \
  -storetype PKCS12 \
  -storepass "${CA_PASSWORD}" \
  -file "${TMP_DIR}/ca.crt"

keytool -genkeypair \
  -alias "${SERVER_ALIAS}" \
  -keyalg RSA \
  -keysize 2048 \
  -validity "${VALIDITY_DAYS}" \
  -dname "${SERVER_DNAME}" \
  -ext "SAN=${SAN}" \
  -keystore "${TMP_DIR}/kafka.server.keystore.p12" \
  -storetype PKCS12 \
  -storepass "${KEYSTORE_PASSWORD}" \
  -keypass "${KEYSTORE_PASSWORD}" \
  -noprompt

keytool -certreq \
  -alias "${SERVER_ALIAS}" \
  -keystore "${TMP_DIR}/kafka.server.keystore.p12" \
  -storetype PKCS12 \
  -storepass "${KEYSTORE_PASSWORD}" \
  -file "${TMP_DIR}/kafka.server.csr" \
  -ext "SAN=${SAN}"

keytool -gencert \
  -alias ca \
  -keystore "${TMP_DIR}/kafka.ca.p12" \
  -storetype PKCS12 \
  -storepass "${CA_PASSWORD}" \
  -infile "${TMP_DIR}/kafka.server.csr" \
  -outfile "${TMP_DIR}/kafka.server.crt" \
  -rfc \
  -validity "${VALIDITY_DAYS}" \
  -ext "SAN=${SAN}" \
  -ext KU=digitalSignature,keyEncipherment \
  -ext EKU=serverAuth

keytool -importcert \
  -alias ca \
  -file "${TMP_DIR}/ca.crt" \
  -keystore "${TMP_DIR}/kafka.server.keystore.p12" \
  -storetype PKCS12 \
  -storepass "${KEYSTORE_PASSWORD}" \
  -noprompt

keytool -importcert \
  -alias "${SERVER_ALIAS}" \
  -file "${TMP_DIR}/kafka.server.crt" \
  -keystore "${TMP_DIR}/kafka.server.keystore.p12" \
  -storetype PKCS12 \
  -storepass "${KEYSTORE_PASSWORD}" \
  -noprompt

keytool -importcert \
  -alias ca \
  -file "${TMP_DIR}/ca.crt" \
  -keystore "${TMP_DIR}/kafka.client.truststore.p12" \
  -storetype PKCS12 \
  -storepass "${TRUSTSTORE_PASSWORD}" \
  -noprompt

cat > "${TMP_DIR}/README.generated.txt" <<EOF
Generated local-dev Kafka SSL material for Milena integration validation.

These files are intentionally ignored by git. Regenerate by removing
docker/kafka/generated and starting docker-compose.kafka.yml again.

CA PEM for librdkafka clients:
  ${CA_CERT}

Java client truststore:
  ${CLIENT_TRUSTSTORE}
EOF

mv "${TMP_DIR}/kafka.ca.p12" "${CA_STORE}"
mv "${TMP_DIR}/ca.crt" "${CA_CERT}"
mv "${TMP_DIR}/kafka.server.keystore.p12" "${SERVER_STORE}"
mv "${TMP_DIR}/kafka.client.truststore.p12" "${CLIENT_TRUSTSTORE}"
mv "${TMP_DIR}/README.generated.txt" "${OUTPUT_DIR}/README.generated.txt"
chmod 0644 "${CA_CERT}" "${OUTPUT_DIR}/README.generated.txt"
chmod 0600 "${CA_STORE}" "${SERVER_STORE}" "${CLIENT_TRUSTSTORE}"

echo "Generated Kafka SSL material in ${OUTPUT_DIR}"
