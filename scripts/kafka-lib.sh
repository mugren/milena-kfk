#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KAFKA_COMPOSE_FILE="${MILENA_KAFKA_COMPOSE_FILE:-docker-compose.kafka.yml}"
KAFKA_PROJECT_NAME="${MILENA_KAFKA_PROJECT_NAME:-milena-kafka}"
KAFKA_SERVICE="${MILENA_KAFKA_SERVICE:-kafka}"
KAFKA_TOPIC="${MILENA_KAFKA_TOPIC:-milena.issue14.records}"
KAFKA_SMOKE_BROKERS="${MILENA_KAFKA_SMOKE_BROKERS:-kafka:9092}"
KAFKA_APP_BROKERS="${MILENA_KAFKA_BROKERS:-localhost:19092}"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

info() {
  printf '==> %s\n' "$*"
}

require_compose_file() {
  [[ -f "$ROOT_DIR/$KAFKA_COMPOSE_FILE" ]] || die "missing $KAFKA_COMPOSE_FILE; create the local Kafka compose infra first"
}

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD=(docker-compose)
  else
    die "Docker Compose is required"
  fi
}

compose() {
  require_compose_file
  compose_cmd
  (cd "$ROOT_DIR" && "${COMPOSE_CMD[@]}" -f "$KAFKA_COMPOSE_FILE" -p "$KAFKA_PROJECT_NAME" "$@")
}

kafka_cli_path() {
  local executable="$1"
  compose exec -T "$KAFKA_SERVICE" sh -lc "
    for dir in /opt/bitnami/kafka/bin /opt/kafka/bin /usr/bin /bin; do
      if [ -x \"\$dir/$executable\" ]; then
        printf '%s\n' \"\$dir/$executable\"
        exit 0
      fi
    done
    command -v $executable
  " 2>/dev/null
}

credentials_for_profile() {
  local profile="${1:-}"
  case "$profile" in
    plain | PLAIN)
      KAFKA_SASL_MECHANISM="${MILENA_KAFKA_SASL_MECHANISM:-PLAIN}"
      KAFKA_USERNAME="${MILENA_KAFKA_USERNAME:-milena_plain}"
      KAFKA_PASSWORD="${MILENA_KAFKA_PASSWORD:-milena-plain-secret}"
      ;;
    scram | SCRAM | SCRAM-SHA-512)
      KAFKA_SASL_MECHANISM="${MILENA_KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"
      KAFKA_USERNAME="${MILENA_KAFKA_USERNAME:-milena_scram}"
      KAFKA_PASSWORD="${MILENA_KAFKA_PASSWORD:-milena-scram-secret}"
      ;;
    "")
      KAFKA_SASL_MECHANISM="${MILENA_KAFKA_SASL_MECHANISM:-PLAIN}"
      KAFKA_USERNAME="${MILENA_KAFKA_USERNAME:-milena_plain}"
      KAFKA_PASSWORD="${MILENA_KAFKA_PASSWORD:-milena-plain-secret}"
      ;;
    *)
      die "unknown Kafka auth profile '$profile'; use plain or scram"
      ;;
  esac
}
