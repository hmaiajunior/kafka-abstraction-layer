#!/bin/sh
# Registers the demo Avro schema in Confluent Schema Registry under <topic>-value.
# Idempotent: SR rejects duplicate registrations with HTTP 409, which we treat as success.
set -eu

SR_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SUBJECT="${SUBJECT:-demo-events-value}"
SCHEMA_FILE="${SCHEMA_FILE:-/schemas/demo-event.avsc}"

if ! command -v jq >/dev/null 2>&1; then
  apk add --no-cache jq curl >/dev/null
fi

echo "[register-schema] Waiting for Schema Registry at $SR_URL..."
i=0
until curl -fs -o /dev/null "$SR_URL/subjects"; do
  i=$((i+1))
  if [ "$i" -ge 60 ]; then
    echo "[register-schema] Timeout waiting for Schema Registry" >&2
    exit 1
  fi
  sleep 2
done

SCHEMA_STR=$(jq -Rs . < "$SCHEMA_FILE")
PAYLOAD=$(printf '{"schemaType":"AVRO","schema":%s}' "$SCHEMA_STR")

echo "[register-schema] Registering subject=$SUBJECT"
HTTP=$(curl -s -o /tmp/sr-resp -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$PAYLOAD" \
  "$SR_URL/subjects/$SUBJECT/versions")

if [ "$HTTP" = "200" ] || [ "$HTTP" = "201" ]; then
  echo "[register-schema] OK ($HTTP): $(cat /tmp/sr-resp)"
elif [ "$HTTP" = "409" ]; then
  echo "[register-schema] Subject already exists ($HTTP) — OK"
else
  echo "[register-schema] FAILED ($HTTP): $(cat /tmp/sr-resp)" >&2
  exit 1
fi
