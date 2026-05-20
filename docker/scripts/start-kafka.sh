#!/bin/sh
# Formats KRaft storage on first start (idempotent), then runs the broker.
set -eu

CONFIG="${KAFKA_CONFIG:-/etc/kafka/server.properties}"
DATA_DIR="${KAFKA_LOG_DIRS:-/var/lib/kafka/data}"

mkdir -p "$DATA_DIR"

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  CID="${KAFKA_CLUSTER_ID:-$(/opt/kafka/bin/kafka-storage.sh random-uuid)}"
  echo "[start-kafka] Formatting storage at $DATA_DIR with cluster-id=$CID"
  /opt/kafka/bin/kafka-storage.sh format -t "$CID" -c "$CONFIG" --ignore-formatted
fi

echo "[start-kafka] Launching broker with $CONFIG"
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
