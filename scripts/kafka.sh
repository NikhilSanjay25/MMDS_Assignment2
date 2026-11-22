#!/usr/bin/env bash
# Single-node Kafka broker in KRaft mode (no ZooKeeper).  Usage: scripts/kafka.sh start|stop|status
set -euo pipefail
source "${MMDS_HOME:-$HOME/mmds}/env.sh"
K="$MMDS_HOME/kafka"
CFG="$MMDS_HOME/kafka-server.properties"
DATA_DIR="$MMDS_HOME/kafka-data"

port_open() { (exec 3<>/dev/tcp/127.0.0.1/9092) 2>/dev/null; }

case "${1:-}" in
  start)
    if port_open; then echo "kafka already running"; exit 0; fi
    sed -e "s#^log.dirs=.*#log.dirs=$DATA_DIR#" \
        -e "s#^num.partitions=.*#num.partitions=4#" "$K/config/kraft/server.properties" > "$CFG"
    if [ ! -f "$DATA_DIR/meta.properties" ]; then
      "$K/bin/kafka-storage.sh" format -t "$("$K/bin/kafka-storage.sh" random-uuid)" -c "$CFG" > /dev/null
    fi
    KAFKA_HEAP_OPTS="-Xms512m -Xmx1g" LOG_DIR="$MMDS_HOME/logs/kafka" \
      "$K/bin/kafka-server-start.sh" -daemon "$CFG"
    for _ in $(seq 60); do port_open && { echo "kafka up on localhost:9092"; exit 0; }; sleep 1; done
    echo "kafka did not start; see $MMDS_HOME/logs/kafka" >&2; exit 1 ;;
  stop)
    "$K/bin/kafka-server-stop.sh" || true
    for _ in $(seq 30); do port_open || { echo "kafka stopped"; exit 0; }; sleep 1; done ;;
  status)
    port_open && echo "running" || echo "stopped" ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
