#!/usr/bin/env bash
# Spark Standalone cluster: one master and N worker processes (separate JVMs) on this host.
# Usage: scripts/cluster.sh start [N_WORKERS=8] [CORES=2] [MEMORY=1600m] | stop | status
# Master UI: http://localhost:8080   Master URL: spark://localhost:7077
set -euo pipefail
source "${MMDS_HOME:-$HOME/mmds}/env.sh"
SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME
RUN="$MMDS_HOME/run"; LOGS="$MMDS_HOME/logs"; mkdir -p "$RUN" "$LOGS" "$MMDS_HOME/work"

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

case "${1:-}" in
  start)
    N="${2:-8}"; CORES="${3:-2}"; MEM="${4:-1600m}"
    if ! port_open 7077; then
      nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
        --host localhost --port 7077 --webui-port 8080 > "$LOGS/master.log" 2>&1 &
      echo $! > "$RUN/master.pid"
      for _ in $(seq 60); do port_open 7077 && break; sleep 1; done
    fi
    for i in $(seq "$N"); do
      nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
        --host localhost --cores "$CORES" --memory "$MEM" --webui-port $((8080 + i)) \
        --work-dir "$MMDS_HOME/work" spark://localhost:7077 > "$LOGS/worker$i.log" 2>&1 &
      echo $! > "$RUN/worker$i.pid"
    done
    sleep 8
    echo "master spark://localhost:7077 with $N workers x $CORES cores x $MEM" ;;
  stop)
    for f in "$RUN"/worker*.pid "$RUN"/master.pid; do
      [ -f "$f" ] && { kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; }
    done
    echo "cluster stopped" ;;
  status)
    port_open 7077 && echo "master running; workers: $(ls "$RUN"/worker*.pid 2>/dev/null | wc -l)" || echo "stopped" ;;
  *) echo "usage: $0 start [N] [CORES] [MEM] | stop | status" >&2; exit 2 ;;
esac
