#!/usr/bin/env bash
# Run every part of the assignment on Linux / WSL2 and log each run to results/logs/.
# Usage: scripts/run_all.sh [step ...]   steps: b c d dstream ss kafka-replay kafka-live cluster figures
# With no arguments all steps run in order (~25 minutes, of which 10 are the live Lichess feed).
set -euo pipefail
source "${MMDS_HOME:-$HOME/mmds}/env.sh"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$REPO/results/logs"; mkdir -p "$LOGS"
cd "$REPO/src"
STEPS=("$@"); [ ${#STEPS[@]} -eq 0 ] && STEPS=(b c d dstream ss kafka-replay kafka-live cluster figures)
LIVE_SECONDS="${LIVE_SECONDS:-600}"

run() { local name=$1; shift; echo "=== $name: $*"; "$@" 2>&1 | tee "$LOGS/$name.log"; }

for s in "${STEPS[@]}"; do
  case "$s" in
    b)       run part_b python part_b_rdd.py ;;
    c)       run part_c python part_c_dataframe.py ;;
    d)       run part_d python part_d_pipeline.py ;;
    dstream) run part_e_dstream python part_e_streaming.py ;;
    ss)      run part_e_structured python part_e_structured_streaming.py ;;
    kafka-replay)
      bash "$REPO/scripts/kafka.sh" start
      python stream_producer.py replay --limit 200000 --rate 4000 > "$LOGS/producer_replay.log" 2>&1 &
      P=$!
      run part_e_kafka_replay python part_e_kafka_streaming.py --mode replay --expect 200000 \
          --max-offsets 10000 --trigger 3 --window "1 hour" --watermark "2 hours"
      wait $P ;;
    kafka-live)
      bash "$REPO/scripts/kafka.sh" start
      python stream_producer.py live --duration "$LIVE_SECONDS" > "$LOGS/producer_live.log" 2>&1 &
      P=$!
      run part_e_kafka_live python part_e_kafka_streaming.py --mode live --duration $((LIVE_SECONDS + 30)) \
          --max-offsets 1000 --trigger 5 --window "1 minute" --watermark "2 minutes" --sample 200
      wait $P
      bash "$REPO/scripts/kafka.sh" stop ;;
    cluster)
      bash "$REPO/scripts/cluster.sh" start 8 2 1600m
      run part_f_cluster python part_f_cluster_benchmark.py --master spark://localhost:7077 --executors 1 2 4 8
      bash "$REPO/scripts/cluster.sh" stop ;;
    figures) run figures python make_figures.py ;;
    *) echo "unknown step $s" >&2; exit 2 ;;
  esac
done
echo "all requested steps finished"
