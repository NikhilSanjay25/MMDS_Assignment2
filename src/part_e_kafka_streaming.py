"""Part E (real stream): Spark Structured Streaming over a Kafka topic of chess games.

stream_producer.py publishes one JSON message per finished game, either replayed from the
2014 dump at a fixed rate or taken live from lichess.org.  This job subscribes to the topic
and runs three independent streaming queries on it:

  q1 per-batch  (foreachBatch)  records per micro-batch, results, mean rating; plus two
                stream-mining structures kept across batches on the driver:
                  * a uniform random sample of fixed size (bottom-k / reservoir sampling)
                  * a Bloom filter that flags players never seen before (stream filtering)
  q2 windows    event-time tumbling windows per time-control category with a watermark,
                so late records are dropped instead of growing state forever
  q3 running    stateful running totals per termination type (complete mode)

A StreamingQueryListener records every query's progress (input / processing rate, batch
duration, rows dropped by the watermark).  maxOffsetsPerTrigger caps the batch size, which
is Spark's back-pressure for Kafka: a burst stays in Kafka instead of overloading a batch.

Usage:
  python part_e_kafka_streaming.py --mode replay --expect 200000
  python part_e_kafka_streaming.py --mode live --duration 600 --window "1 minute" --watermark "2 minutes"
Results: results/e_kafka_<mode>_*.csv and results/part_e_kafka_<mode>_summary.json
"""
import argparse
import hashlib
import json
import math
import shutil
import tempfile
import time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common import KAFKA_BOOTSTRAP, KAFKA_JARS, RESULTS, environment, get_spark

ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["replay", "live"], default="replay")
ap.add_argument("--topic")
ap.add_argument("--expect", type=int, default=0, help="stop after this many records (replay)")
ap.add_argument("--duration", type=int, default=900, help="stop after this many seconds")
ap.add_argument("--trigger", type=int, default=3, help="micro-batch interval, seconds")
ap.add_argument("--max-offsets", type=int, default=10000, help="maxOffsetsPerTrigger")
ap.add_argument("--window", default="1 hour", help="event-time window length")
ap.add_argument("--watermark", default="2 hours", help="allowed lateness")
ap.add_argument("--sample", type=int, default=2000, help="reservoir sample size")
a = ap.parse_args()
topic = a.topic or f"lichess-{a.mode}"
tag = f"e_kafka_{a.mode}"

spark = get_spark(f"PartE-Kafka-{a.mode}", spark__jars=",".join(KAFKA_JARS),
                  spark__sql__shuffle__partitions="4",
                  spark__sql__streaming__noDataMicroBatches__enabled="true")
ckpt = tempfile.mkdtemp(prefix=f"mmds_kafka_{a.mode}_")

schema = StructType([StructField(n, t) for n, t in [
    ("game_id", StringType()), ("white", StringType()), ("black", StringType()),
    ("result", StringType()), ("white_elo", IntegerType()), ("black_elo", IntegerType()),
    ("eco", StringType()), ("opening", StringType()), ("time_control", StringType()),
    ("category", StringType()), ("termination", StringType()), ("num_moves", IntegerType()),
    ("event_time", StringType()), ("source", StringType())]])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", topic)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", a.max_offsets)
       .option("failOnDataLoss", "false")
       .load())
games = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("g"),
                    F.col("timestamp").alias("kafka_time"))
         .select("g.*", "kafka_time")
         .filter(F.col("game_id").isNotNull())                          # drop malformed messages
         .withColumn("event_time", F.to_timestamp("event_time"))
         .withColumn("avg_elo", (F.col("white_elo") + F.col("black_elo")) / 2))


# ---- stream mining structures (driver side, updated once per micro-batch) -------------
class Bloom:
    """Bloom filter with k hash functions over an m-bit array (MMDS ch. 4, filtering)."""

    def __init__(self, n_expected, fp_rate=0.01):
        self.m = int(-n_expected * math.log(fp_rate) / math.log(2) ** 2)
        self.k = max(1, round(self.m / n_expected * math.log(2)))
        self.bits = bytearray(self.m // 8 + 1)

    def _idx(self, item):
        h = hashlib.blake2b(item.encode(), digest_size=16).digest()
        h1, h2 = int.from_bytes(h[:8], "little"), int.from_bytes(h[8:], "little")
        return [(h1 + i * h2) % self.m for i in range(self.k)]   # double hashing

    def add_if_new(self, item):
        """Return True if item was (probably) not in the filter, then add it."""
        new = False
        for i in self._idx(item):
            byte, bit = divmod(i, 8)
            if not self.bits[byte] >> bit & 1:
                new = True
                self.bits[byte] |= 1 << bit
        return new


bloom = Bloom(n_expected=100000)
exact_players = set()          # ground truth, only to measure the Bloom filter's error
reservoir = pd.DataFrame()     # bottom-k sample: keep the k rows with the smallest random key
state = {"records": 0}
batch_log, window_log, running_log, progress_log = [], [], [], []


def per_batch(df, batch_id):
    df = df.persist()
    r = df.agg(F.count("*").alias("records"),
               F.sum((F.col("result") == "1-0").cast("int")).alias("white_wins"),
               F.sum((F.col("result") == "0-1").cast("int")).alias("black_wins"),
               F.sum((F.col("result") == "1/2-1/2").cast("int")).alias("draws"),
               F.round(F.avg("avg_elo"), 1).alias("avg_rating"),
               F.min("event_time").alias("event_time_min"),
               F.max("event_time").alias("event_time_max")).collect()[0].asDict()
    if not r["records"]:
        df.unpersist()
        return
    # Sampling: a uniform sample of fixed size over the whole stream so far
    global reservoir
    cand = (df.withColumn("rkey", F.rand(seed=batch_id))
              .orderBy("rkey").limit(a.sample)
              .select("rkey", "game_id", "category", "termination", "result", "avg_elo").toPandas())
    reservoir = pd.concat([reservoir, cand]).nsmallest(a.sample, "rkey")
    # Filtering: which players appear for the first time? (Bloom filter vs exact set)
    players = [p.player for p in df.select(F.explode(F.array("white", "black")).alias("player"))
               .distinct().collect() if p.player]
    bloom_new = sum(bloom.add_if_new(p) for p in players)
    exact_new = len(set(players) - exact_players)
    exact_players.update(players)
    df.unpersist()
    state["records"] += r["records"]
    r.update(batch_id=batch_id, wall_time=time.strftime("%H:%M:%S"),
             new_players_bloom=bloom_new, new_players_exact=exact_new,
             event_time_min=str(r["event_time_min"]), event_time_max=str(r["event_time_max"]))
    batch_log.append(r)
    print(f"BATCH {batch_id:3d} records={r['records']:6d} total={state['records']:7d} "
          f"W/B/D={r['white_wins']}/{r['black_wins']}/{r['draws']} rating={r['avg_rating']} "
          f"new players bloom/exact={bloom_new}/{exact_new}", flush=True)


def per_window(df, batch_id):
    for row in df.collect():
        window_log.append({"batch_id": batch_id, "window_start": str(row.window.start),
                           "window_end": str(row.window.end), "category": row.category,
                           "games": row.games, "avg_rating": row.avg_rating})


def per_running(df, batch_id):
    for row in df.collect():
        running_log.append({"batch_id": batch_id, "termination": row.termination, "games": row.games})


class Progress(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows == 0:
            return
        progress_log.append({
            "query": p.name, "batch_id": p.batchId, "input_rows": p.numInputRows,
            "input_rows_per_sec": round(p.inputRowsPerSecond, 1),
            "processed_rows_per_sec": round(p.processedRowsPerSecond, 1),
            "batch_ms": p.durationMs.get("triggerExecution"),
            "state_rows": sum(s.numRowsTotal for s in p.stateOperators),
            "dropped_by_watermark": sum(s.numRowsDroppedByWatermark for s in p.stateOperators)})

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


spark.streams.addListener(Progress())
trig = f"{a.trigger} seconds"
q1 = (games.writeStream.queryName("per_batch").foreachBatch(per_batch).trigger(processingTime=trig)
      .option("checkpointLocation", f"{ckpt}/q1").start())
windows = (games.withWatermark("event_time", a.watermark)
           .groupBy(F.window("event_time", a.window), "category")
           .agg(F.count("*").alias("games"), F.round(F.avg("avg_elo"), 1).alias("avg_rating")))
q2 = (windows.writeStream.queryName("windows").outputMode("update").foreachBatch(per_window)
      .trigger(processingTime=trig).option("checkpointLocation", f"{ckpt}/q2").start())
running = games.groupBy("termination").agg(F.count("*").alias("games"))
q3 = (running.writeStream.queryName("running").outputMode("complete").foreachBatch(per_running)
      .trigger(processingTime=trig).option("checkpointLocation", f"{ckpt}/q3").start())

t0 = time.time()
while time.time() - t0 < a.duration and not (a.expect and state["records"] >= a.expect):
    time.sleep(1)
for q in (q1, q2, q3):
    q.processAllAvailable()           # drain what is already in Kafka
    q.stop()
wall = round(time.time() - t0, 1)

# ---- outputs ----------------------------------------------------------------------------
batches = pd.DataFrame(batch_log)
batches.to_csv(f"{RESULTS}/{tag}_batches.csv", index=False)
win = pd.DataFrame(window_log)
if not win.empty:   # update mode re-emits a window when it changes: keep its last value
    win = (win.sort_values("batch_id").groupby(["window_start", "category"], as_index=False).last()
              .sort_values(["window_start", "category"]))
win.to_csv(f"{RESULTS}/{tag}_windows.csv", index=False)
run = pd.DataFrame(running_log)
run.to_csv(f"{RESULTS}/{tag}_running.csv", index=False)
prog = pd.DataFrame(progress_log)
prog.to_csv(f"{RESULTS}/{tag}_progress.csv", index=False)

total = int(batches.records.sum()) if not batches.empty else 0
final_running = ({r.termination: int(r.games) for r in run[run.batch_id == run.batch_id.max()].itertuples()}
                 if not run.empty else {})
sample_mix = (reservoir.termination.value_counts(normalize=True).round(4).to_dict()
              if not reservoir.empty else {})
full_mix = {k: round(v / total, 4) for k, v in final_running.items()} if total else {}
p1 = prog[prog["query"] == "per_batch"] if not prog.empty else prog
summary = {
    "api": "Structured Streaming, Kafka source", "mode": a.mode, "topic": topic,
    "trigger_seconds": a.trigger, "max_offsets_per_trigger": a.max_offsets,
    "window": a.window, "watermark": a.watermark,
    "non_empty_batches": int(len(batches)), "records_processed": total,
    "running_totals_final": final_running,
    "event_time_range": [str(batches.event_time_min.min()), str(batches.event_time_max.max())] if total else None,
    "windows_emitted": int(len(win)),
    "rows_dropped_by_watermark": int(prog.dropped_by_watermark.sum()) if not prog.empty else 0,
    "mean_batch_ms": round(float(p1.batch_ms.mean()), 1) if not p1.empty else None,
    "mean_processed_rows_per_sec": round(float(p1.processed_rows_per_sec.mean()), 1) if not p1.empty else None,
    "sampling": {"sample_size": int(len(reservoir)), "termination_mix_sample": sample_mix,
                 "termination_mix_full_stream": full_mix},
    "filtering": {"bloom_bits": bloom.m, "bloom_hashes": bloom.k,
                  "distinct_players_exact": len(exact_players),
                  "distinct_players_bloom": int(batches.new_players_bloom.sum()) if total else 0},
    "wall_seconds": wall, "environment": environment()}
with open(f"{RESULTS}/part_{tag}_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)
print(json.dumps(summary, indent=2, default=str))
spark.stop()
shutil.rmtree(ckpt, ignore_errors=True)
