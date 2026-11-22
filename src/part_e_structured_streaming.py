"""Part E: Structured Streaming over a simulated feed of Lichess games (socket source).

A producer thread plays the role of the Lichess game feed: it listens on a local TCP
socket and pushes a burst of game records (one tab-separated line per finished game)
every 3 seconds.  Two Structured Streaming queries read the feed:
  q1  foreachBatch: count the records of every micro-batch and summarise results/ratings
  q2  stateful running aggregation per termination type (outputMode "complete")

Every Structured Streaming query opens its own socket connection, so the producer waits
for both queries to connect and sends every burst to each of them.

Checkpointing needs Hadoop's native libraries on Windows; run this on Linux / WSL / Docker.
Results go to results/e_ss_*.csv and results/part_e_ss_summary.json.
"""
import json
import shutil
import socket
import tempfile
import threading
import time

import pandas as pd
from pyspark.sql import functions as F

from common import DATA, RESULTS, environment, get_spark

HOST, PORT = "localhost", 9998
N_BATCHES, ROWS_PER_BATCH, INTERVAL = 20, 10000, 3   # 200k games, one burst every 3 s
N_QUERIES = 2

cols = ["game_id", "white", "black", "result", "white_elo", "black_elo", "opening",
        "time_control", "termination", "utc_date", "utc_time", "num_moves"]
source = pd.read_csv(DATA, usecols=cols, dtype=str, keep_default_na=False,
                     nrows=N_BATCHES * ROWS_PER_BATCH)[cols]
ready = threading.Event()


def producer():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(N_QUERIES)
    ready.set()
    conns = [srv.accept()[0] for _ in range(N_QUERIES)]   # one connection per query
    print(f"[producer] {len(conns)} queries connected", flush=True)
    time.sleep(INTERVAL)
    for i in range(N_BATCHES):
        chunk = source.iloc[i * ROWS_PER_BATCH:(i + 1) * ROWS_PER_BATCH]
        payload = ("\n".join("\t".join(r) for r in chunk.itertuples(index=False)) + "\n").encode()
        for c in conns:
            c.sendall(payload)
        print(f"[producer] sent burst {i} ({len(chunk)} games)", flush=True)
        time.sleep(INTERVAL)
    time.sleep(2 * INTERVAL)
    for c in conns:
        c.close()
    srv.close()


th = threading.Thread(target=producer, daemon=True)
th.start()
ready.wait()

spark = get_spark("PartE-StructuredStreaming", spark__sql__shuffle__partitions="4")
ckpt = tempfile.mkdtemp(prefix="mmds_ss_ckpt_")

raw = spark.readStream.format("socket").option("host", HOST).option("port", PORT).load()
f = F.split("value", "\t")
events = (raw.select(*[f[i].alias(c) for i, c in enumerate(cols)])
          .filter(F.col("termination").isNotNull())                  # drop malformed lines
          .withColumn("avg_elo", (F.col("white_elo").cast("int") + F.col("black_elo").cast("int")) / 2)
          .withColumn("white_win", (F.col("result") == "1-0").cast("int"))
          .withColumn("black_win", (F.col("result") == "0-1").cast("int"))
          .withColumn("draw", (F.col("result") == "1/2-1/2").cast("int")))

batch_log, running_log = [], []


def per_batch(batch_df, batch_id):
    """Basic aggregation: count the records of this micro-batch and summarise them."""
    r = batch_df.agg(F.count("*").alias("records"), F.sum("white_win").alias("white_wins"),
                     F.sum("black_win").alias("black_wins"), F.sum("draw").alias("draws"),
                     F.round(F.avg("avg_elo"), 1).alias("avg_rating")).collect()[0].asDict()
    if not r["records"]:
        return
    r.update(batch_id=batch_id, time=time.strftime("%H:%M:%S"))
    batch_log.append(r)
    print("BATCH", r, flush=True)


def per_batch_running(df, batch_id):
    pdf = df.toPandas()
    if pdf.empty:
        return
    pdf["batch_id"] = batch_id
    running_log.append(pdf)
    print("RUNNING after batch", batch_id, pdf[["termination", "games"]].values.tolist(), flush=True)


trigger = f"{INTERVAL} seconds"
q1 = (events.writeStream.foreachBatch(per_batch).trigger(processingTime=trigger)
      .option("checkpointLocation", f"{ckpt}/q1").start())
running = events.groupBy("termination").agg(F.count("*").alias("games"),
                                            F.round(F.avg("avg_elo"), 1).alias("avg_rating"))
q2 = (running.writeStream.outputMode("complete").foreachBatch(per_batch_running)
      .trigger(processingTime=trigger).option("checkpointLocation", f"{ckpt}/q2").start())

t0 = time.time()
th.join()
time.sleep(INTERVAL * 2)          # let the last micro-batches drain
for q in (q1, q2):
    q.stop()

batches = pd.DataFrame(batch_log).sort_values("batch_id")
batches.to_csv(f"{RESULTS}/e_ss_batches.csv", index=False)
running_df = pd.concat(running_log).sort_values(["batch_id", "games"], ascending=[True, False])
running_df.to_csv(f"{RESULTS}/e_ss_running.csv", index=False)
final = running_df[running_df.batch_id == running_df.batch_id.max()]
summary = {"api": "Structured Streaming (socket source, foreachBatch + complete-mode aggregate)",
           "bursts_sent": N_BATCHES, "rows_per_burst": ROWS_PER_BATCH,
           "trigger_seconds": INTERVAL, "non_empty_batches": int(len(batches)),
           "records_processed": int(batches.records.sum()),
           "running_totals_final": dict(zip(final.termination, final.games.astype(int))),
           "wall_seconds": round(time.time() - t0, 1), "environment": environment()}
with open(f"{RESULTS}/part_e_ss_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)
print(summary)
spark.stop()
shutil.rmtree(ckpt, ignore_errors=True)
