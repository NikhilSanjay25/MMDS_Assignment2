"""Part E: Structured Streaming over a simulated feed of Lichess games.

A producer thread plays the role of the Lichess game feed: it listens on a local TCP
socket and pushes batches of game records (one tab-separated line per finished game)
every few seconds.  Spark Structured Streaming connects to the socket, and for every
micro-batch counts the records and summarises results/ratings; a second query keeps a
running (stateful) aggregation over everything seen so far.

NOTE: Structured Streaming needs Hadoop's native Windows libraries (winutils.exe + hadoop.dll)
to create checkpoint directories. They are not available on the development machine, so this
script could NOT be executed there; run it on Linux/WSL/Docker (or with winutils installed).
part_e_streaming.py is the variant that was actually run for the reported results.
"""
import json
import socket
import threading
import time

import pandas as pd
from pyspark.sql import functions as F

from common import DATA, RESULTS, get_spark

HOST, PORT = "localhost", 9999
N_BATCHES, ROWS_PER_BATCH, INTERVAL = 20, 10000, 3   # 200k games, one burst every 3 s

cols = ["game_id", "white", "black", "result", "white_elo", "black_elo", "opening",
        "time_control", "termination", "utc_date", "utc_time", "num_moves"]
source = pd.read_csv(DATA, usecols=cols, dtype=str, keep_default_na=False,
                     nrows=N_BATCHES * ROWS_PER_BATCH)[cols]
ready = threading.Event()


def producer():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(1)
    ready.set()
    conn, _ = srv.accept()
    for i in range(N_BATCHES):
        chunk = source.iloc[i * ROWS_PER_BATCH:(i + 1) * ROWS_PER_BATCH]
        payload = "\n".join("\t".join(r) for r in chunk.itertuples(index=False)) + "\n"
        conn.sendall(payload.encode("utf-8"))
        print(f"[producer] sent burst {i} ({len(chunk)} games)", flush=True)
        time.sleep(INTERVAL)
    conn.close()
    srv.close()


th = threading.Thread(target=producer, daemon=True)
th.start()
ready.wait()

spark = get_spark("PartE-Streaming")
spark.conf.set("spark.sql.shuffle.partitions", "4")

raw = (spark.readStream.format("socket").option("host", HOST).option("port", PORT).load())
f = F.split("value", "\t")
events = (raw.select(*[f[i].alias(c) for i, c in enumerate(cols)])
          .withColumn("white_elo", F.col("white_elo").cast("int"))
          .withColumn("black_elo", F.col("black_elo").cast("int"))
          .withColumn("avg_elo", (F.col("white_elo") + F.col("black_elo")) / 2)
          .withColumn("white_win", (F.col("result") == "1-0").cast("int"))
          .withColumn("black_win", (F.col("result") == "0-1").cast("int"))
          .withColumn("draw", (F.col("result") == "1/2-1/2").cast("int")))

batch_log, running_log = [], []


def per_batch(batch_df, batch_id):
    """Basic aggregation: count the records of this micro-batch and summarise them."""
    r = batch_df.agg(F.count("*").alias("records"), F.sum("white_win").alias("white_wins"),
                     F.sum("black_win").alias("black_wins"), F.sum("draw").alias("draws"),
                     F.round(F.avg("avg_elo"), 1).alias("avg_rating")).collect()[0].asDict()
    r["batch_id"] = batch_id
    r["time"] = time.strftime("%H:%M:%S")
    batch_log.append(r)
    print("BATCH", r, flush=True)


trigger = f"{INTERVAL} seconds"
q1 = events.writeStream.foreachBatch(per_batch).trigger(processingTime=trigger).start()

# Stateful running aggregation over the whole stream (outputMode "complete")
running = events.groupBy("termination").agg(F.count("*").alias("games"),
                                            F.round(F.avg("avg_elo"), 1).alias("avg_rating"))


def per_batch_running(df, batch_id):
    pdf = df.toPandas().sort_values("games", ascending=False)
    pdf["batch_id"] = batch_id
    running_log.append(pdf)
    print("RUNNING totals after batch", batch_id, pdf[["termination", "games"]].values.tolist(),
          flush=True)


q2 = (running.writeStream.outputMode("complete").foreachBatch(per_batch_running)
      .trigger(processingTime=trigger).start())

t0 = time.time()
th.join()
time.sleep(INTERVAL * 2)          # let the last micro-batches drain
for q in (q1, q2):
    q.stop()

batches = pd.DataFrame(batch_log).sort_values("batch_id")
batches = batches[batches.records > 0]      # drop empty trigger intervals
batches.to_csv(f"{RESULTS}/e_stream_batches.csv", index=False)
pd.concat(running_log).to_csv(f"{RESULTS}/e_stream_running.csv", index=False)
summary = {"bursts_sent": N_BATCHES, "rows_per_burst": ROWS_PER_BATCH,
           "interval_seconds": INTERVAL, "non_empty_batches": int(len(batches)),
           "records_processed": int(batches.records.sum()),
           "wall_seconds": round(time.time() - t0, 1)}
with open(f"{RESULTS}/part_e_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)
print(summary)
spark.stop()
