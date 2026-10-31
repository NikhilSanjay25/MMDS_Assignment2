"""Part E: Spark streaming over a simulated feed of Lichess games (micro-batch DStream API).

A producer thread plays the role of the Lichess game feed: it listens on a local TCP socket
and pushes a burst of game records (one tab-separated line per finished game) every 3 seconds.
Spark Streaming cuts the feed into 3-second micro-batches and, for every batch,
  * counts the records and summarises results / ratings            (basic aggregation)
  * updates running totals per termination type                    (stateful, kept on the driver)
  * keeps a sliding 9-second window of records per termination     (windowed aggregation)

part_e_structured_streaming.py is the same pipeline written with the Structured Streaming API;
it needs Hadoop's Windows native libraries for checkpointing and could not be run on the
development machine, which is why this DStream variant produced the reported results.
"""
import json
import socket
import threading
import time
from operator import add

import pandas as pd
from pyspark.streaming import StreamingContext

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
        conn.sendall(("\n".join("\t".join(r) for r in chunk.itertuples(index=False)) + "\n").encode())
        print(f"[producer] sent burst {i} ({len(chunk)} games)", flush=True)
        time.sleep(INTERVAL)
    conn.close()
    srv.close()


th = threading.Thread(target=producer, daemon=True)
th.start()
ready.wait()

# 1 receiver core + a few workers; one block (= one partition) per 3 s batch keeps task counts small
spark = get_spark("PartE-Streaming", master="local[4]", spark__streaming__blockInterval="1500ms",
                  spark__sql__shuffle__partitions="4")
sc = spark.sparkContext
ssc = StreamingContext(sc, INTERVAL)

batch_log, running_log, window_log = [], [], []
totals, batch_no = {}, [0]


def parse(line):
    f = line.split("\t")
    try:
        elo = (int(f[4]) + int(f[5])) / 2.0
    except ValueError:
        elo = None
    return f[3], f[8], elo          # result, termination, mean rating


events = ssc.socketTextStream(HOST, PORT).filter(lambda l: l.count("\t") >= 11).map(parse)


def per_batch(t, rdd):
    """Basic aggregation: count the records of this micro-batch and summarise them."""
    # fold() consumes the whole RDD and is safe on empty batches
    n, w, b, d, esum, en = rdd.map(lambda r: (1, r[0] == "1-0", r[0] == "0-1", r[0] == "1/2-1/2",
                                              r[2] or 0.0, 1 if r[2] else 0)
                                   ).fold((0, 0, 0, 0, 0.0, 0),
                                          lambda x, y: tuple(p + q for p, q in zip(x, y)))
    if n == 0:
        return
    for term, c in rdd.map(lambda r: (r[1], 1)).reduceByKey(add).collect():
        totals[term] = totals.get(term, 0) + c
    batch_no[0] += 1
    rec = {"batch_id": batch_no[0], "time": time.strftime("%H:%M:%S"), "records": n,
           "white_wins": int(w), "black_wins": int(b), "draws": int(d),
           "avg_rating": round(esum / en, 1)}
    batch_log.append(rec)
    for term, c in totals.items():
        running_log.append({"batch_id": batch_no[0], "termination": term, "games": c})
    print("BATCH", rec, "| running:", dict(totals), flush=True)


def per_window(t, rdd):
    rows = rdd.collect()
    if rows:
        window_log.append({"batch_id": batch_no[0], **dict(rows)})
        print("WINDOW(9s)", dict(rows), flush=True)


events.foreachRDD(per_batch)
(events.map(lambda r: (r[1], 1))
       .reduceByKeyAndWindow(add, None, 3 * INTERVAL, INTERVAL)   # no inverse fn -> no checkpoint
       .foreachRDD(per_window))

t0 = time.time()
ssc.start()
th.join()
ssc.awaitTerminationOrTimeout(INTERVAL * 3)     # let the last micro-batches drain
ssc.stop(stopSparkContext=False, stopGraceFully=True)

pd.DataFrame(batch_log).to_csv(f"{RESULTS}/e_stream_batches.csv", index=False)
pd.DataFrame(running_log).to_csv(f"{RESULTS}/e_stream_running.csv", index=False)
pd.DataFrame(window_log).fillna(0).to_csv(f"{RESULTS}/e_stream_window.csv", index=False)
summary = {"api": "Spark Streaming (DStream micro-batches)", "bursts_sent": N_BATCHES,
           "rows_per_burst": ROWS_PER_BATCH, "batch_interval_seconds": INTERVAL,
           "non_empty_batches": len(batch_log),
           "records_processed": sum(r["records"] for r in batch_log),
           "wall_seconds": round(time.time() - t0, 1)}
with open(f"{RESULTS}/part_e_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)
print(summary)
spark.stop()
