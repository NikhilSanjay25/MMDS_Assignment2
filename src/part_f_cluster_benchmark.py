"""Scaling benchmark on a Spark Standalone cluster (master + worker processes).

Start the cluster first (scripts/cluster.sh start 8), then run
    python part_f_cluster_benchmark.py --master spark://localhost:7077 --executors 1 2 4 8

For every executor count the same two jobs run over the full 1.39M-game CSV:
  rdd  Part B style: textFile -> csv parse (Python) -> flatMap moves -> reduceByKey
  df   Part C style: CSV with schema -> casts/cleaning -> groupBy aggregations + token explode
Each executor gets 2 cores; spark.cores.max limits the application to n executors.
Results: results/f_cluster_benchmark.csv and results/part_f_cluster_summary.json
"""
import argparse
import csv
import json
import os
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import DATA, RESULTS, environment

ap = argparse.ArgumentParser()
ap.add_argument("--master", default="spark://localhost:7077")
ap.add_argument("--executors", type=int, nargs="+", default=[1, 2, 4, 8])
ap.add_argument("--cores", type=int, default=2, help="cores per executor")
ap.add_argument("--memory", default="1400m", help="memory per executor")
ap.add_argument("--repeats", type=int, default=2)
a = ap.parse_args()

COLS = ["game_id", "white", "black", "result", "white_elo", "black_elo", "white_diff",
        "black_diff", "eco", "opening", "time_control", "termination", "utc_date",
        "utc_time", "num_moves", "moves"]


def rdd_job(spark):
    sc = spark.sparkContext
    rows = (sc.textFile(DATA, minPartitions=48).filter(lambda l: not l.startswith("game_id,"))
              .map(lambda l: next(csv.reader([l]))))
    moves = (rows.flatMap(lambda r: r[15].split()).map(lambda m: (m.rstrip("+#"), 1))
                 .reduceByKey(lambda x, y: x + y))
    openings = rows.map(lambda r: (r[9], 1)).reduceByKey(lambda x, y: x + y)
    return moves.count() + openings.count()


def df_job(spark):
    schema = StructType([StructField(c, StringType()) for c in COLS])
    df = (spark.read.csv(DATA, header=True, schema=schema, quote='"', escape='"')
          .withColumn("we", F.col("white_elo").cast("int"))
          .withColumn("be", F.col("black_elo").cast("int"))
          .dropna(subset=["we", "be"])
          .withColumn("score", F.when(F.col("result") == "1-0", 1.0)
                                .when(F.col("result") == "0-1", 0.0).otherwise(0.5)))
    n = df.groupBy("opening", "time_control").agg(F.count("*"), F.avg("score"), F.avg("we")).count()
    n += (df.select(F.explode(F.split("moves", " ")).alias("m"))
            .groupBy("m").count().count())
    return n


def session(n):
    return (SparkSession.builder.master(a.master).appName(f"PartF-bench-{n}x{a.cores}")
            .config("spark.cores.max", n * a.cores).config("spark.executor.cores", a.cores)
            .config("spark.executor.memory", a.memory).config("spark.driver.memory", "3g")
            .config("spark.sql.shuffle.partitions", max(8, 2 * n * a.cores))
            .config("spark.ui.showConsoleProgress", "false").getOrCreate())


rows = []
for n in a.executors:
    spark = session(n)
    spark.sparkContext.setLogLevel("ERROR")
    t = time.time()                                  # wait until all n executors registered
    while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1 < n:  # minus driver
        if time.time() - t > 60:
            raise SystemExit(f"cluster has fewer than {n} free executors; start more workers")
        time.sleep(1)
    for job, fn in (("rdd", rdd_job), ("df", df_job)):
        for rep in range(a.repeats):
            t = time.time()
            fn(spark)
            secs = round(time.time() - t, 1)
            rows.append({"executors": n, "cores": n * a.cores, "job": job, "repeat": rep, "seconds": secs})
            print(f"executors={n} cores={n * a.cores} job={job} rep={rep}: {secs} s", flush=True)
    spark.stop()

raw = pd.DataFrame(rows)
best = raw.groupby(["executors", "cores", "job"], as_index=False).seconds.min()
base = best[best.executors == min(a.executors)].set_index("job").seconds
best["speedup"] = (best.job.map(base) / best.seconds).round(2)
best["efficiency"] = (best.speedup / (best.executors / min(a.executors))).round(2)
best.to_csv(f"{RESULTS}/f_cluster_benchmark.csv", index=False)
raw.to_csv(f"{RESULTS}/f_cluster_benchmark_runs.csv", index=False)
summary = {"master": a.master, "cores_per_executor": a.cores, "executor_memory": a.memory,
           "repeats": a.repeats, "metric": "best of repeats, wall seconds",
           "results": best.to_dict("records"), "environment": {**environment(), "master": a.master},
           "note": "standalone cluster: master and workers are separate JVM processes on one host"}
with open(f"{RESULTS}/part_f_cluster_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)
print(best.to_string(index=False))
