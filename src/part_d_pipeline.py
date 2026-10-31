"""Part D: end-to-end workflow  Input -> RDD processing -> DataFrame processing -> Analytical output."""
import csv
import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)

from common import DATA, RESULTS, get_spark, save

spark = get_spark("PartD-Pipeline")
sc = spark.sparkContext
stages = {}


def timed(name, fn):
    t = time.time()
    res = fn()
    stages[name] = round(time.time() - t, 1)
    return res


# Stage 1: input data ---------------------------------------------------------
lines = sc.textFile(DATA, minPartitions=24)
n_in = timed("1_input_count", lambda: lines.count() - 1)


# Stage 2: RDD processing: parse, validate, project the columns we need ------
def parse(line):
    r = next(csv.reader([line]))
    try:
        we, be = int(r[4]), int(r[5])
        base, inc = r[10].split("+")
        est = int(base) + 40 * int(inc)
    except ValueError:
        return None
    if r[3] not in ("1-0", "0-1", "1/2-1/2") or not (600 <= we <= 3000 and 600 <= be <= 3000):
        return None
    score = {"1-0": 1.0, "0-1": 0.0, "1/2-1/2": 0.5}[r[3]]
    cat = "Bullet" if est < 180 else "Blitz" if est < 480 else "Rapid" if est < 1500 else "Classical"
    return (r[0], r[9], cat, (we + be) / 2.0, score, int(r[14]))


rdd = (lines.filter(lambda l: not l.startswith("game_id,"))
            .map(parse).filter(lambda x: x is not None))
rdd.cache()
n_rdd = timed("2_rdd_processing", rdd.count)

# Stage 3: DataFrame processing -----------------------------------------------
schema = StructType([StructField("game_id", StringType()), StructField("opening", StringType()),
                     StructField("category", StringType()), StructField("avg_elo", DoubleType()),
                     StructField("white_score", DoubleType()), StructField("plies", IntegerType())])
df = timed("3a_rdd_to_dataframe", lambda: spark.createDataFrame(rdd, schema).cache())
timed("3b_dataframe_materialise", df.count)

# Stage 4: analytical output ----------------------------------------------------
out = timed("4_analytics", lambda: (
    df.groupBy("category", "opening")
      .agg(F.count("*").alias("games"), F.round(F.avg("white_score"), 4).alias("white_score"),
           F.round(F.avg("avg_elo"), 1).alias("avg_rating"))
      .filter(F.col("games") >= 500)
      .orderBy(F.desc("white_score")).limit(10).toPandas()))
out.to_csv(f"{RESULTS}/d_pipeline_output.csv", index=False)
print(out.to_string())

jobs = sc.statusTracker().getJobIdsForGroup()
meta = {"rows_in": n_in, "rows_after_rdd_cleaning": n_rdd, "stage_seconds": stages,
        "spark_jobs_run": len(jobs), "rdd_partitions": rdd.getNumPartitions(),
        "df_plan": df.groupBy("category").count()._jdf.queryExecution().simpleString()[:600]}
with open(f"{RESULTS}/part_d_pipeline.json", "w") as f:
    json.dump(meta, f, indent=2)
print(json.dumps(meta, indent=2))
spark.stop()
