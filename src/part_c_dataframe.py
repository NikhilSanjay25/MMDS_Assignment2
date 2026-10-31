"""Part C: DataFrame creation, cleaning and querying (DataFrame API + Spark SQL)."""
import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common import DATA, RESULTS, get_spark, save

spark = get_spark("PartC-DataFrame")
log = {}

# Schema creation ----------------------------------------------------------
schema = StructType([StructField(n, StringType(), True) for n in [
    "game_id", "white", "black", "result", "white_elo", "black_elo", "white_diff",
    "black_diff", "eco", "opening", "time_control", "termination", "utc_date",
    "utc_time", "num_moves", "moves"]])
raw = spark.read.csv(DATA, header=True, schema=schema, quote='"', escape='"')
log["raw_rows"] = raw.count()

# Data cleaning ------------------------------------------------------------
df = (raw
      .withColumn("white_elo", F.col("white_elo").cast(IntegerType()))
      .withColumn("black_elo", F.col("black_elo").cast(IntegerType()))
      .withColumn("white_diff", F.col("white_diff").cast(IntegerType()))
      .withColumn("black_diff", F.col("black_diff").cast(IntegerType()))
      .withColumn("num_moves", F.col("num_moves").cast(IntegerType()))
      .withColumn("ts", F.to_timestamp(F.concat_ws(" ", "utc_date", "utc_time"), "yyyy.MM.dd HH:mm:ss"))
      .withColumn("base", F.split("time_control", r"\+")[0].cast(IntegerType()))
      .withColumn("inc", F.split("time_control", r"\+")[1].cast(IntegerType())))
log["null_counts"] = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                                for c in ["white_elo", "black_elo", "ts", "base", "num_moves"]]
                               ).collect()[0].asDict()
clean = (df.dropna(subset=["white_elo", "black_elo", "ts", "base", "inc"])
           .filter(F.col("result").isin("1-0", "0-1", "1/2-1/2"))
           .filter(F.col("num_moves") > 0)
           .filter(F.col("white_elo").between(600, 3000) & F.col("black_elo").between(600, 3000))
           .dropDuplicates(["game_id"])
           .withColumn("est_secs", F.col("base") + 40 * F.col("inc"))
           .withColumn("category", F.when(F.col("est_secs") < 180, "Bullet")
                                    .when(F.col("est_secs") < 480, "Blitz")
                                    .when(F.col("est_secs") < 1500, "Rapid")
                                    .otherwise("Classical"))
           .withColumn("avg_elo", (F.col("white_elo") + F.col("black_elo")) / 2)
           .withColumn("white_score", F.when(F.col("result") == "1-0", 1.0)
                                        .when(F.col("result") == "0-1", 0.0).otherwise(0.5))
           .withColumn("elo_band", (F.floor(F.col("avg_elo") / 200) * 200).cast("int"))
           .withColumn("day", F.to_date("ts"))
           .withColumn("hour", F.hour("ts"))
           .withColumn("plies", F.col("num_moves")))
clean.cache()
log["clean_rows"] = clean.count()
log["removed_rows"] = log["raw_rows"] - log["clean_rows"]
clean.createOrReplaceTempView("games")

t0 = time.time()
# Filtering / aggregation / sorting / groupBy (DataFrame API) --------------
by_cat = (clean.groupBy("category")
          .agg(F.count("*").alias("games"), F.round(F.avg("avg_elo"), 1).alias("avg_rating"),
               F.round(F.avg("plies"), 1).alias("avg_plies"),
               F.round(F.avg("white_score"), 4).alias("white_score"))
          .orderBy(F.desc("games")))
by_cat.show(); save(by_cat, "c_by_category")

top_openings = (clean.groupBy("opening")
                .agg(F.count("*").alias("games"), F.round(F.avg("white_score"), 4).alias("white_score"),
                     F.round(F.avg("avg_elo"), 1).alias("avg_rating"))
                .orderBy(F.desc("games")).limit(15))
top_openings.show(truncate=False); save(top_openings, "c_top_openings")

top_players = (clean.select(F.col("white").alias("player"), F.col("white_elo").alias("elo"),
                            F.col("white_score").alias("score"))
               .unionAll(clean.select(F.col("black").alias("player"), F.col("black_elo").alias("elo"),
                                      (1 - F.col("white_score")).alias("score")))
               .groupBy("player")
               .agg(F.count("*").alias("games"), F.round(F.avg("score"), 4).alias("score"),
                    F.round(F.avg("elo"), 0).alias("avg_rating"))
               .orderBy(F.desc("games")).limit(15))
top_players.show(); save(top_players, "c_top_players")

daily = clean.groupBy("day").agg(F.count("*").alias("games")).orderBy("day")
daily.show(5); save(daily, "c_daily_games")

hourly = clean.groupBy("hour").agg(F.count("*").alias("games")).orderBy("hour")
save(hourly, "c_hourly_games")

# Most frequent terms: explode the move text into tokens
terms = (clean.select(F.explode(F.split("moves", " ")).alias("move"))
              .withColumn("move", F.regexp_replace("move", r"[+#]", ""))
              .groupBy("move").count().orderBy(F.desc("count")).limit(15))
terms.show(); save(terms, "c_top_terms")

# Spark SQL ---------------------------------------------------------------
q_band = spark.sql("""
    SELECT elo_band, COUNT(*) AS games,
           ROUND(AVG(white_score), 4) AS white_score,
           ROUND(AVG(CASE WHEN result = '1/2-1/2' THEN 1.0 ELSE 0.0 END), 4) AS draw_rate,
           ROUND(AVG(plies), 1) AS avg_plies
    FROM games GROUP BY elo_band ORDER BY elo_band""")
q_band.show(); save(q_band, "c_sql_rating_band")

q_term = spark.sql("""
    SELECT category, termination, COUNT(*) AS games,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY category), 2) AS pct_in_category
    FROM games GROUP BY category, termination ORDER BY category, games DESC""")
q_term.show(30); save(q_term, "c_sql_termination")

q_eco = spark.sql("""
    SELECT SUBSTR(eco, 1, 1) AS eco_volume, COUNT(*) AS games,
           ROUND(AVG(white_score), 4) AS white_score
    FROM games WHERE eco <> '' GROUP BY SUBSTR(eco, 1, 1) ORDER BY eco_volume""")
q_eco.show(); save(q_eco, "c_sql_eco_volume")

q_upset = spark.sql("""
    SELECT CASE WHEN ABS(white_elo - black_elo) < 100 THEN '0-99'
                WHEN ABS(white_elo - black_elo) < 200 THEN '100-199'
                WHEN ABS(white_elo - black_elo) < 300 THEN '200-299'
                WHEN ABS(white_elo - black_elo) < 400 THEN '300-399' ELSE '400+' END AS gap,
           COUNT(*) AS decisive_games,
           ROUND(AVG(CASE WHEN (white_elo < black_elo AND result = '1-0')
                            OR (white_elo > black_elo AND result = '0-1') THEN 1.0 ELSE 0.0 END), 4) AS underdog_win_rate
    FROM games WHERE result <> '1/2-1/2' AND white_elo <> black_elo
    GROUP BY 1 ORDER BY 1""")
q_upset.show(); save(q_upset, "c_sql_upsets")

log["query_seconds"] = round(time.time() - t0, 1)
with open(f"{RESULTS}/part_c_log.json", "w") as f:
    json.dump(log, f, indent=2, default=str)
print(json.dumps(log, indent=2, default=str))
spark.stop()
