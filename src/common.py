import os
import sys

from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "lichess_games.csv")
RESULTS = os.path.join(ROOT, "results")
os.environ.setdefault("HADOOP_HOME", os.path.join(ROOT, "hadoop"))  # local winutils stand-in
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(name, master="local[*]", **conf):
    builder = SparkSession.builder.master(master)
    for k, v in conf.items():
        builder = builder.config(k.replace("__", "."), v)
    spark = (builder.appName(name)
             .config("spark.driver.memory", "6g")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def save(df, name):
    """Write a small Spark result to results/<name>.csv via pandas."""
    df.toPandas().to_csv(os.path.join(RESULTS, name + ".csv"), index=False)
