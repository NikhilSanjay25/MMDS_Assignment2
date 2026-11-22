import glob
import os
import sys
import time

from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# MMDS_DATA lets the CSV live on a fast native disk (e.g. ~/mmds/data inside WSL)
DATA = os.environ.get("MMDS_DATA", os.path.join(ROOT, "data", "lichess_games.csv"))
RESULTS = os.path.join(ROOT, "results")
# Spark-Kafka connector jars (downloaded by scripts/fetch_vendor.sh into vendor/jars)
KAFKA_JARS = sorted(glob.glob(os.path.join(os.environ.get("MMDS_JARS", os.path.join(ROOT, "vendor", "jars")), "*.jar")))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
if os.name == "nt":  # only Windows needs a winutils stand-in
    os.environ.setdefault("HADOOP_HOME", os.path.join(ROOT, "hadoop"))
os.environ["TZ"] = "UTC"   # PySpark converts collected timestamps with the process time zone
if hasattr(time, "tzset"):
    time.tzset()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(name, master=None, **conf):
    """SparkSession in local mode unless SPARK_MASTER (e.g. spark://host:7077) is set.

    Extra config is passed as keyword arguments with '.' written as '__'.
    """
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    builder = SparkSession.builder.master(master)
    conf.setdefault("spark__driver__memory", os.environ.get("MMDS_DRIVER_MEMORY", "6g"))
    conf.setdefault("spark__sql__shuffle__partitions", "16")
    conf.setdefault("spark__sql__session__timeZone", "UTC")   # Lichess timestamps are UTC
    for k, v in conf.items():
        builder = builder.config(k.replace("__", "."), v)
    spark = (builder.appName(name)
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def save(df, name):
    """Write a small Spark result to results/<name>.csv via pandas."""
    df.toPandas().to_csv(os.path.join(RESULTS, name + ".csv"), index=False)


def environment():
    """Facts about the machine and Spark build, recorded next to every result file."""
    import platform

    import pyspark
    return {"python": platform.python_version(), "pyspark": pyspark.__version__,
            "os": f"{platform.system()} {platform.release()}", "cpus": os.cpu_count(),
            "master": os.environ.get("SPARK_MASTER", "local[*]")}
