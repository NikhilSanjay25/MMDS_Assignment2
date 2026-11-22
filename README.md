# MMDS Assignment 2 - Scalable analytics with Spark on Lichess games

Dataset: Lichess Open Database, Jan + Feb 2014 (1,389,994 rated games), https://database.lichess.org

Everything runs on **Linux or WSL2** (Windows itself is not needed: Spark Structured Streaming's
checkpoints need Hadoop native libraries that Windows lacks). No sudo is required.

## Setup (once)

```bash
bash scripts/setup_linux.sh     # downloads JDK 17, Kafka 3.9, uv, Spark-Kafka jars and the two
                                # Lichess dumps into vendor/ and data/raw/, builds a Python 3.11 venv
                                # with PySpark 3.5.3 and the game CSV, all under ~/mmds
source ~/mmds/env.sh
```

`src/parse_pgn.py` turns the PGN dumps into one CSV (merged, de-duplicated on game id):
`python src/parse_pgn.py data/raw/*2014-01.pgn.zst data/raw/*2014-02.pgn.zst data/lichess_games.csv`
(setup does this automatically when the CSV is missing).

## Run

```bash
bash scripts/run_all.sh                 # every part, logs in results/logs/ (~25 min)
bash scripts/run_all.sh b c d           # or individual steps
```

| Step | Script | What it does |
|---|---|---|
| `b` | `part_b_rdd.py` | Part B, RDDs: map / filter / flatMap / reduceByKey |
| `c` | `part_c_dataframe.py` | Part C, DataFrames + Spark SQL |
| `d` | `part_d_pipeline.py` | Part D, timed Input -> RDD -> DataFrame -> Output workflow |
| `dstream` | `part_e_streaming.py` | Part E, classic Spark Streaming (DStream) over a socket feed |
| `ss` | `part_e_structured_streaming.py` | Part E, Structured Streaming over the same socket feed |
| `kafka-replay` | `stream_producer.py replay` + `part_e_kafka_streaming.py` | Kafka topic with 200k replayed games; event-time windows, watermark, sampling, Bloom filter |
| `kafka-live` | `stream_producer.py live` + `part_e_kafka_streaming.py` | Kafka topic fed by games finishing **live on lichess.org** (public API) |
| `cluster` | `part_f_cluster_benchmark.py` | Spark Standalone cluster (master + 8 workers), scaling 1 -> 8 executors |
| `figures` | `make_figures.py` | `figures/*.pdf` for the report |

Helpers: `scripts/kafka.sh start|stop` (single-node KRaft broker on `localhost:9092`),
`scripts/cluster.sh start N|stop` (master UI on http://localhost:8080).
`LIVE_SECONDS=300 bash scripts/run_all.sh kafka-live` shortens the live run.

Report: `report/report.tex` (compile with `pdflatex report.tex` or `tectonic report.tex`;
figures are read from `../figures`).

`tools/winutils_shim.cs` is only for running Parts B-D natively on Windows
(build with `csc.exe -out:hadoop/bin/winutils.exe tools/winutils_shim.cs`).
