# MMDS Assignment 2 - Scalable analytics with Spark on Lichess games

Dataset: Lichess Open Database, Jan + Feb 2014 (1,389,994 rated games), https://database.lichess.org

```
python -m pip install pyspark==3.5.3 zstandard pandas matplotlib
curl -L -o data/raw/lichess_db_standard_rated_2014-01.pgn.zst https://database.lichess.org/standard/lichess_db_standard_rated_2014-01.pgn.zst
curl -L -o data/raw/lichess_db_standard_rated_2014-02.pgn.zst https://database.lichess.org/standard/lichess_db_standard_rated_2014-02.pgn.zst
python src/parse_pgn.py data/raw/<file>.pgn.zst data/<month>.csv   # then concatenate the months into data/lichess_games.csv
cd src
python part_b_rdd.py            # Part B  RDDs
python part_c_dataframe.py      # Part C  DataFrames + Spark SQL
python part_d_pipeline.py       # Part D  RDD -> DataFrame workflow
python part_e_streaming.py      # Part E  streaming (DStream micro-batches; runs on Windows)
python make_figures.py          # figures/*.pdf for the report
```
`part_e_structured_streaming.py` is the Structured Streaming version; it needs Hadoop native
libraries on Windows (or run it on Linux/WSL/Docker) and was not executed here.
`tools/winutils_shim.cs` is a tiny local stand-in for `winutils.exe` (build with
`csc.exe -out:hadoop/bin/winutils.exe tools/winutils_shim.cs`); `common.py` points `HADOOP_HOME` at `hadoop/`.

Report: `report/report.tex` (compile with `pdflatex report.tex`, figures are read from `../figures`).
