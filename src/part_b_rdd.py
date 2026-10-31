"""Part B: load the Lichess games with RDDs and apply map/filter/flatMap/reduceByKey."""
import csv
import json
import time

from common import DATA, RESULTS, get_spark

spark = get_spark("PartB-RDD")
sc = spark.sparkContext
out = {}
t0 = time.time()

# RDD creation -------------------------------------------------------------
lines = sc.textFile(DATA, minPartitions=24)
HEADER_PREFIX = "game_id,"  # first() would end the worker early, which crashes on Windows
out["num_partitions"] = lines.getNumPartitions()
out["total_lines_incl_header"] = lines.count()

# map(): parse each CSV line into a tuple of fields (csv module handles quoted commas)
rows = (lines.filter(lambda l: not l.startswith(HEADER_PREFIX))
             .map(lambda l: next(csv.reader([l]))))
rows.cache()
out["num_games"] = rows.count()
# Columns: 0 id,1 white,2 black,3 result,4 welo,5 belo,6 wdiff,7 bdiff,8 eco,
#          9 opening,10 tc,11 termination,12 date,13 time,14 nmoves,15 moves

# 1) Opening frequency  (map -> reduceByKey -> takeOrdered)
opening_freq = (rows.map(lambda r: (r[9], 1)).reduceByKey(lambda a, b: a + b))
out["top_openings"] = opening_freq.takeOrdered(15, key=lambda kv: -kv[1])
out["distinct_openings"] = opening_freq.count()

# 2) Word count over the move text  (flatMap -> map -> reduceByKey)
#    Strip check/capture marks so that 'Qh5+' and 'Qh5' are the same token.
move_freq = (rows.flatMap(lambda r: r[15].split())
                 .map(lambda m: (m.rstrip("+#"), 1))
                 .reduceByKey(lambda a, b: a + b))
out["top_moves"] = move_freq.takeOrdered(15, key=lambda kv: -kv[1])
out["total_moves"] = rows.map(lambda r: int(r[14])).sum()

# 3) First-move frequency  (map on a flatMap-free key)
first_move = (rows.filter(lambda r: r[15]).map(lambda r: (r[15].split()[0], 1))
                  .reduceByKey(lambda a, b: a + b))
out["first_moves"] = first_move.takeOrdered(8, key=lambda kv: -kv[1])

# 4) Most active players  (flatMap emits both colours of every game)
players = (rows.flatMap(lambda r: [(r[1], 1), (r[2], 1)])
               .reduceByKey(lambda a, b: a + b))
out["top_players"] = players.takeOrdered(15, key=lambda kv: -kv[1])
out["distinct_players"] = players.count()

# 5) filter(): decisive games with a rating gap over 300 -> how often does the underdog win?
def underdog_win(r):
    try:
        w, b = int(r[4]), int(r[5])
    except ValueError:
        return None
    if abs(w - b) < 300 or r[3] not in ("1-0", "0-1"):
        return None
    white_is_underdog = w < b
    white_won = r[3] == "1-0"
    return white_is_underdog == white_won

gap = rows.map(underdog_win).filter(lambda x: x is not None)
n_gap = gap.count()
out["gap300_games"] = n_gap
out["gap300_underdog_wins"] = gap.filter(lambda x: x).count()

# 6) Result distribution  (reduceByKey with action collectAsMap)
out["results"] = rows.map(lambda r: (r[3], 1)).reduceByKey(lambda a, b: a + b).collectAsMap()

# 7) Termination types
out["terminations"] = (rows.map(lambda r: (r[11], 1)).reduceByKey(lambda a, b: a + b)
                           .collectAsMap())

out["elapsed_seconds"] = round(time.time() - t0, 1)
with open(f"{RESULTS}/part_b_rdd.json", "w") as f:
    json.dump(out, f, indent=2)
print(json.dumps(out, indent=2))
spark.stop()
