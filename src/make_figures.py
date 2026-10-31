"""Plot the Spark results (results/*.csv) into figures/*.pdf for the LaTeX report."""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R, FIG = os.path.join(ROOT, "results"), os.path.join(ROOT, "figures")
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
BLUE, ORANGE = "#2a6fb0", "#e08a1e"


def read(name):
    return pd.read_csv(os.path.join(R, name + ".csv"))


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name + ".pdf"))
    plt.close(fig)


d = read("c_daily_games")
d = d[d.games > 5000]
fig, ax = plt.subplots(figsize=(6, 2.4))
ax.plot(pd.to_datetime(d.day), d.games, color=BLUE)
ax.set_ylabel("games / day")
ax.set_title("Games played per day (Jan-Feb 2014)")
fig.autofmt_xdate()
save(fig, "daily_games")

h = read("c_hourly_games")
fig, ax = plt.subplots(figsize=(6, 2.4))
ax.bar(h.hour, h.games / 1000, color=BLUE)
ax.set_xlabel("hour of day (UTC)")
ax.set_ylabel("games (thousands)")
ax.set_title("Games per hour of day")
save(fig, "hourly_games")

b = read("c_sql_upsets")
fig, ax = plt.subplots(figsize=(4, 2.6))
ax.bar(b.gap, b.underdog_win_rate * 100, color=ORANGE)
ax.axhline(50, color="grey", ls="--", lw=0.8)
ax.set_xlabel("rating gap")
ax.set_ylabel("underdog win rate (%)")
ax.set_title("Upset probability vs rating gap")
save(fig, "upsets")

bd = read("c_sql_rating_band")
bd = bd[bd.games > 1000]
fig, ax = plt.subplots(figsize=(4, 2.6))
ax.plot(bd.elo_band, bd.draw_rate * 100, marker="o", color=BLUE)
ax.set_xlabel("average rating band")
ax.set_ylabel("draw rate (%)")
ax.set_title("Draw rate grows with rating")
save(fig, "draw_rate")

o = read("c_top_openings").head(10).iloc[::-1]
fig, ax = plt.subplots(figsize=(6, 3))
ax.barh(o.opening, o.games / 1000, color=BLUE)
ax.set_xlabel("games (thousands)")
ax.set_title("Ten most played openings")
save(fig, "top_openings")

e = read("e_stream_batches")
fig, ax = plt.subplots(figsize=(6, 2.4))
ax.bar(range(len(e)), e.records, color=BLUE)
ax.set_xlabel("micro-batch #")
ax.set_ylabel("records in batch")
ax.set_title("Structured Streaming: records per micro-batch")
save(fig, "stream_batches")

r = read("e_stream_running")
r = r.pivot_table(index="batch_id", columns="termination", values="games")
fig, ax = plt.subplots(figsize=(6, 2.4))
for c in r.columns:
    ax.plot(r.index, r[c], marker=".", label=c)
ax.set_xlabel("micro-batch id")
ax.set_ylabel("cumulative games")
ax.set_title("Running aggregation by termination type")
ax.legend(frameon=False)
save(fig, "stream_running")
print("figures written")
