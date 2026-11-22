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


def have(name):
    p = os.path.join(R, name + ".csv")
    return os.path.exists(p) and os.path.getsize(p) > 5


def save(fig, name):
    """PDF for the LaTeX report, PNG (figures/png/) for the slides."""
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name + ".pdf"))
    os.makedirs(os.path.join(FIG, "png"), exist_ok=True)
    fig.savefig(os.path.join(FIG, "png", name + ".png"), dpi=200)
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
ax.set_title("Spark Streaming (DStream): records per micro-batch")
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
# ---- Structured Streaming (socket) -------------------------------------------------------
if have("e_ss_batches"):
    e = read("e_ss_batches")
    fig, ax = plt.subplots(figsize=(6, 2.4))
    ax.bar(range(1, len(e) + 1), e.records, color=BLUE)
    ax.set_xlabel("micro-batch #")
    ax.set_ylabel("records in batch")
    ax.set_title("Structured Streaming (socket): records per micro-batch")
    save(fig, "ss_batches")

# ---- Structured Streaming (Kafka replay) -------------------------------------------------
if have("e_kafka_replay_batches"):
    e = read("e_kafka_replay_batches")
    p = read("e_kafka_replay_progress")
    p = p[p["query"] == "per_batch"]
    fig, ax = plt.subplots(figsize=(6, 2.4))
    ax.bar(range(1, len(e) + 1), e.records / 1000, color=BLUE, label="records (thousands)")
    ax.set_xlabel("micro-batch #")
    ax.set_ylabel("records (thousands)")
    ax2 = ax.twinx()
    ax2.plot(range(1, len(p) + 1), p.batch_ms / 1000, color=ORANGE, marker=".", label="batch time (s)")
    ax2.set_ylabel("batch time (s)")
    ax2.spines["right"].set_visible(True)
    ax.set_title("Kafka replay: records and processing time per micro-batch")
    save(fig, "kafka_replay_batches")

if have("e_kafka_replay_windows"):
    w = read("e_kafka_replay_windows")
    w["window_start"] = pd.to_datetime(w.window_start)
    pv = w.pivot_table(index="window_start", columns="category", values="games", aggfunc="sum").fillna(0)
    fig, ax = plt.subplots(figsize=(6, 2.6))
    for c in [c for c in ["Bullet", "Blitz", "Rapid", "Classical"] if c in pv.columns]:
        ax.plot(pv.index, pv[c], lw=1, label=c)
    ax.set_ylabel("games per 1-hour window")
    ax.set_ylim(0, pv.values.max() * 1.25)          # headroom for the legend
    ax.set_title("Kafka replay: event-time windows (1 h, watermark 2 h)")
    ax.legend(frameon=False, ncol=4, fontsize=7)
    fig.autofmt_xdate()
    save(fig, "kafka_replay_windows")

# ---- Structured Streaming (Kafka, live lichess.org feed) ---------------------------------
if have("e_kafka_live_windows"):
    w = read("e_kafka_live_windows")
    w["window_start"] = pd.to_datetime(w.window_start)
    pv = w.pivot_table(index="window_start", columns="category", values="games", aggfunc="sum").fillna(0)
    order = [c for c in ["Bullet", "Blitz", "Rapid", "Classical"] if c in pv.columns]
    fig, ax = plt.subplots(figsize=(6, 2.4))
    bottom = None
    for c in order:
        ax.bar(pv.index, pv[c], width=0.0006, bottom=bottom, label=c)
        bottom = pv[c] if bottom is None else bottom + pv[c]
    ax.set_ylabel("games finished")
    ax.set_ylim(0, bottom.max() * 1.3)               # headroom for the legend
    ax.set_title("Live lichess.org feed: games finished per 1-minute window")
    ax.legend(frameon=False, ncol=4, fontsize=7)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
    save(fig, "kafka_live_windows")

# ---- Cluster scaling -----------------------------------------------------------------------
if have("f_cluster_benchmark"):
    b = read("f_cluster_benchmark")
    fig, axs = plt.subplots(1, 2, figsize=(6.4, 3.2))
    for job, col, lab in (("rdd", ORANGE, "RDD job (Python)"), ("df", BLUE, "DataFrame job (JVM)")):
        d = b[b.job == job]
        axs[0].plot(d.executors, d.seconds, marker="o", color=col, label=lab)
        axs[1].plot(d.executors, d.speedup, marker="o", color=col, label=lab)
    ex = sorted(b.executors.unique())
    axs[1].plot(ex, [e / ex[0] for e in ex], ls="--", color="grey", lw=0.8, label="ideal")
    for ax in axs:
        ax.set_xscale("log", base=2)
        ax.set_xticks(ex)
        ax.set_xticklabels(ex)
        ax.set_xlabel("executors (2 cores each)")
    axs[0].set_ylabel("wall time (s)")
    axs[1].set_ylabel("speed-up vs 1 executor")
    axs[0].legend(frameon=False, fontsize=7)
    axs[1].legend(frameon=False, fontsize=7)
    fig.suptitle("Spark Standalone cluster: scaling with executors", fontsize=9)
    save(fig, "cluster_scaling")

print("figures written")
