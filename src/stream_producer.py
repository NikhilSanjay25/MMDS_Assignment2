"""Publish chess games to a Kafka topic, one JSON message per finished game.

Two sources:
  replay  historic games from data/lichess_games.csv, sent at a fixed rate (games/second);
          event_time is the game's real UTC start time, so event-time windows are meaningful.
  live    games finishing on lichess.org right now.  The producer asks the public Lichess API
          for ongoing games on the TV channels (/api/tv/<channel>), follows them with
          /api/stream/games/<id> and publishes each game the moment it ends.  The id set is
          refreshed every minute.  Only public, anonymous endpoints are used, one request
          at a time, as the Lichess API guidelines ask.

Usage:
  python stream_producer.py replay --topic lichess-replay --limit 200000 --rate 4000
  python stream_producer.py live   --topic lichess-live   --duration 600
"""
import argparse
import csv
import json
import os
import random
import string
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import DATA, KAFKA_BOOTSTRAP  # noqa: E402

API = "https://lichess.org/api"
HEADERS = {"User-Agent": "mmds-spark-coursework (PySpark streaming demo)"}
TV_CHANNELS = ["bullet", "blitz", "rapid", "classical", "best", "ultraBullet"]
# Lichess game status codes -> the Termination values used in the PGN dumps
STATUS = {30: "Normal", 31: "Normal", 32: "Normal", 34: "Normal",   # mate, resign, stalemate, draw
          33: "Time forfeit", 35: "Time forfeit",                    # timeout, outoftime
          36: "Rules infraction"}                                    # cheat
SPEED = {"ultraBullet": "Bullet", "bullet": "Bullet", "blitz": "Blitz", "rapid": "Rapid",
         "classical": "Classical", "correspondence": "Classical"}


def to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def category(time_control):
    """Same rule as Part C: estimated duration = base + 40 * increment seconds."""
    try:
        base, inc = time_control.split("+")
        est = int(base) + 40 * int(inc)
    except ValueError:
        return None
    return "Bullet" if est < 180 else "Blitz" if est < 480 else "Rapid" if est < 1500 else "Classical"


def fresh_topic(name, partitions):
    """(Re)create the topic so every run starts from an empty, known state."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="mmds-admin")
    try:
        admin.delete_topics([name])
        time.sleep(3)
    except UnknownTopicOrPartitionError:
        pass
    for _ in range(10):
        try:
            admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
            break
        except TopicAlreadyExistsError:      # deletion still in progress
            time.sleep(2)
    admin.close()


def make_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, linger_ms=20, batch_size=256 * 1024)


def publish(producer, topic, msg):
    producer.send(topic, key=msg["game_id"].encode(), value=json.dumps(msg).encode())


def replay(producer, topic, limit, rate):
    sent, t0 = 0, time.time()
    with open(DATA, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if sent >= limit:
                break
            ts = f"{r['utc_date'].replace('.', '-')}T{r['utc_time']}Z"
            msg = {"game_id": r["game_id"], "white": r["white"], "black": r["black"],
                   "result": r["result"], "white_elo": to_int(r["white_elo"]),
                   "black_elo": to_int(r["black_elo"]), "eco": r["eco"],
                   "opening": r["opening"], "time_control": r["time_control"],
                   "category": category(r["time_control"]), "termination": r["termination"],
                   "num_moves": to_int(r["num_moves"]), "event_time": ts, "source": "replay"}
            publish(producer, topic, msg)
            sent += 1
            if sent % rate == 0:                       # pace: `rate` games per second
                producer.flush()
                ahead = sent / rate - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
                print(f"[replay] {sent} games sent", flush=True)
    producer.flush()
    return sent


def ongoing_games():
    """Ids (and metadata) of games currently shown on the Lichess TV channels."""
    import requests
    games = {}
    for ch in TV_CHANNELS:
        r = requests.get(f"{API}/tv/{ch}", params={"nb": 30, "moves": "false", "opening": "true"},
                         headers={**HEADERS, "Accept": "application/x-ndjson"}, timeout=20)
        if r.status_code == 429:
            time.sleep(60)
            continue
        for line in r.text.splitlines():
            if line.strip():
                g = json.loads(line)
                if g.get("variant") == "standard" and g.get("status") == "started":
                    games[g["id"]] = g
    return games


def to_message(ev, meta):
    """Turn a finished-game event from /api/stream/games into the replay message schema."""
    status = ev.get("status")
    if status not in STATUS or ev.get("variant") != "standard":
        return None                                  # still running, aborted, or a variant
    p = ev["players"]
    m = meta.get(ev["id"], {})
    clock = ev.get("clock") or m.get("clock") or {}
    tc = f"{clock.get('initial', 0)}+{clock.get('increment', 0)}" if clock else "-"
    winner = ev.get("winner")
    return {"game_id": ev["id"],
            "white": p["white"].get("userId", ""), "black": p["black"].get("userId", ""),
            "result": {"white": "1-0", "black": "0-1"}.get(winner, "1/2-1/2"),
            "white_elo": p["white"].get("rating"), "black_elo": p["black"].get("rating"),
            "eco": (m.get("opening") or {}).get("eco", ""),
            "opening": (m.get("opening") or {}).get("name", ""),
            "time_control": tc, "category": SPEED.get(ev.get("speed"), category(tc)),
            "termination": STATUS[status], "status": ev.get("statusName"),
            "num_moves": None,
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "source": "live"}


def live(producer, topic, duration, refresh=60):
    import requests
    sent, seen, end = 0, set(), time.time() + duration
    stream_id = "mmds" + "".join(random.choices(string.ascii_lowercase, k=10))
    while time.time() < end:
        meta = ongoing_games()
        ids = [i for i in meta if i not in seen][:500]
        if not ids:
            time.sleep(10)
            continue
        print(f"[live] following {len(ids)} ongoing games", flush=True)
        stop_at = min(end, time.time() + refresh)
        try:
            with requests.post(f"{API}/stream/games/{stream_id}", data=",".join(ids),
                               headers=HEADERS, stream=True, timeout=(10, 20)) as r:
                if r.status_code == 429:
                    print("[live] rate limited, waiting 60 s", flush=True)
                    time.sleep(60)
                    continue
                for line in r.iter_lines():
                    if line:
                        ev = json.loads(line)
                        msg = to_message(ev, meta)
                        if msg and ev["id"] not in seen:
                            seen.add(ev["id"])
                            publish(producer, topic, msg)
                            sent += 1
                            print(f"[live] {sent:5d} {msg['category']:<9} {msg['result']:<7} "
                                  f"{msg['termination']:<12} {msg['white']} vs {msg['black']}",
                                  flush=True)
                    if time.time() > stop_at:
                        break
        except requests.exceptions.RequestException as e:   # read timeout = quiet minute
            print(f"[live] stream reconnect ({type(e).__name__})", flush=True)
        producer.flush()
    return sent


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["replay", "live"])
    ap.add_argument("--topic")
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--limit", type=int, default=200000, help="replay: games to send")
    ap.add_argument("--rate", type=int, default=4000, help="replay: games per second")
    ap.add_argument("--duration", type=int, default=600, help="live: seconds to run")
    ap.add_argument("--warmup", type=int, default=15, help="seconds to wait for the consumer")
    a = ap.parse_args()
    topic = a.topic or f"lichess-{a.mode}"
    fresh_topic(topic, a.partitions)
    print(f"[{a.mode}] topic {topic} ready on {KAFKA_BOOTSTRAP}; starting in {a.warmup} s", flush=True)
    time.sleep(a.warmup)
    producer = make_producer()
    t0 = time.time()
    n = replay(producer, topic, a.limit, a.rate) if a.mode == "replay" else live(producer, topic, a.duration)
    producer.close()
    print(f"[{a.mode}] done: {n} games published to {topic} in {time.time() - t0:.1f} s", flush=True)


if __name__ == "__main__":
    main()
