"""Stream one or more Lichess .pgn.zst dumps into one flat CSV (one row per game).

Games from all inputs are merged and de-duplicated on game id.

Usage: python src/parse_pgn.py data/raw/<month1>.pgn.zst [data/raw/<month2>.pgn.zst ...] data/lichess_games.csv
"""
import csv
import io
import re
import sys

import zstandard

FIELDS = ["game_id", "white", "black", "result", "white_elo", "black_elo",
          "white_diff", "black_diff", "eco", "opening", "time_control",
          "termination", "utc_date", "utc_time", "num_moves", "moves"]
TAG = re.compile(r'^\[(\w+) "(.*)"\]$')
COMMENT = re.compile(r"\{[^}]*\}")
MOVENUM = re.compile(r"\d+\.(\.\.)?")


def clean_moves(text):
    text = COMMENT.sub("", text)
    tokens = [t for t in MOVENUM.sub(" ", text).split()
              if t not in ("1-0", "0-1", "1/2-1/2", "*") and not t.startswith("$")]
    return tokens


def games(stream):
    tags, movetext = {}, []
    for line in stream:
        line = line.strip()
        if not line:
            continue
        m = TAG.match(line)
        if m:
            if movetext:
                yield tags, " ".join(movetext)
                tags, movetext = {}, []
            tags[m.group(1)] = m.group(2)
        else:
            movetext.append(line)
    if tags and movetext:
        yield tags, " ".join(movetext)


def main(srcs, dst):
    n, dup, seen = 0, 0, set()
    with open(dst, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(FIELDS)
        for src in srcs:
            per_file = 0
            with open(src, "rb") as fh:
                reader = zstandard.ZstdDecompressor().stream_reader(fh)
                text = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
                for t, mt in games(text):
                    gid = t.get("Site", "").rsplit("/", 1)[-1]
                    if gid in seen:
                        dup += 1
                        continue
                    seen.add(gid)
                    toks = clean_moves(mt)
                    w.writerow([
                        gid, t.get("White", ""), t.get("Black", ""),
                        t.get("Result", ""), t.get("WhiteElo", ""), t.get("BlackElo", ""),
                        t.get("WhiteRatingDiff", ""), t.get("BlackRatingDiff", ""),
                        t.get("ECO", ""), t.get("Opening", ""), t.get("TimeControl", ""),
                        t.get("Termination", ""), t.get("UTCDate", t.get("Date", "")),
                        t.get("UTCTime", ""), len(toks), " ".join(toks)])
                    n += 1
                    per_file += 1
                    if n % 200000 == 0:
                        print(f"{n} games parsed", flush=True)
            print(f"{src}: {per_file} games", flush=True)
    print(f"done: {n} games ({dup} duplicates skipped) -> {dst}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    main(sys.argv[1:-1], sys.argv[-1])
