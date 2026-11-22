#!/usr/bin/env bash
# One-time setup on Linux / WSL2 (no sudo needed).  Installs into $MMDS_HOME (default ~/mmds):
#   Java 17, Kafka 3.9 (KRaft), Python 3.11 venv with PySpark 3.5.3, and a copy of the
#   game CSV on the Linux filesystem (reading it through /mnt/c is several times slower).
# Afterwards:  source ~/mmds/env.sh
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
MMDS="${MMDS_HOME:-$HOME/mmds}"
V="$REPO/vendor"
mkdir -p "$MMDS"/{data,logs,run,bin}

bash "$REPO/scripts/fetch_vendor.sh"

if [ ! -x "$MMDS/jdk/bin/java" ]; then
  echo "installing Java 17"; mkdir -p "$MMDS/jdk"
  tar -xzf "$V/jdk17-linux-x64.tar.gz" -C "$MMDS/jdk" --strip-components=1
fi
if [ ! -x "$MMDS/kafka/bin/kafka-server-start.sh" ]; then
  echo "installing Kafka"; mkdir -p "$MMDS/kafka"
  tar -xzf "$V/kafka_2.13-3.9.1.tgz" -C "$MMDS/kafka" --strip-components=1
fi
if [ ! -x "$MMDS/bin/uv" ]; then
  tar -xzf "$V/uv-linux.tar.gz" -C "$MMDS/bin" --strip-components=1
fi
export UV_PYTHON_INSTALL_DIR="$MMDS/python" UV_CACHE_DIR="$MMDS/uv-cache"
[ -x "$MMDS/venv/bin/python" ] || "$MMDS/bin/uv" venv --python 3.11 "$MMDS/venv"
"$MMDS/bin/uv" pip install --python "$MMDS/venv/bin/python" -r "$REPO/requirements.txt"

CSV="$MMDS/data/lichess_games.csv"
if [ ! -s "$CSV" ]; then
  if [ -s "$REPO/data/lichess_games.csv" ]; then
    echo "copying game CSV to the Linux filesystem"; cp "$REPO/data/lichess_games.csv" "$CSV"
  else
    echo "parsing the PGN dumps"
    "$MMDS/venv/bin/python" "$REPO/src/parse_pgn.py" "$REPO"/data/raw/lichess_db_standard_rated_2014-0{1,2}.pgn.zst "$CSV"
  fi
fi

cat > "$MMDS/env.sh" <<EOF
export MMDS_HOME="$MMDS"
export MMDS_REPO="$REPO"
export JAVA_HOME="$MMDS/jdk"
export PATH="$MMDS/venv/bin:$MMDS/jdk/bin:$MMDS/kafka/bin:\$PATH"
export MMDS_DATA="$CSV"
export MMDS_JARS="$REPO/vendor/jars"
export SPARK_LOCAL_IP=127.0.0.1
EOF
source "$MMDS/env.sh"
java -version 2>&1 | head -1
python -c "import pyspark, sys; print('python', sys.version.split()[0], 'pyspark', pyspark.__version__)"
echo "setup complete: source $MMDS/env.sh"
