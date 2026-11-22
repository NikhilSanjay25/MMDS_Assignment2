#!/usr/bin/env bash
# Download everything the Linux setup needs into vendor/ (JDK 17, Kafka, uv, Spark-Kafka jars)
# and the two Lichess monthly dumps into data/raw/.  Safe to re-run: existing files are kept.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
V="$REPO/vendor"
M=https://repo1.maven.org/maven2
mkdir -p "$V/jars" "$REPO/data/raw"

get() {  # get <url> <dest>
  [ -s "$2" ] && { echo "have $(basename "$2")"; return; }
  echo "downloading $(basename "$2")"
  curl -sSfL -o "$2.part" "$1" && mv "$2.part" "$2"
}

get "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse" "$V/jdk17-linux-x64.tar.gz"
get https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz "$V/kafka_2.13-3.9.1.tgz"
get https://github.com/astral-sh/uv/releases/download/0.5.31/uv-x86_64-unknown-linux-gnu.tar.gz "$V/uv-linux.tar.gz"
get $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar "$V/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar"
get $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar "$V/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
get $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar "$V/jars/kafka-clients-3.4.1.jar"
get $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar "$V/jars/commons-pool2-2.11.1.jar"
for m in 01 02; do
  get "https://database.lichess.org/standard/lichess_db_standard_rated_2014-$m.pgn.zst" \
      "$REPO/data/raw/lichess_db_standard_rated_2014-$m.pgn.zst"
done
echo "vendor/ and data/raw/ are complete"
