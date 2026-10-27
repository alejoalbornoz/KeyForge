#!/usr/bin/env bash
# Compares the Netty and the virtual-thread server at several connection counts.
#
# Each run starts a fresh server container and a load-generator container on one Docker network, pinned to disjoint
# CPUs. The load generator shares the server's PID namespace (--pid container:...) so it can sample the server's RSS,
# thread count and CPU time from /proc/1. Results are appended to $OUT/results.csv, one row per run, with the
# latency distribution of each run in $OUT/<label>-<connections>c-<rate>.hgrm and its console output in $OUT/*.log.
#
# Build the jars first:   ./mvnw -q package -DskipTests
# Then:                   keyforge-bench/scripts/run-benchmarks.sh
# Smaller smoke run:      CONNECTIONS=1000 RATES=0 WARMUP=2 DURATION=5 keyforge-bench/scripts/run-benchmarks.sh
#
# Works on Linux, macOS and Git Bash on Windows (Docker Desktop).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
IMAGE=${IMAGE:-eclipse-temurin:25-jdk}
SERVERS=${SERVERS:-"netty virtual-threads"}
CONNECTIONS=${CONNECTIONS:-"1000 10000 50000"}
# Offered requests per second over all connections; 0 means closed loop (as fast as the server answers).
RATES=${RATES:-"0 20000 40000"}
WARMUP=${WARMUP:-10}
DURATION=${DURATION:-30}
SETTLE=${SETTLE:-5}
SERVER_CPUS=${SERVER_CPUS:-0,1}
CLIENT_CPUS=${CLIENT_CPUS:-2,3}
# Both servers get the same JVM options: the JDK defaults (G1) plus a heap cap. GC events go to the server log.
SERVER_JAVA_OPTS=${SERVER_JAVA_OPTS:-"-Xmx2g -XX:+UseG1GC -Xlog:gc:stdout"}
# The load generator runs ZGC, whose sub-millisecond pauses keep its own stalls out of the measured latencies.
CLIENT_JAVA_OPTS=${CLIENT_JAVA_OPTS:-"-Xmx2g -XX:+UseZGC"}
OUT=${OUT:-$ROOT/docs/benchmarks/results/$(date +%Y-%m-%d)}
NETWORK=keyforge-bench

VERSION=0.1.0-SNAPSHOT
SERVER_JAR=$ROOT/keyforge-server/target/keyforge-server-$VERSION-all.jar
BENCH_JAR=$ROOT/keyforge-bench/target/keyforge-bench-$VERSION-all.jar
for jar in "$SERVER_JAR" "$BENCH_JAR"; do
  [[ -f $jar ]] || { echo "Missing $jar; run ./mvnw -q package -DskipTests first" >&2; exit 1; }
done

# Git Bash: keep MSYS from rewriting container paths such as /app, and give Docker Windows paths for volumes.
export MSYS_NO_PATHCONV=1
host_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

mkdir -p "$OUT"
docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "image: $IMAGE ($(docker image inspect --format '{{.Id}}' "$IMAGE"))"
  echo "docker: $(docker info --format '{{.ServerVersion}}, {{.OperatingSystem}}, kernel {{.KernelVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
  echo "server: cpus $SERVER_CPUS, java $SERVER_JAVA_OPTS"
  echo "client: cpus $CLIENT_CPUS, java $CLIENT_JAVA_OPTS"
  echo "warmup ${WARMUP}s, duration ${DURATION}s, settle ${SETTLE}s"
  docker run --rm "$IMAGE" java -version 2>&1
} > "$OUT/environment.txt"

cleanup() { docker rm -f kf-server >/dev/null 2>&1 || true; }
trap cleanup EXIT

for connections in $CONNECTIONS; do
  for rate in $RATES; do
    for server in $SERVERS; do
      name="$server-${connections}c-$( [[ $rate == 0 ]] && echo max || echo "${rate}rps" )"
      echo "=== $name"
      cleanup
      docker run -d --name kf-server --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" \
        --sysctl net.core.somaxconn=65535 \
        -v "$(host_path "$SERVER_JAR"):/app/server.jar:ro" \
        "$IMAGE" java $SERVER_JAVA_OPTS -jar /app/server.jar \
          --server.type="$server" --server.bind=0.0.0.0 --server.port=6379 >/dev/null

      docker run --rm --name kf-load --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" \
        --pid container:kf-server \
        --sysctl net.ipv4.ip_local_port_range="1024 65535" \
        -v "$(host_path "$BENCH_JAR"):/app/bench.jar:ro" \
        -v "$(host_path "$OUT"):/results" \
        "$IMAGE" java $CLIENT_JAVA_OPTS -Xlog:gc:file=/results/"$name".client-gc.log -jar /app/bench.jar \
          --host=kf-server --port=6379 --connections="$connections" --rate="$rate" \
          --warmup-seconds="$WARMUP" --duration-seconds="$DURATION" --settle-seconds="$SETTLE" \
          --label="$server" --server-pid=1 --output=/results/results.csv \
        2>&1 | tee "$OUT/$name.log"

      docker logs kf-server > "$OUT/$name.server.log" 2>&1
    done
  done
done

echo "Results: $OUT/results.csv"
