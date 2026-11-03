#!/usr/bin/env bash
# Compares KeyForge with real Redis on the same machine, two ways:
#
#   1. redis-benchmark (closed loop): SET, GET and INCR with and without pipelining. Measures throughput; its latencies
#      are those of a closed loop and suffer from coordinated omission.
#   2. KeyForge's load generator (open loop): 80% GET / 20% SET at fixed offered rates. Every latency is measured from
#      the request's intended send time, so a stall counts against every request it delays (no coordinated omission),
#      and the full distribution is kept in HdrHistogram files.
#
# Targets (TARGETS), each started fresh:
#   redis             Redis 8, no persistence (--save "" --appendonly no)
#   redis-aof         Redis 8, append-only file fsynced every second (--appendonly yes --appendfsync everysec)
#   keyforge-memory   KeyForge single node, Netty, in-memory engine
#   keyforge-lsm      KeyForge single node, Netty, LSM engine with group commit (the default configuration)
#   keyforge-lsm-vt   the same with the virtual-thread front end, whose connections do not share a blocked thread
#   cluster-leader    KeyForge 3-node Raft cluster (virtual threads, LSM), clients connected to the leader
#   cluster-follower  the same cluster, clients connected to a follower, which forwards to the leader
#
# Servers (all three cluster nodes too) are pinned to SERVER_CPUS, clients to CLIENT_CPUS; everything runs on one
# Docker network, so no traffic crosses Docker Desktop's port proxy. Server CPU time comes from the server containers'
# cgroups and is reported per request.
#
# Output in $OUT: redis-benchmark.csv (one row per target, pipeline depth and command), latency.csv (one row per
# open-loop run), <target>-<connections>c-<rate>rps.hgrm (HdrHistogram percentile distributions), *.log, and charts/
# (SVG charts and summary.md, written by io.keyforge.bench.report.BenchmarkReport).
#
# Build the jars first:   ./mvnw -q package -DskipTests
# Then:                   keyforge-bench/scripts/compare-with-redis.sh
# Smaller smoke run:      TARGETS="redis keyforge-memory" REQUESTS=50000 PIPELINED_REQUESTS=200000 \
#                           WARMUP_REQUESTS=10000 RATES=10000 WARMUP=2 DURATION=5 \
#                           keyforge-bench/scripts/compare-with-redis.sh
# Charts only:            REPORT_ONLY=1 OUT=docs/benchmarks/results/<date>/redis-comparison keyforge-bench/scripts/...
# CPU profile (JFR):      PROFILE=1 writes <target>-<phase>-<container>.jfr for every KeyForge server (settings=profile)
#
# Works on Linux, macOS and Git Bash on Windows (Docker Desktop).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
JAVA_IMAGE=${JAVA_IMAGE:-eclipse-temurin:25-jdk}
REDIS_IMAGE=${REDIS_IMAGE:-redis:8}
TARGETS=${TARGETS:-"redis redis-aof keyforge-memory keyforge-lsm keyforge-lsm-vt cluster-leader cluster-follower"}
SERVER_CPUS=${SERVER_CPUS:-0,1}
CLIENT_CPUS=${CLIENT_CPUS:-2,3}
SERVER_JAVA_OPTS=${SERVER_JAVA_OPTS:-"-Xmx2g -XX:+UseG1GC -Xlog:gc:stdout"}
# Extra KeyForge options for every KeyForge server, for example SERVER_ARGS=--metrics.port=9464 to measure metrics.
SERVER_ARGS=${SERVER_ARGS:-}
CLIENT_JAVA_OPTS=${CLIENT_JAVA_OPTS:-"-Xmx1g -XX:+UseZGC"}

# redis-benchmark: clients, commands, pipeline depths, and requests per command: for a single node without and with
# pipelining (enough for several seconds each), and for the cluster. Before measuring, every target gets the same
# untimed warm-up (WARMUP_REQUESTS of each command), so that the JVM's JIT compilation is not counted.
CLIENTS=${CLIENTS:-50}
TESTS=${TESTS:-"set get incr"}
PIPELINES=${PIPELINES:-"1 16"}
REQUESTS=${REQUESTS:-1000000}
PIPELINED_REQUESTS=${PIPELINED_REQUESTS:-5000000}
CLUSTER_REQUESTS=${CLUSTER_REQUESTS:-30000}
WARMUP_REQUESTS=${WARMUP_REQUESTS:-500000}
CLUSTER_WARMUP_REQUESTS=${CLUSTER_WARMUP_REQUESTS:-5000}
BENCH_THREADS=${BENCH_THREADS:-2}
KEYSPACE=${KEYSPACE:-100000}
VALUE_SIZE=${VALUE_SIZE:-64}

# Load generator: connections and offered rates (requests per second) for a single node and for the cluster.
LOAD_CONNECTIONS=${LOAD_CONNECTIONS:-50}
RATES=${RATES:-"20000 60000"}
CLUSTER_RATES=${CLUSTER_RATES:-"500 1000"}
WARMUP=${WARMUP:-10}
DURATION=${DURATION:-30}

OUT=${OUT:-$ROOT/docs/benchmarks/results/$(date +%Y-%m-%d)/redis-comparison}
NETWORK=keyforge-bench
PROFILE=${PROFILE:-0}
REPORT_ONLY=${REPORT_ONLY:-0}

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

report() {
  docker run --rm -v "$(host_path "$BENCH_JAR"):/app/bench.jar:ro" -v "$(host_path "$OUT"):/results" \
    "$JAVA_IMAGE" java -cp /app/bench.jar io.keyforge.bench.report.BenchmarkReport /results
  echo "Results: $OUT (charts in $OUT/charts)"
}
if [[ $REPORT_ONLY == 1 ]]; then
  report
  exit 0
fi

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "java image: $JAVA_IMAGE ($(docker image inspect --format '{{.Id}}' "$JAVA_IMAGE" 2>/dev/null || echo unknown))"
  echo "redis image: $REDIS_IMAGE ($(docker image inspect --format '{{.Id}}' "$REDIS_IMAGE" 2>/dev/null || echo unknown))"
  echo "docker: $(docker info --format '{{.ServerVersion}}, {{.OperatingSystem}}, kernel {{.KernelVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
  echo "servers: cpus $SERVER_CPUS, java $SERVER_JAVA_OPTS, KeyForge options ${SERVER_ARGS:-(none)}"
  echo "clients: cpus $CLIENT_CPUS, java $CLIENT_JAVA_OPTS"
  echo "redis-benchmark: -c $CLIENTS -d $VALUE_SIZE -r $KEYSPACE --threads $BENCH_THREADS, tests $TESTS, pipelines $PIPELINES, -n $REQUESTS (pipelined $PIPELINED_REQUESTS, cluster $CLUSTER_REQUESTS), warm-up $WARMUP_REQUESTS (cluster $CLUSTER_WARMUP_REQUESTS)"
  echo "load generator: $LOAD_CONNECTIONS connections, rates $RATES (cluster $CLUSTER_RATES), warmup ${WARMUP}s, duration ${DURATION}s"
  docker run --rm "$REDIS_IMAGE" redis-server --version
  docker run --rm "$JAVA_IMAGE" java -version 2>&1
} > "$OUT/environment.txt"

SERVERS=()   # containers of the current target, whose CPU time is counted
cleanup() {
  docker rm -f kf-target kf-node-1 kf-node-2 kf-node-3 kf-tools >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The tools container runs redis-benchmark and redis-cli; it stays up so that its start-up costs nothing per test.
start_tools() {
  docker run -d --name kf-tools --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" "$REDIS_IMAGE" sleep infinity \
    >/dev/null
}

# Cumulative CPU time of a container in nanoseconds: cgroup v1 cpuacct, else cgroup v2 cpu.stat.
container_cpu_ns() {
  docker exec "$1" sh -c 'if [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then cat /sys/fs/cgroup/cpuacct/cpuacct.usage;
    else awk "/^usage_usec/ { printf \"%.0f\n\", \$2 * 1000 }" /sys/fs/cgroup/cpu.stat; fi'
}
servers_cpu_ns() {
  local total=0 container
  for container in "${SERVERS[@]}"; do
    total=$((total + $(container_cpu_ns "$container")))
  done
  echo "$total"
}

java_server() {  # name, then server arguments
  local name=$1; shift
  local jfr=()
  if [[ $PROFILE == 1 ]]; then
    jfr=(-XX:StartFlightRecording:filename=/results/$TARGET-$PHASE-$name.jfr,settings=profile,dumponexit=true)
  fi
  docker run -d --name "$name" --hostname "$name" --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" \
    --sysctl net.core.somaxconn=65535 \
    -v "$(host_path "$SERVER_JAR"):/app/server.jar:ro" -v "$(host_path "$OUT"):/results" \
    "$JAVA_IMAGE" java $SERVER_JAVA_OPTS "${jfr[@]}" -jar /app/server.jar \
      --server.bind=0.0.0.0 --server.port=6379 "$@" $SERVER_ARGS >/dev/null
}

await_ping() {
  local host=$1
  for _ in $(seq 1 120); do
    [[ $(docker exec kf-tools redis-cli -h "$host" PING 2>/dev/null) == PONG ]] && return 0
    sleep 0.5
  done
  echo "$host did not answer PING" >&2
  return 1
}

# Prints the leader's node id once one node reports itself leader.
await_leader() {
  local node role
  for _ in $(seq 1 120); do
    for node in kf-node-1 kf-node-2 kf-node-3; do
      role=$(docker exec kf-tools redis-cli -h "$node" INFO raft 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')
      if [[ $role == leader ]]; then
        echo "${node#kf-}"
        return 0
      fi
    done
    sleep 0.5
  done
  echo "No leader elected" >&2
  return 1
}

# Starts the target and sets HOST (where clients connect) and SERVERS.
start_target() {
  case $1 in
    redis|redis-aof)
      local persistence=(--save "" --appendonly no)
      [[ $1 == redis-aof ]] && persistence=(--save "" --appendonly yes --appendfsync everysec)
      docker run -d --name kf-target --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" \
        --sysctl net.core.somaxconn=65535 \
        "$REDIS_IMAGE" redis-server --protected-mode no "${persistence[@]}" >/dev/null
      HOST=kf-target; SERVERS=(kf-target)
      ;;
    keyforge-memory)
      java_server kf-target --node.id=kf-target --server.type=netty --store.engine=memory
      HOST=kf-target; SERVERS=(kf-target)
      ;;
    keyforge-lsm)
      java_server kf-target --node.id=kf-target --server.type=netty --store.engine=lsm --store.data-dir=/data/store
      HOST=kf-target; SERVERS=(kf-target)
      ;;
    keyforge-lsm-vt)
      java_server kf-target --node.id=kf-target --server.type=virtual-threads --store.engine=lsm \
        --store.data-dir=/data/store
      HOST=kf-target; SERVERS=(kf-target)
      ;;
    cluster-leader|cluster-follower)
      local members=node-1=kf-node-1:7000,node-2=kf-node-2:7000,node-3=kf-node-3:7000 i
      for i in 1 2 3; do
        java_server "kf-node-$i" --node.id="node-$i" --server.type=virtual-threads --cluster.members="$members" \
          --store.data-dir=/data/store --raft.data-dir=/data/raft
      done
      SERVERS=(kf-node-1 kf-node-2 kf-node-3)
      local leader
      leader=$(await_leader)
      if [[ $1 == cluster-leader ]]; then
        HOST=kf-$leader
      else
        for i in 1 2 3; do [[ node-$i != "$leader" ]] && { HOST=kf-node-$i; break; }; done
      fi
      echo "leader $leader, clients on $HOST"
      ;;
    *) echo "Unknown target '$1'" >&2; exit 2 ;;
  esac
  await_ping "$HOST"
}

stop_target() {
  local container
  for container in "${SERVERS[@]}"; do
    docker logs "$container" > "$OUT/$TARGET-$container.server.log" 2>&1 || true
    # A clean stop lets a JFR recording be written (dumponexit).
    docker stop -t 30 "$container" >/dev/null 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  SERVERS=()
}

run_redis_benchmark() {
  local requests pipeline test before after csv warmup=$WARMUP_REQUESTS
  [[ $TARGET == cluster-* ]] && warmup=$CLUSTER_WARMUP_REQUESTS
  docker exec kf-tools redis-benchmark -h "$HOST" -p 6379 -c "$CLIENTS" -n "$warmup" -d "$VALUE_SIZE" \
    -r "$KEYSPACE" --threads "$BENCH_THREADS" -t "${TESTS// /,}" -q > /dev/null
  for pipeline in $PIPELINES; do
    requests=$REQUESTS
    [[ $pipeline != 1 ]] && requests=$PIPELINED_REQUESTS
    [[ $TARGET == cluster-* ]] && requests=$CLUSTER_REQUESTS
    for test in $TESTS; do
      before=$(servers_cpu_ns)
      csv=$(docker exec kf-tools redis-benchmark -h "$HOST" -p 6379 -c "$CLIENTS" -n "$requests" -d "$VALUE_SIZE" \
        -r "$KEYSPACE" -P "$pipeline" --threads "$BENCH_THREADS" -t "$test" --csv | tr -d '\r' | tail -n 1)
      after=$(servers_cpu_ns)
      # "SET","98039.22","0.287","0.088","0.271","0.415","0.551","2.215" -> rps, avg, min, p50, p95, p99, max (ms)
      local row
      row=$(printf '%s' "$csv" | tr -d '"')
      local cpu_us_per_request
      cpu_us_per_request=$(awk -v ns=$((after - before)) -v n="$requests" 'BEGIN { printf "%.2f", ns / 1000 / n }')
      echo "$TARGET,$pipeline,$CLIENTS,$requests,$row,$cpu_us_per_request" >> "$OUT/redis-benchmark.csv"
      echo "  redis-benchmark P=$pipeline $row, server CPU $cpu_us_per_request us/request"
    done
  done
}

run_load_generator() {
  local rates=$RATES rate
  [[ $TARGET == cluster-* ]] && rates=$CLUSTER_RATES
  for rate in $rates; do
    docker run --rm --name kf-load --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" \
      --pid "container:$HOST" \
      -v "$(host_path "$BENCH_JAR"):/app/bench.jar:ro" -v "$(host_path "$OUT"):/results" \
      "$JAVA_IMAGE" java $CLIENT_JAVA_OPTS -jar /app/bench.jar \
        --host="$HOST" --port=6379 --connections="$LOAD_CONNECTIONS" --rate="$rate" \
        --keyspace="$KEYSPACE" --value-size="$VALUE_SIZE" \
        --warmup-seconds="$WARMUP" --duration-seconds="$DURATION" --settle-seconds=1 \
        --label="$TARGET" --server-pid=1 --output=/results/latency.csv \
      2>&1 | tee "$OUT/$TARGET-${LOAD_CONNECTIONS}c-${rate}rps.log"
  done
}

if [[ ! -s $OUT/redis-benchmark.csv ]]; then
  echo "target,pipeline,clients,requests,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms,server_cpu_us_per_request" \
    > "$OUT/redis-benchmark.csv"
fi

cleanup
start_tools
for TARGET in $TARGETS; do
  echo "=== $TARGET"
  PHASE=redis-benchmark
  start_target "$TARGET"
  run_redis_benchmark
  stop_target

  # A fresh server for the open-loop runs, so that redis-benchmark's keys and counters do not carry over.
  PHASE=open-loop
  start_target "$TARGET"
  run_load_generator
  stop_target
done

report
