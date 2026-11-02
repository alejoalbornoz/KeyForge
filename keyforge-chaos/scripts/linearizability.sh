#!/usr/bin/env bash
# Phase 5: a Jepsen-style linearizability test of the five-node KeyForge cluster under faults, in one command.
#
#  1. Starts a fresh cluster (keyforge-chaos/docker-compose.yml).
#  2. Runs concurrent clients (GET, SET and INCR on a few keys) for DURATION seconds while the nemesis injects NEMESIS
#     faults, then heals them, waits for the cluster to recover and reads every key once more. The runner
#     (keyforge-chaos, Java, in the "runner" container on the cluster's network) records every invocation and
#     completion in history.json.
#  3. Checks the history with Porcupine (keyforge-chaos/checker, Go, run in Docker) and prints the report.
#
# Results go to keyforge-chaos/results/<time>-<nemesis>/: history.json, run.txt (the runner's summary), runner.log,
# report.txt, report.json and nodes.log (every node's log) always; when the history is not linearizable, or the checker
# could not decide, linearizability.html (Porcupine's visualization of the keys concerned) as well. The script also
# warns when the cluster did not serve again after the faults were healed: a liveness problem, not a safety one.
#
#   keyforge-chaos/scripts/linearizability.sh                          five minutes of random partitions
#   DURATION=120 NEMESIS=all keyforge-chaos/scripts/linearizability.sh   partitions, kills, pauses, latency, loss
#   NEMESIS=kill,pause keyforge-chaos/scripts/linearizability.sh --clients=30 --rate=300
#   NEMESIS=wipe DURATION=60 keyforge-chaos/scripts/linearizability.sh  checker self-test: must NOT be linearizable
#   CHECK=keyforge-chaos/results/<run> keyforge-chaos/scripts/linearizability.sh   re-check a saved history
#
# Faults: partition, kill, pause, latency, loss; all = every one of them; wipe = the checker's self-test (ADR 0016).
# Arguments are passed to the runner (java -jar keyforge-chaos/target/keyforge-chaos-*-all.jar --help lists them).
# Environment: DURATION (seconds, 300), NEMESIS (partition), SEED (random), SNAPSHOT_THRESHOLD (2000, so that snapshots
# and InstallSnapshot happen under faults), CHECK_TIMEOUT (checker time limit per key, 60s), KEEP=1 (leave the cluster
# running afterwards).
#
# Exit status: 0 linearizable, 1 not linearizable, 2 unknown (the checker timed out), 3 the test could not run.
# Works on Linux, macOS and Git Bash on Windows (Docker Desktop). Needs Docker, and JDK 25 if the jars are not built.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
DURATION=${DURATION:-300}
NEMESIS=${NEMESIS:-partition}
export SNAPSHOT_THRESHOLD=${SNAPSHOT_THRESHOLD:-2000}
SERVER_JAR=$ROOT/keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar
CHAOS_JAR=$ROOT/keyforge-chaos/target/keyforge-chaos-0.1.0-SNAPSHOT-all.jar

# Git Bash: keep MSYS from rewriting arguments, and give Docker Windows paths.
export MSYS_NO_PATHCONV=1
host_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}
COMPOSE=(docker compose -f "$(host_path "$ROOT/keyforge-chaos/docker-compose.yml")" -p keyforge)

check() { # check <results directory>: runs the checker on its history.json; prints the report, returns its status
  docker run --rm -v "$(host_path "$1"):/run" keyforge-checker -history /run/history.json \
    -timeout "${CHECK_TIMEOUT:-60s}"
}

echo "=== Building the checker (runs its tests)"
docker build -q -t keyforge-checker "$(host_path "$ROOT/keyforge-chaos/checker")" >/dev/null

if [[ -n ${CHECK:-} ]]; then
  echo "=== Checking $CHECK/history.json"
  status=0
  check "$(cd "$CHECK" && pwd)" || status=$?
  exit "$status"
fi

if [[ ! -f $SERVER_JAR || ! -f $CHAOS_JAR ]]; then
  echo "=== Building the server and the runner"
  (cd "$ROOT" && ./mvnw -q package -DskipTests -pl keyforge-server,keyforge-chaos -am)
fi

OUT=$ROOT/keyforge-chaos/results/$(date +%Y%m%d-%H%M%S)-${NEMESIS//,/-}
mkdir -p "$OUT"
cleanup() { [[ ${KEEP:-0} == 1 ]] || "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
save_logs() { "${COMPOSE[@]}" logs --no-color --timestamps > "$OUT/nodes.log" 2>&1 || true; }

echo "=== Starting a fresh five-node cluster (snapshot threshold $SNAPSHOT_THRESHOLD)"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
build_log=$("${COMPOSE[@]}" --profile test build --quiet 2>&1) || { echo "$build_log" >&2; exit 3; }
"${COMPOSE[@]}" up -d --no-build 2>&1 | grep -v "Creat\|Start" || true

echo "=== Running the workload for $DURATION s with nemesis '$NEMESIS'; results in $OUT"
status=0
"${COMPOSE[@]}" run --rm -T -v "$(host_path "$OUT"):/results" runner \
  --duration-seconds="$DURATION" --nemesis="$NEMESIS" --project=keyforge --output=/results \
  ${SEED:+--seed="$SEED"} "$@" 2>&1 | tee "$OUT/runner.log" || status=$?
save_logs
if (( status != 0 )) || [[ ! -f $OUT/history.json ]]; then
  echo "=== The test could not run (runner exit $status); see $OUT" >&2
  exit 3
fi

echo "=== Checking the history"
status=0
check "$OUT" || status=$?
if grep -q "did NOT recover" "$OUT/run.txt"; then
  echo "=== WARNING: the cluster did not serve again after the faults were healed (liveness); see $OUT/nodes.log"
fi
echo "=== Results: $OUT"
exit "$status"
