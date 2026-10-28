#!/usr/bin/env bash
# Phase 4 acceptance check: a three-node cluster in Docker Compose accepts writes and survives killing the leader.
#
#  1. Starts a fresh cluster (keyforge-chaos/docker-compose.yml) and waits for a leader.
#  2. Writes KEYS keys and KEYS increments of one counter through a follower, which forwards them to the leader.
#  3. Kills the leader with SIGKILL and waits for the two survivors to elect a new one.
#  4. Checks every acknowledged write on both survivors, then writes KEYS more keys through them.
#  5. Runs redis-benchmark against a follower of the new leader.
#  6. Restarts the killed node and waits until it has caught up with the leader's commit index.
#
# Build the jar first:   ./mvnw -q package -DskipTests
# Then:                  keyforge-chaos/scripts/leader-failover.sh       (KEEP=1 leaves the cluster running)
# With snapshots:        SNAPSHOT_THRESHOLD=5000 keyforge-chaos/scripts/leader-failover.sh
#                        (the restarted node then catches up through InstallSnapshot)
#
# Works on Linux, macOS and Git Bash on Windows (Docker Desktop).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
KEYS=${KEYS:-1000}
NODES=(kf1 kf2 kf3)
JAR=$ROOT/keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar
[[ -f $JAR ]] || { echo "Missing $JAR; run ./mvnw -q package -DskipTests first" >&2; exit 1; }

# Git Bash: keep MSYS from rewriting arguments, and give Docker a Windows path for the compose file.
export MSYS_NO_PATHCONV=1
host_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}
COMPOSE=(docker compose -f "$(host_path "$ROOT/keyforge-chaos/docker-compose.yml")" -p keyforge)

cli() { "${COMPOSE[@]}" exec -T client redis-cli -h "$@" | tr -d '\r'; }
raft_field() { cli "$1" INFO raft 2>/dev/null | sed -n "s/^$2://p"; }
now_ms() { date +%s%3N; }
fail() { echo "FAIL: $*" >&2; "${COMPOSE[@]}" logs --tail 40 >&2 || true; exit 1; }
expect() { # expect <description> <expected> <actual>
  [[ $2 == "$3" ]] || fail "$1: expected '$2', got '$3'"
  echo "  ok  $1 = $3"
}
first_other_than() { # first node of the rest that is not $1
  local skip=$1 node; shift
  for node in "$@"; do [[ $node != "$skip" ]] && { echo "$node"; return; }; done
}

# Prints the leader among the given nodes once one is elected; polls from inside the client container every 50 ms.
wait_for_leader() {
  "${COMPOSE[@]}" exec -T client sh -c '
    end=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$end" ]; do
      for node in "$@"; do
        if redis-cli -h "$node" INFO raft 2>/dev/null | grep -q "^role:leader"; then echo "$node"; exit 0; fi
      done
      sleep 0.05
    done
    exit 1' sh "$@" | tr -d '\r' || fail "no leader among $* after 30 s"
}

cleanup() { [[ ${KEEP:-0} == 1 ]] || "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "=== Starting a fresh three-node cluster"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d 2>&1 | grep -v "Creating\|Starting" || true
for node in "${NODES[@]}"; do
  until [[ $(cli "$node" PING 2>/dev/null) == PONG ]]; do sleep 0.5; done
done
LEADER=$(wait_for_leader "${NODES[@]}")
echo "  leader: $LEADER (term $(raft_field "$LEADER" term))"
FOLLOWER=$(first_other_than "$LEADER" "${NODES[@]}")

echo "=== Writing $KEYS keys and $KEYS increments through follower $FOLLOWER"
started=$(now_ms)
seq 1 "$KEYS" | awk '{ printf "SET key:%d value-%d\nINCR counter\n", $1, $1 }' | cli "$FOLLOWER" >/dev/null
echo "  $(( 2 * KEYS )) commands in $(( $(now_ms) - started )) ms (one redis-cli connection, one command at a time)"
expect "DBSIZE via $FOLLOWER" $(( KEYS + 1 )) "$(cli "$FOLLOWER" DBSIZE)"
expect "counter via $LEADER" "$KEYS" "$(cli "$LEADER" GET counter)"
cli "$FOLLOWER" SET ephemeral gone PX 300 >/dev/null

echo "=== Killing the leader $LEADER with SIGKILL at $(date -u +%H:%M:%S.%3N) UTC"
"${COMPOSE[@]}" kill -s SIGKILL "$LEADER" >/dev/null 2>&1
killed=$(now_ms)
SURVIVORS=()
for node in "${NODES[@]}"; do [[ $node != "$LEADER" ]] && SURVIVORS+=("$node"); done
NEW_LEADER=$(wait_for_leader "${SURVIVORS[@]}")
echo "  new leader: $NEW_LEADER (term $(raft_field "$NEW_LEADER" term)), observed $(( $(now_ms) - killed )) ms after the kill"
echo "  from $NEW_LEADER's log:"
"${COMPOSE[@]}" logs --no-log-prefix "$NEW_LEADER" 2>/dev/null \
  | grep -E "Starting an election|Became the leader" | tail -2 | sed 's/^/    /'

echo "=== Checking every acknowledged write on the survivors"
for node in "${SURVIVORS[@]}"; do
  expect "DBSIZE via $node" $(( KEYS + 1 )) "$(cli "$node" DBSIZE)"
  expect "counter via $node" "$KEYS" "$(cli "$node" GET counter)"
  expect "key:1 via $node" "value-1" "$(cli "$node" GET key:1)"
  expect "key:$KEYS via $node" "value-$KEYS" "$(cli "$node" GET "key:$KEYS")"
done
expect "ephemeral key expired" "" "$(cli "${SURVIVORS[0]}" GET ephemeral)"
missing=$(seq 1 "$KEYS" | awk '{ printf "EXISTS key:%d\n", $1 }' | cli "${SURVIVORS[1]}" | grep -c '^0$' || true)
expect "keys missing after the failover" 0 "$missing"

echo "=== Writing $KEYS more keys through the survivors"
seq 1 "$KEYS" | awk '{ printf "SET after:%d %d\n", $1, $1 }' | cli "${SURVIVORS[0]}" >/dev/null
expect "DBSIZE via ${SURVIVORS[1]}" $(( 2 * KEYS + 1 )) "$(cli "${SURVIVORS[1]}" DBSIZE)"

NEW_FOLLOWER=$(first_other_than "$NEW_LEADER" "${SURVIVORS[@]}")
echo "=== redis-benchmark against follower $NEW_FOLLOWER (forwarded to $NEW_LEADER)"
"${COMPOSE[@]}" exec -T client redis-benchmark -h "$NEW_FOLLOWER" -t set,get,incr -n 20000 -c 20 -q | tr '\r' '\n' \
  | grep -v '^$' | grep -v 'rps=' || true

echo "=== Restarting $LEADER"
"${COMPOSE[@]}" start "$LEADER" >/dev/null 2>&1
until [[ $(cli "$LEADER" PING 2>/dev/null) == PONG ]]; do sleep 0.5; done
deadline=$(( $(now_ms) + 60000 ))
target=$(raft_field "$NEW_LEADER" commit_index)
until (( $(raft_field "$LEADER" last_applied) >= target )); do
  (( $(now_ms) < deadline )) || fail "$LEADER did not catch up to index $target"
  sleep 0.5
done
expect "$LEADER role" follower "$(raft_field "$LEADER" role)"
expect "after:$KEYS via $LEADER" "$KEYS" "$(cli "$LEADER" GET "after:$KEYS")"
echo "  $LEADER applied index $(raft_field "$LEADER" last_applied), snapshot index $(raft_field "$LEADER" snapshot_index)"
"${COMPOSE[@]}" logs --no-log-prefix "$LEADER" "$NEW_LEADER" 2>/dev/null   | grep -E "Sent snapshot|Restored the state machine" | tail -2 | sed 's/^/    /' || true

echo "=== PASS: writes survived killing the leader; the cluster kept serving and the old leader caught up"
