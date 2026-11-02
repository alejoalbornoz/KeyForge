# ADR 0015: Jepsen-style histories from a Java workload, checked per key by Porcupine

- Status: accepted
- Date: 2026-10-29
- Phase: 5

## Context

Phase 4 argued that reads and writes are linearizable (ADR 0009, ADR 0010) and checked it in a simulator (ADR 0013).
The simulator runs the Raft core only, not the integrated system: the gRPC transport, forwarding, the RESP servers and
the LSM store under real faults. The Phase 5 specification asks for a Jepsen-style test of the running cluster:

- a workload generator: concurrent clients run `GET`, `SET` and `INCR` on a small key set and record a history of
  operations (invoke, ok, fail, info/timeout) with timestamps, written in Python (asyncio and redis-py) or Java;
- a checker: the history exported to JSON and verified with Porcupine, as a small Go program in
  `keyforge-chaos/checker`. Timeouts must be recorded as indeterminate (`info`), never as failures;
- one command that runs a test (for example five minutes of random partitions), reports linearizable or not, and keeps
  the history and a visualization of failed runs.

Phase 4 left a suspected anomaly for this checker to find. A follower that forwards a write and gets `UNAVAILABLE` from
a leader that has just died retries with the next leader, which can apply an `INCR` twice (ADR 0014).

## Decision

**The workload is Java**, in a new module `keyforge-chaos`, package `io.keyforge.chaos`.

- **Clients are Jepsen processes.** Each is a virtual thread with one blocking RESP connection (`RespConnection`, on
  the Phase 1 codec) to one node: client `i` talks to node `i mod 5`. A client has at most one operation in flight and
  paces itself to share `--rate` operations per second (default 150, 15 clients). Half the operations are `GET`, a
  quarter `SET` and a quarter `INCR`.
- **A small key set, rotated** (`KeySchedule`, Jepsen's "independent keys"). Five keys are in use at a time, so
  clients contend on them. Each key is retired after 300 operations and replaced with a fresh one. Checking is NP-hard
  in the length of a key's history, and an indeterminate write stays open until the end of it. Rotation keeps each
  key's history short and bounds that cost, while every key still lives through several faults.
- **`SET` writes multiples of 1,000 from a global counter.** Every value is an integer, so `INCR` always applies, and
  values reached by increments rarely repeat a written value, which keeps the checker's search small.
- **Final reads.** Once the faults are healed and every node serves again, every key the run used is read once more. A
  lost write then shows up even on a key nobody read after it.

**Outcomes** (`Completion`). A wrong `fail` makes a correct system look broken, so an operation counts as failed only
when it certainly had no effect:

| Outcome | When |
| --- | --- |
| `ok` | A proper reply: the value read, `OK`, the new counter value |
| `fail` | The connection could not be opened, so nothing was sent; or the server answered `CLUSTERDOWN`, which KeyForge only sends when it found no leader to run the command (ADR 0014) |
| `info` | Everything else: a reply that did not come within `--op-timeout-ms` (10 s, longer than the server's own 5 s request timeout, so the server's answer usually arrives first), a broken connection, and every other error, including KeyForge's "may or may not have been applied" errors |

After an indeterminate completion the client continues as a new process: its old operation may still take effect at
any later time. If the connection may be out of step with its replies (a timeout or an I/O error), the client also
reconnects.

**The runner runs on the cluster's network**, in a `runner` container (`docker compose run runner`), not on the host.
The first runs went from the Windows host through the published ports, and Docker Desktop's port forwarder broke the
`fail` rule. It keeps accepting connections to a killed container's port and then closes them, so an operation that
never reached any server looked indeterminate. In a two-minute run with a majority kill, one key collected 104
indeterminate writes, and the checker gave up on it. On the cluster network a killed server's port answers with a TCP
reset, the client knows nothing was sent, and the same kills produce failures.

**The history** (`History`, format `keyforge-history/1`) is JSON: the start time, the run's options, and one event per
line.

```json
{"index":7,"time":1504223100,"type":"invoke","process":4,"f":"set","key":"lin:3f2a1c:12","value":42000,"node":"kf5"}
{"index":9,"time":1517887000,"type":"ok","process":4,"f":"set","key":"lin:3f2a1c:12","value":42000,"node":"kf5"}
{"index":10,"time":1520011400,"type":"invoke","process":"nemesis","f":"start","fault":"partition","value":"halves {kf2,kf4} | {kf1,kf3,kf5}"}
```

`time` is nanoseconds from one monotonic clock, read under the same lock that appends the event, so indexes and times
grow together. Nemesis actions are an `invoke` when they begin and an `info` when they are done (ADR 0016).

**The checker is Go and Porcupine** (v1.3.1), in `keyforge-chaos/checker`, run as a 5 MB static binary in a `scratch`
image. Go is not installed on the development machine; Docker is. Building the image runs `go vet` and the checker's
tests, so a broken checker never checks a history.

- **The model is one key** under `GET`, `SET` and `INCR`. The history is partitioned by key, and each key is checked on
  its own, on all cores, with a time limit (`-timeout`, 60 s per key).
- **Failed operations are left out**, since they had no effect. **Indeterminate reads are left out**, since they
  constrain nothing. **An indeterminate write returns after the end of the history and accepts any result.** Porcupine
  may then place it anywhere after its invocation, including after everything else, which is the same as it never
  taking effect. That is exactly what an indeterminate write means.
- **Indeterminate `SET`s that nobody observed are left out.** Every open operation multiplies the orders Porcupine
  must try. A `SET v` can only be seen as a value between `v` and `v + n`, where `n` counts the `INCR`s that may have
  taken effect on the key. If no completed `GET` or `INCR` returned a value in that range, then in every linearization
  the `SET` is followed only by indeterminate `INCR`s until the next `SET`. Removing it changes nothing anyone
  observed, so the history is linearizable exactly when it is linearizable without it.
- **Indeterminate `INCR`s take effect in invocation order.** They cannot be left out, because an increment's effect
  shows in every later value; that is how an `INCR` applied twice is caught. But they are interchangeable: each adds one,
  accepts any result and never returns, so no operation has to follow one. In any linearization, two of them can swap
  places, the earlier-invoked one taking the later one's place. If any linearization exists, one exists that applies
  them in invocation order. The model numbers them per key and only accepts them in that order. A key with `k` open
  increments then has `k + 1` possible prefixes for Porcupine to explore, not `2^k` subsets.
- **Both reductions are tested against the plain model.** 3,000 random histories of one key, with overlapping
  operations and random outcomes, are checked with and without them, and the verdicts must be identical. The trials
  must include violations, dropped `SET`s and keys with several open increments. The first two-minute run with every
  fault kind had two keys with about 50 open increments each, and the checker gave up on them after a minute. With the
  reductions, the same history checks as linearizable in under a second.
- **The verdict** is `LINEARIZABLE`, `NOT LINEARIZABLE` (some key has no linearization) or `UNKNOWN` (the checker ran
  out of time on some key and found no violation). For each key without a linearization the report gives the longest
  linearizable prefix Porcupine found, the value it leaves, and the operations that could have come next, none of
  which fits.
- **Output.** `report.txt` and `report.json` are written always. When the verdict is not `LINEARIZABLE`,
  `linearizability.html` is written as well: Porcupine's interactive visualization of the keys concerned, with the
  nemesis's faults as annotations on the timeline. Exit status: 0, 1, 2, or 3 for a history that cannot be read.

**One command**, `keyforge-chaos/scripts/linearizability.sh`:

1. builds the checker image (and so runs its tests);
2. starts a fresh five-node cluster with a snapshot threshold of 2,000, so that snapshots, compaction and
   InstallSnapshot happen under faults;
3. runs the workload and the nemesis (`DURATION`, default 300 s; `NEMESIS`, default `partition`);
4. checks the history.

Results go to `keyforge-chaos/results/<time>-<nemesis>/`: the history, the runner's summary and log, and the report.
Runs that are not linearizable also get the visualization and every node's log. `CHECK=<dir>` re-checks a saved
history.

**The checker must bite.** Its tests feed it histories with a stale read, a lost write, an `INCR` applied twice, and a
failed write that is later read. Each must be reported as not linearizable. Indeterminate operations must be allowed
either way. The `wipe` nemesis (ADR 0016) is the end-to-end counterpart: it destroys acknowledged writes on the real
cluster, and the checker must say so.

## Consequences

- Running a test needs only Docker: the runner, the cluster and the checker all run in containers. Building the jars
  needs JDK 25. Neither Go nor Python is installed on the host.
- `mvn verify` covers the Java side. Unit tests cover the classification rules, the history format, the RESP client
  against fake servers, the key rotation, the partition shapes (jqwik properties), the fault planner, the Docker
  commands (against a recording shell) and the nemesis schedule. `ChaosRunIT` runs the real workload and nemesis
  against three in-process nodes that are killed and restarted, and checks that every invocation completes exactly
  once. The Go tests run when the checker image is built. The Docker runs themselves stay outside `mvn verify`, like
  Phase 4's failover script.
- Linearizability is checked per key. Multi-key operations (`MSET`, `MGET`) are not in the workload, and anomalies
  that span keys (such as a transaction seeing part of another) are outside what this checker can see.
- A run gets fewer completed operations than its target rate while faults last. Clients of a cut-off node wait up to
  the server's 5 s request timeout for each operation. That is the point: those operations are the ones that test the
  edge cases.
- Process numbers grow with every indeterminate operation, as in Jepsen. The visualization maps them back to a few rows
  per key.

## Alternatives considered

- **Python with asyncio and redis-py**, as the specification allows. It is quick to write, but it adds a second
  toolchain that is not installed here, and its code would be tested outside `mvn verify`. Java reuses the project's
  RESP codec, build and test conventions; virtual threads make one blocking client per process natural.
- **A checker written in Java** (Wing and Gong's algorithm with Lowe's memoization, which is what Porcupine
  implements). It would run inside `mvn verify`, but the specification names Porcupine, and a well-used external
  checker is worth more as evidence than a home-made one.
- **Knossos or Elle** (Jepsen's own checkers, Clojure). They are the reference, but they need a JVM-based Clojure
  toolchain and the Jepsen history format (EDN), and Elle's strength, transactional anomalies, does not apply to
  single-key operations.
- **One long history per key** (no rotation). It finds anomalies that span many faults on one key, but a few
  indeterminate operations make the search explode, and the checker's verdict becomes `UNKNOWN`.
- **Treating `CLUSTERDOWN` as indeterminate.** It is always safe for the checker, but it would hide the server breaking
  its own contract: an operation that was applied after the server said it was not is a real bug, and the history
  should show it.
- **Treating timeouts on reads as failures.** Reads have no effect, so it would be sound, but the specification asks
  for every timeout to be `info`; the checker leaves indeterminate reads out either way.
