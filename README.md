# KeyForge

A distributed key-value store written from scratch in Java 25. It speaks the Redis RESP2 protocol, so stock
`redis-cli` and `redis-benchmark` work against it unchanged. Under the hood it will run its own LSM-tree storage engine
and its own Raft implementation, without any existing storage or consensus library.

> **Status: Phase 4 of 7 complete.** RESP2 protocol, two interchangeable network front ends (Netty and virtual
> threads, compared in [docs/benchmarks/netty-vs-virtual-threads.md](docs/benchmarks/netty-vs-virtual-threads.md)), a
> durable LSM-tree storage engine (write-ahead log, memtable, SSTables, size-tiered compaction) whose data survives
> `kill -9`, and Raft consensus: a node runs standalone or as a member of a replicated cluster with linearizable reads
> and writes, which keeps serving when its leader is killed.

## Build

You need JDK 25. The Maven wrapper downloads Maven 3.9.16 on first use.

```bash
./mvnw verify
```

`verify` runs unit tests, jqwik property tests and the integration tests (both real servers on both storage engines,
driven by Jedis and by raw sockets; crash tests that kill writing processes; short load-generator and JMH runs; the
10,000-seed Raft simulation; Raft clusters over real gRPC), then
builds `keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar` and
`keyforge-bench/target/keyforge-bench-0.1.0-SNAPSHOT-all.jar` (load generator and JMH benchmarks).

## Run

```bash
java -jar keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar
```

Configuration comes from defaults, then an optional properties file (`--config <file>`), then environment variables,
then command-line overrides:

| Property | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `node.id` | `KEYFORGE_NODE_ID` | `node-1` | Name in every log line (MDC `nodeId`) |
| `server.bind` | `KEYFORGE_SERVER_BIND` | `0.0.0.0` | Listen address |
| `server.port` | `KEYFORGE_SERVER_PORT` | `6379` | Listen port |
| `server.type` | `KEYFORGE_SERVER_TYPE` | `netty` | Front end: `netty` or `virtual-threads` |
| `server.io-threads` | `KEYFORGE_SERVER_IO_THREADS` | `0` | Netty event loops (0 = 2 × CPUs); ignored by `virtual-threads` |
| `protocol.max-bulk-length` | `KEYFORGE_PROTOCOL_MAX_BULK_LENGTH` | `536870912` | Largest accepted bulk string |
| `protocol.max-array-length` | `KEYFORGE_PROTOCOL_MAX_ARRAY_LENGTH` | `1048576` | Most arguments per request |
| `store.engine` | `KEYFORGE_STORE_ENGINE` | `lsm` | Storage engine: `lsm` (durable) or `memory` |
| `store.data-dir` | `KEYFORGE_STORE_DATA_DIR` | `data` | LSM: directory for WAL, SSTables and manifest |
| `store.wal.fsync-policy` | `KEYFORGE_STORE_WAL_FSYNC_POLICY` | `group-commit` | LSM: `always` or `group-commit` (see below) |
| `store.wal.group-commit-interval-ms` | `KEYFORGE_STORE_WAL_GROUP_COMMIT_INTERVAL_MS` | `10` | LSM, group commit: longest time between fsyncs |
| `store.wal.group-commit-bytes` | `KEYFORGE_STORE_WAL_GROUP_COMMIT_BYTES` | `1048576` | LSM, group commit: unsynced bytes that force an fsync |
| `store.memtable-size-bytes` | `KEYFORGE_STORE_MEMTABLE_SIZE_BYTES` | `67108864` | LSM: memtable size that triggers a flush |
| `store.segments` | `KEYFORGE_STORE_SEGMENTS` | `256` | Memory engine: lock segments (power of two) |
| `store.active-expire-interval-ms` | `KEYFORGE_STORE_ACTIVE_EXPIRE_INTERVAL_MS` | `100` | Memory engine: active expiry period (0 = off) |
| `cluster.members` | `KEYFORGE_CLUSTER_MEMBERS` | (empty) | Raft cluster: `id=host:port,...` for every member, this node included (the Raft transport's address); empty = standalone |
| `raft.bind` | `KEYFORGE_RAFT_BIND` | `0.0.0.0` | Raft: address the gRPC transport listens on (the port comes from this node's entry) |
| `raft.data-dir` | `KEYFORGE_RAFT_DATA_DIR` | `raft` | Raft: directory for the log, term, vote and snapshots |
| `raft.election-timeout-ms` | `KEYFORGE_RAFT_ELECTION_TIMEOUT_MS` | `1000` | Raft: shortest election timeout (each is drawn from [t, 2t]) |
| `raft.heartbeat-interval-ms` | `KEYFORGE_RAFT_HEARTBEAT_INTERVAL_MS` | `100` | Raft: leader heartbeat period |
| `raft.snapshot-threshold` | `KEYFORGE_RAFT_SNAPSHOT_THRESHOLD` | `100000` | Raft: applied entries between snapshots (and log compactions) |
| `raft.request-timeout-ms` | `KEYFORGE_RAFT_REQUEST_TIMEOUT_MS` | `5000` | Raft: how long a command waits for its commit, read confirmation or the leader |

Example: `java -jar keyforge-server-...-all.jar --server.port=6380 --server.bind=127.0.0.1`

### Choosing a front end

Both front ends share the RESP codec and the command dispatcher and pass the same integration suite.

- `netty` (default): Netty event loops over NIO; commands run on the connection's event loop
  ([ADR 0002](docs/adr/0002-threading-model-and-in-memory-store.md), [ADR 0004](docs/adr/0004-flushing-and-backpressure.md)).
- `virtual-threads`: a `ServerSocket` accept loop and one virtual thread per connection doing blocking reads and
  writes ([ADR 0005](docs/adr/0005-virtual-thread-server-and-pinning.md)).

```bash
java -jar keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar --server.type=virtual-threads
```

### Choosing a storage engine

- `lsm` (default): a log-structured merge tree in `store.data-dir`
  ([ADR 0006](docs/adr/0006-lsm-storage-engine.md), [ADR 0007](docs/adr/0007-wal-fsync-policies-and-recovery.md),
  [ADR 0008](docs/adr/0008-size-tiered-compaction.md)). Every write is in the write-ahead log before it is acknowledged,
  so acknowledged writes survive a killed process. The fsync policy decides what survives a crash of the machine:
  - `always`: a write is acknowledged after an fsync that covers it (concurrent writes share one fsync). Survives power
    loss.
  - `group-commit`: a write is acknowledged once written to the OS; the WAL is fsynced every
    `store.wal.group-commit-interval-ms` or every `store.wal.group-commit-bytes`, whichever comes first. A power loss can
    lose that last window, like Redis' `appendfsync everysec`.
- `memory`: the Phase 1 segmented hash table. Nothing is persisted.

On startup the LSM engine replays whatever the write-ahead log holds. A record torn by a crash at the very end of the
log is truncated; damage anywhere else stops the server with a `CorruptionException` rather than silently dropping
data. Only one process can open a data directory at a time.

### Running a cluster

Give every node the same `cluster.members`, its own `node.id` and its own directories. Each node then runs Raft
([ADR 0009](docs/adr/0009-raft-as-a-pure-state-machine.md)) over gRPC
([ADR 0014](docs/adr/0014-grpc-transport-and-client-forwarding.md)):

```bash
java -jar keyforge-server-0.1.0-SNAPSHOT-all.jar --node.id=node-1 --server.type=virtual-threads \
  --cluster.members=node-1=host1:7000,node-2=host2:7000,node-3=host3:7000 \
  --store.data-dir=/data/store --raft.data-dir=/data/raft
```

- Clients may talk to any node. Followers forward data commands to the leader and relay its reply, so `redis-cli` and
  `redis-benchmark` work unchanged against any node. `INFO raft` shows a node's role, term, leader and log positions.
- Writes return once committed on a majority and applied; reads are linearizable through ReadIndex
  ([ADR 0010](docs/adr/0010-linearizable-reads-with-readindex.md)).
- The leader turns `EX`/`PX` into absolute instants and stamps every entry with its clock; replicas decide expiry at
  that log time, never with their own clocks ([ADR 0011](docs/adr/0011-deterministic-expiry-under-raft.md)).
- The Raft log is the only write-ahead log: in a cluster the LSM engine writes no WAL and records the last applied log
  index in its manifest. After a crash it is rebuilt from that point, from the latest snapshot plus the log
  ([ADR 0012](docs/adr/0012-no-double-wal.md)). A cluster requires `store.engine=lsm`.
- Prefer `server.type=virtual-threads`: commands wait for their commit, which blocks a Netty event loop and the other
  connections on it.
- Membership is fixed: changing `cluster.members` of a running cluster is not supported.

`keyforge-chaos/docker-compose.yml` starts a three-node cluster in containers, and
`keyforge-chaos/scripts/leader-failover.sh` kills its leader under load and checks what survived:

```bash
./mvnw -q package -DskipTests
keyforge-chaos/scripts/leader-failover.sh
```

### Try it with the real Redis tools

If you have no local Redis install, use the official image. On Docker Desktop, `host.docker.internal` reaches the host:

```bash
docker run --rm -it redis:7.4 redis-cli -h host.docker.internal
```

```bash
docker run --rm redis:7.4 redis-benchmark -h host.docker.internal -t set,get -n 1000000 -P 16 -c 50
```

## Commands

| Command | Notes |
| --- | --- |
| `PING [msg]`, `ECHO msg`, `QUIT` | |
| `GET`, `SET key value [NX\|XX] [EX s\|PX ms]` | `KEEPTTL`, `GET`, `EXAT`, `PXAT` are not supported yet (syntax error) |
| `INCR`, `DECR` | Redis' integer rules and error messages, overflow detected |
| `MGET`, `MSET` | Atomic with respect to every other command |
| `DEL`, `EXISTS`, `DBSIZE`, `FLUSHDB [ASYNC\|SYNC]` | |
| `CONFIG GET pattern...` | Reports `save`, `appendonly` (`yes` on the LSM engine), `dir`, `port`, `bind`, ... (globs `*` and `?`) |
| `INFO [section ...]` | Sections `server` and, in a cluster, `raft` (role, term, leader, commit/applied/snapshot index, members) |
| `COMMAND [DOCS\|INFO\|COUNT\|LIST]` | `COMMAND`, `DOCS` and `INFO` return an empty array |
| `HELLO [2]`, `CLIENT SETNAME\|GETNAME\|ID\|SETINFO` | `HELLO 3` fails with `NOPROTO`: only RESP2 is spoken |

Unknown commands, wrong arity and protocol errors use Redis 7's exact wording, for example
`-ERR unknown command 'FOO', with args beginning with: 'x' `.

## Modules

| Module | Contents |
| --- | --- |
| `keyforge-protocol` | RESP2 encoder and incremental decoders over `ByteBuffer`; no dependencies |
| `keyforge-storage` | `KeyValueStore` interface; the LSM engine (`io.keyforge.storage.lsm`: WAL, memtable, SSTables, manifest, compaction) and the segmented in-memory engine |
| `keyforge-raft` | Raft: the pure core (`io.keyforge.raft`), the runtime that drives it, durable log and snapshot storage, the gRPC transport; the deterministic simulator in its tests |
| `keyforge-server` | Command table and dispatcher, Netty and virtual-thread servers, configuration, `main`; in a cluster, the replicated state machine, ReadIndex reads and forwarding to the leader (`io.keyforge.server.cluster`) |
| `keyforge-chaos` | Docker Compose cluster and failover script (Phase 5 adds fault injection and a linearizability checker) |
| `keyforge-bench` | Load generator (open-loop RESP traffic, HdrHistogram latencies, server memory/threads/CPU from `/proc`) and JMH benchmarks of the LSM engine |

Design decisions are recorded in [docs/adr](docs/adr/README.md).

## Benchmarks

`keyforge-bench/scripts/run-benchmarks.sh` compares the two front ends at 1k, 10k and 50k connections. Every run uses a
fresh server container and a load-generator container on one Docker network, pinned to separate CPUs. It reports
throughput, p50/p99/p99.9 latency corrected for coordinated omission, RSS, live heap, OS threads and CPU. It needs
Docker and the jars from `./mvnw package`, and works from Linux, macOS or Git Bash on Windows:

```bash
./mvnw -q package -DskipTests
keyforge-bench/scripts/run-benchmarks.sh
```

The load generator also runs standalone against any RESP server (`java -jar keyforge-bench-...-all.jar --help`).
Results and analysis: [docs/benchmarks/netty-vs-virtual-threads.md](docs/benchmarks/netty-vs-virtual-threads.md).

### Storage engine microbenchmarks (JMH)

`LsmPutBenchmark`, `LsmGetBenchmark` and `LsmScanBenchmark` measure the LSM engine directly: put throughput per fsync
policy with 1 and 16 threads, point lookups (hits and misses, from the memtable and from an SSTable) and 100-key range
scans (one compacted table or several layered sources), over a million keys:

```bash
./mvnw -q package -DskipTests
java -cp keyforge-bench/target/keyforge-bench-0.1.0-SNAPSHOT-all.jar org.openjdk.jmh.Main 'io.keyforge.bench.jmh.Lsm'
```

Results and analysis: [docs/benchmarks/lsm-engine.md](docs/benchmarks/lsm-engine.md).

## Phase 1 verification

What was checked on 2026-10-27 (Windows 10, JDK 25.0.5; Redis tools from the `redis:7.4` and `redis:8` images):

- `./mvnw clean verify`: 138 unit and property tests and 14 integration tests pass.
- `redis-cli` 7.4: interactive session (prompt, `PING`, `SET`, `GET`, unknown command, `HELLO 3`) and one-shot
  commands behave like Redis. `redis-cli` 8.10 one-shot commands work too.
- `redis-benchmark -t set,get -n 1000000 -P 16 -c 50` (7.4) completes every request with no errors, and `-t
  set,get,incr,mset` (8.10) also completes. The throughput it printed (~105k SET/s, ~117k GET/s) mostly measures
  Docker Desktop's `host.docker.internal` proxy, not KeyForge. Meaningful numbers come with the Phase 2 benchmark
  harness and the Phase 6 comparison against real Redis on the same machine.

## Phase 2 verification

What was checked on 2026-10-27 (Windows 10; jars built with JDK 25.0.5; benchmarks in Linux containers on Temurin
25.0.4.1):

- `./mvnw clean verify`: 173 unit and property tests and 37 integration tests pass. `KeyForgeServerIT` runs its 14
  end-to-end tests (Jedis and raw RESP) against both servers. `VirtualThreadServerIT` checks that 1,000 connections add
  no platform threads, that shutdown closes open connections, and that serving requests never pins a virtual thread
  (JFR `jdk.VirtualThreadPinned`, with a positive control). `LoadGeneratorIT` drives both servers with the load
  generator.
- `keyforge-bench/scripts/run-benchmarks.sh`: two full passes of {1k, 10k, 50k} connections × {closed loop, 20k, 40k,
  60k req/s} × both servers, plus sensitivity runs. Every run completed with zero errors and zero dropped connections;
  both servers held 50,000 concurrent connections. Results and analysis are in
  [docs/benchmarks/netty-vs-virtual-threads.md](docs/benchmarks/netty-vs-virtual-threads.md).

## Phase 3 verification

What was checked on 2026-10-28 (Windows 10, JDK 25.0.5; the SIGKILL check in Linux containers on Temurin 25 with the
`redis:7.4` tools):

- `./mvnw clean verify`: 289 unit and property tests and 70 integration tests pass. For the LSM engine that includes:
  - `LsmKeyValueStoreModelTest` (jqwik): 150 random sequences of up to 50 operations, checked against a `TreeMap`
    model after every step (every key, the key count and a full ordered scan). The operations are puts with
    `NX`/`XX`/TTL, multi-key puts, deletes, read-modify-writes, clock moves, flushes, major compactions, clears,
    range scans and restarts. Memtables of 600 bytes and eager compaction keep the background threads busy throughout.
    As a check that the test bites, making flushes drop tombstones unsafely made it fail at once with a deleted key
    coming back.
  - `WalTornTailPropertyTest`: a WAL cut at any byte recovers exactly the complete records before the cut. `WalTest`
    covers torn tails, zero-filled tails, mid-log corruption (fatal) and torn tails in older WALs (fatal).
  - `LsmKeyValueStoreConcurrencyTest`: no lost `compute` updates, no torn `getAll` or `scan` while memtables rotate and
    tables merge, and read-your-writes.
  - `CrashRecoveryIT`: three rounds per fsync policy. A child JVM writes with four threads and is killed without
    warning (`Process.destroyForcibly`) after a random delay. Every thread's keys must equal the state after its last
    acknowledged operation, or after the one in flight, and earlier rounds' keys must be untouched.
  - `KillRecoveryIT`: the real server (`KeyForgeMain`) in a child JVM, Netty with `group-commit` and virtual threads
    with `always`, killed while four Jedis clients write. After a restart every acknowledged `SET` is present and the
    `INCR` counter is within one in-flight increment per client.
  - `KeyForgeServerIT` runs its 14 end-to-end tests on all four combinations of front end and storage engine.
    `VirtualThreadServerIT`'s pinning check now runs on the LSM engine. `JmhBenchmarksIT` runs every JMH benchmark
    once.
- `kill -9` on Linux. The server jar ran in an `eclipse-temurin:25-jdk` container with 4 MiB memtables and its data in
  a Docker volume:
  - 100,000 `SET`s through `redis-cli --pipe`, plus `INCR`, `MSET`, `DEL` and overwrites;
  - `docker kill --signal=KILL` (exit code 137), then a restart: `DBSIZE` was still 100,002, and every probed value,
    overwrite and delete was intact;
  - a second SIGKILL in the middle of `redis-benchmark -t set,incr -P 16 -c 20`: the server came back with 299,452
    keys, no corruption reported and the earlier data untouched;
  - a clean `docker stop` and start: same key count.
- JMH (`docs/benchmarks/lsm-engine.md`):
  - puts from one thread: about 30,000/s with `group-commit` and about 1,300/s with `always` (one fsync each);
  - puts from 16 threads: 2.5 to 3.7 times as many with `group-commit`, and about 5.5 times as many with `always`
    until compactions slow the disk;
  - point lookups: about 1.8 to 2 µs for a hit, 158 ns for an SSTable miss (Bloom filter);
  - 100-key scans: 12 to 29 µs.

## Phase 4 verification

What was checked on 2026-10-28 (Windows 10, JDK 25.0.5; the cluster in Linux containers on Temurin 25 with the
`redis:7.4` tools):

- `./mvnw clean verify`: 354 unit and property tests and 78 integration tests pass. For Raft that includes:
  - `RaftSimulationIT`: 10,000 seeds of the deterministic simulator (ADR 0013), 3–5 nodes each, with message loss,
    duplication, delays and reordering, symmetric and one-way partitions, crashes (also between a persist and a send)
    and restarts. About 250 million simulated events, 23,736 elections, 734,661 acknowledged writes, 491,813
    linearizable reads, 15,309 snapshot installations and 164,102 crashes, with **no invariant violation** (election
    safety, log matching, state machine safety, leader completeness, read linearizability, and liveness after the faults
    stop), in 19 s. `RaftSimulationTest` checks that a seed replays exactly and that three deliberate bugs (sending
    before persisting, forgetting votes, reading without ReadIndex's confirmation) each break an invariant.
  - `RaftNodeTest` (26 scenarios of the pure core: PreVote, vote persistence ordering, leader stickiness, conflict
    truncation, the figure-8 commit rule, pipelining, ReadIndex rounds, snapshots), `FileRaftStorageTest` (torn tails,
    mid-log corruption, interrupted snapshot installs) and `RaftClusterIT` (three runtimes over real gRPC: replication,
    ReadIndex, forwarding, a leader crash, compaction and InstallSnapshot to a restarted node).
  - `LsmReplicatedModeTest` (no WAL; after a crash the store holds exactly the entries up to its durable log position,
    counters included; expiry at log time whatever the local clock; snapshot export and restore),
    `KeyValueStateMachineTest` (replicas with clocks hours apart reach identical results and contents) and `ClusterIT`
    (three nodes with both front ends driven by Jedis: every command through followers, `PX` expiry, a killed leader,
    catch-up, a full restart from the Raft log with no `.wal` file anywhere).
- `keyforge-chaos/scripts/leader-failover.sh` against the Docker Compose cluster:
  - 1,000 `SET`s and 1,000 `INCR`s through a follower, forwarded to the leader;
  - `docker kill --signal=KILL` of the leader: the survivors elected a new leader 2.1 s after the kill (election
    timeout 1–2 s);
  - on both survivors all 1,000 keys and the counter (1,000) were intact, and a 300 ms key had expired;
  - 1,000 more writes through the survivors and `redis-benchmark -t set,get,incr -n 20000 -c 20` against a follower
    completed. In Docker Desktop's VM each write costs a Raft-log fsync on two nodes: about 1,200 `SET`/s
    (p50 14 ms), and about 6,000 `GET`/s (p50 2.8 ms) with ReadIndex;
  - the killed node restarted, rejoined as a follower and caught up to the leader's index 43,003. With
    `SNAPSHOT_THRESHOLD=5000`, it caught up through InstallSnapshot (snapshot index 40,010).
