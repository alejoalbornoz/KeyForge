# KeyForge

A distributed key-value store written from scratch in Java 25. It speaks the Redis RESP2 protocol, so stock
`redis-cli` and `redis-benchmark` work against it unchanged. Under the hood it will run its own LSM-tree storage engine
and its own Raft implementation, without any existing storage or consensus library.

> **Status: Phase 2 of 7 complete.** Single-node server: RESP2 protocol, an in-memory store, and two
> interchangeable network front ends, Netty and virtual threads, compared in
> [docs/benchmarks/netty-vs-virtual-threads.md](docs/benchmarks/netty-vs-virtual-threads.md).
> The LSM engine (Phase 3) and Raft (Phase 4) are not built yet.

## Build

You need JDK 25. The Maven wrapper downloads Maven 3.9.16 on first use.

```bash
./mvnw verify
```

`verify` runs unit tests, jqwik property tests and the integration tests (both real servers, driven by Jedis and by raw
sockets, plus short load-generator runs), then builds `keyforge-server/target/keyforge-server-0.1.0-SNAPSHOT-all.jar`
and the load generator `keyforge-bench/target/keyforge-bench-0.1.0-SNAPSHOT-all.jar`.

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
| `store.segments` | `KEYFORGE_STORE_SEGMENTS` | `256` | Lock segments (power of two) |
| `store.active-expire-interval-ms` | `KEYFORGE_STORE_ACTIVE_EXPIRE_INTERVAL_MS` | `100` | Active expiry period (0 = off) |

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
| `CONFIG GET pattern...` | Reports `save`, `appendonly`, `port`, `bind`, ... (globs `*` and `?`) |
| `COMMAND [DOCS\|INFO\|COUNT\|LIST]` | `COMMAND`, `DOCS` and `INFO` return an empty array |
| `HELLO [2]`, `CLIENT SETNAME\|GETNAME\|ID\|SETINFO` | `HELLO 3` fails with `NOPROTO`: only RESP2 is spoken |

Unknown commands, wrong arity and protocol errors use Redis 7's exact wording, for example
`-ERR unknown command 'FOO', with args beginning with: 'x' `.

## Modules

| Module | Contents |
| --- | --- |
| `keyforge-protocol` | RESP2 encoder and incremental decoders over `ByteBuffer`; no dependencies |
| `keyforge-storage` | `KeyValueStore` interface and the segmented in-memory engine with lazy and active expiry |
| `keyforge-server` | Command table and dispatcher, Netty and virtual-thread servers, configuration, `main` |
| `keyforge-bench` | Load generator: open-loop RESP traffic, HdrHistogram latencies, server memory/threads/CPU from `/proc` |

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
