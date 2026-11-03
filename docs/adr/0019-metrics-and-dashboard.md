# ADR 0019: Micrometer meters in the node, served at /metrics by the JDK's HTTP server; Prometheus and Grafana in the quick start

- Status: accepted
- Date: 2026-11-03
- Phase: 6

## Context

Phase 6 asks for a Grafana dashboard over Prometheus metrics: leader, term, commit index, apply lag, compaction stats and
p99 latency. Until now a node exposed none of these except through `INFO raft`, which Prometheus cannot scrape.

- The project's module layout places `/actuator/prometheus` in a `keyforge-admin` Spring Boot module, which does not
  exist yet. The user chose not to build it in this phase: metrics are served by `keyforge-server` itself.
- No Spring in the data path (network, command execution, Raft, storage). Micrometer is an allowed library; its
  Prometheus registry brings the Prometheus Java client (`io.prometheus:prometheus-metrics-*`) with it, which the user
  accepted.
- Command latency is the one metric recorded on the data path: every command would pay for it. Everything else (Raft
  indices, LSM counters) already exists as state the node keeps.
- p99 must be computable across nodes and over any time window in Grafana. Percentiles computed inside each process
  cannot be aggregated; histogram buckets can (`histogram_quantile` over summed bucket rates).
- The benchmarks of this phase must be able to run with and without metrics, to show their cost.

## Decision

**Meters, recorded with the Micrometer API in `io.keyforge.server.metrics`:**

- `keyforge.commands` (`keyforge_commands_seconds` in Prometheus): one timer per command, tagged `command`, published
  as a histogram with buckets from 10 µs to 10 s; `keyforge.command.errors` counts error replies. The dispatcher calls a
  `CommandObserver` (the command package does not depend on Micrometer) with the command and its duration. Meters are
  created up front for every command in the table, so recording is an identity-map lookup and the timer's lock-free
  update, with no allocation. Without metrics the observer is `NONE` and the dispatcher does not read the clock.
- Raft: `keyforge_raft_leader` (1 on the leader), `keyforge_raft_role` (one series per role), `keyforge_raft_term`,
  `keyforge_raft_commit_index`, `keyforge_raft_applied_index`, `keyforge_raft_last_log_index`,
  `keyforge_raft_snapshot_index` and `keyforge_raft_apply_lag` (committed minus applied). These are gauges over the
  runtime's volatile `RaftStatus` snapshot, read when Prometheus scrapes.
- LSM: flushes, flushed bytes, compactions, bytes compactions read and wrote, time spent compacting (counters), and
  SSTables, their bytes, memtable bytes and memtables waiting to be flushed (gauges), all from a new
  `LsmKeyValueStore.stats()` snapshot. The storage module stays free of Micrometer.
- The JVM's own meters (memory, GC pauses, threads, class loading, CPU, uptime) from Micrometer's binders.
- Every series carries a `node` tag with the node id.

**Exposure:** `metrics.port` (default 0, off) starts a Prometheus registry and serves `GET /metrics` on that port, with
the JDK's built-in `com.sun.net.httpserver` server and a virtual thread per request. It answers in the Prometheus text
format (0.0.4), `HEAD` too, 404 elsewhere and 405 for other methods. It is separate from the RESP port, so a scrape
never runs on a Netty event loop or a connection thread.

**Dashboard and quick start:** `deploy/grafana/dashboards/keyforge.json` (uid `keyforge`) with rows for Raft (leader,
term, term by node, commit index, apply lag), commands (p99 by command and by node, throughput, errors), LSM storage
(flushes and compactions, compaction I/O and time, SSTables, memtables) and the JVM (heap, GC pause time). The root
`docker-compose.yml` builds the server from source (`Dockerfile`), starts three nodes with `metrics.port=9464`,
Prometheus scraping them every 5 s, and Grafana with the data source and the dashboard provisioned.

## Consequences

- `GrafanaDashboardTest` parses the dashboard and checks that every `keyforge_*` and `jvm_*` metric its queries use is
  in the scrape of a cluster node's meters; a renamed meter fails the build instead of leaving an empty panel.
  `MetricsEndpointIT` scrapes a real node after real commands.
- Metrics cost about 0.3 to 0.6 µs per command (two clock reads in a VM and the histogram update): within the noise
  without pipelining, 11% to 29% more CPU per request with a pipeline of 16 on the in-memory engine
  (`docs/benchmarks/keyforge-vs-redis.md`, "Cost of metrics"). They are off by default, and the published benchmarks
  ran without them.
- The latency in `keyforge_commands_seconds` is the server's view: from dispatch to reply on the node that answered,
  including a follower's forwarding and a leader's wait for the Raft commit, but not network time or time spent queued
  in socket buffers. It complements, and cannot replace, the client-side open-loop measurements.
- A forwarded command is counted on the follower that received it and on the leader that executed it.
- Raft meters exist only on cluster nodes; the dashboard's Raft row is empty for a standalone node.
- There is no `keyforge-admin` module yet, so there are no admin endpoints (cluster status, trigger compaction).
  `KeyForgeNode.compact()` and `raft().status()` are the hooks they would call.

## Alternatives considered

- **`keyforge-admin` with Spring Boot Actuator** (the spec's module layout): `/actuator/prometheus` and admin endpoints
  for free, at the price of a Spring Boot application around the node, a second way to configure it, and Spring Boot's
  dependency management overriding Netty's and gRPC's versions in that module. Deferred by the user's choice; the
  meters above are what it would export, unchanged.
- **Netty for the endpoint.** It would share the RESP server's dependencies, but a scrape every few seconds does not
  need an event loop, and the JDK's server keeps the endpoint independent of `server.type`.
- **Client-side percentiles (`publishPercentiles(0.99)`).** Cheaper to scrape, but a p99 per node cannot be combined
  into a cluster p99 or re-windowed in Grafana.
- **Timing commands in the network handlers.** That would include queueing behind other requests of the same
  connection, which differs between Netty and virtual threads; the dispatcher sees both front ends the same way.
