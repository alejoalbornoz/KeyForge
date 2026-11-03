# ADR 0018: Benchmark against Redis with two tools on one machine: redis-benchmark for throughput, an open loop for latency

- Status: accepted
- Date: 2026-11-03
- Phase: 6

## Context

Phase 6 asks for `redis-benchmark` results against KeyForge (one node and a three-node cluster) and against real Redis
on the same machine, with an honest analysis of the gap, and for latency reports that account for coordinated omission.

- **redis-benchmark is a closed loop.** Each client sends its next request only after the previous reply. When the
  server stalls, the clients stop sending, and the requests that would have waited are never issued. Its percentiles
  therefore describe the requests it managed to send, not what a user arriving at a steady rate would see
  (coordinated omission). It is the standard throughput tool, and the spec names it, so its throughput figures belong
  in the comparison, but its latencies cannot be the latency report.
- **The only machine is a desktop running Docker Desktop on Windows**, with 4 vCPUs in a Hyper-V VM. Phase 2 measured
  a jitter floor of 10 to 20 ms at the extreme tail there, with stalls that hit every process at once. A latency
  difference between two servers is only meaningful against that floor.
- **Redis and KeyForge use cores differently.** Redis 8 runs commands on one thread. KeyForge runs them on every
  event loop (Netty) or connection thread (virtual threads), and adds GC, JIT, flush and compaction threads. Requests
  per second alone would hide how much CPU each one spent.
- **"Same machine" must not mean "sharing cores with the load generator".** A client competing with the server for a
  CPU measures the scheduler.

## Decision

One script, `keyforge-bench/scripts/compare-with-redis.sh`, runs every target the same way and keeps the raw data:

- **Targets:** Redis 8 without persistence; Redis 8 with an append-only file fsynced every second (the durability
  closest to KeyForge's group commit); KeyForge on the in-memory engine; KeyForge on the LSM engine with group commit,
  behind Netty and behind virtual threads; a three-node KeyForge cluster, with clients on the leader and on a follower.
- **Isolation:** every server container (all three cluster nodes too) is pinned to vCPUs 0 and 1; the clients to vCPUs 2
  and 3. Everything runs on one Docker bridge network, never through Docker Desktop's port proxy. Each target starts
  fresh, and again before the open-loop runs, so that the closed-loop runs' keys and counters do not carry over.
- **Throughput: redis-benchmark itself,** from the `redis:8` image: `SET`, `GET` and `INCR`, 50 clients, 64-byte values,
  100,000 random keys, two client threads, without pipelining and with a pipeline of 16. An untimed warm-up of the same
  commands precedes the measurement on every target, Redis included, so that the JVM's JIT compilation is not counted.
- **CPU per request:** the server containers' cgroup CPU time across each redis-benchmark run, divided by its requests.
  This shows efficiency independently of how many cores a server can use.
- **Latency: KeyForge's own open-loop load generator** (Phase 2), against every target including Redis. Requests are
  80% `GET` and 20% `SET`, at fixed offered rates (20,000 and 60,000 requests per second for one node, 500 and 1,000 for
  the cluster), from 50 connections. Each latency is measured from the request's intended send time, so a stall counts
  against every request it delays. Distributions are kept in HdrHistogram files, and the report plots them as percentile
  spectra (`log10(1 / (1 - p))` on the x axis, as HdrHistogram's own plots do). A per-second view (the median of the
  per-second p99s) separates the tail of a typical second from isolated stalls.
- **Redis is the control.** Whatever latency Redis shows at a given rate is what this machine adds; KeyForge is judged
  by its distance from the Redis curve, not by absolute tail numbers.
- **The report is generated:** `io.keyforge.bench.report.BenchmarkReport` reads the CSV and `.hgrm` files and writes the
  SVG charts and Markdown tables, so the charts in the README can be regenerated from the raw data
  (`REPORT_ONLY=1 OUT=<results> compare-with-redis.sh`).
- **Profiles explain the gap.** `PROFILE=1` records every KeyForge server with JFR (`settings=profile`); the analysis
  cites its hot methods, allocations and GC pauses rather than guessing.

## Consequences

- The numbers compare servers, not machines. They are honest about this desktop, and they say nothing about the
  absolute capacity of either system on dedicated Linux hardware.
- The cluster runs three nodes on two vCPUs: replication costs CPU on the same cores that serve requests, and every
  Raft log fsync goes to the same virtual disk. A real deployment spreads the nodes over machines; this setup shows the
  cost of consensus at its most concentrated.
- Two passes of the matrix take about an hour, most of it spent in the cluster's slow writes and in the open-loop runs.
- The load generator reaches about 100,000 requests per second on two vCPUs (Phase 2), so the open-loop rates stay
  below that; redis-benchmark, written in C, drives the closed-loop runs further.
- The load generator prepopulates with `MSET`s of 100 keys, so that a Raft cluster loads 100,000 keys with a thousand
  log entries instead of 100,000.

## Alternatives considered

- **redis-benchmark's latencies as the latency report.** They suffer from coordinated omission by construction; under a
  stall they look better the worse the stall is.
- **memtier_benchmark or wrk2-style tools.** memtier is closed loop too. A wrk2 equivalent for RESP is exactly what the
  Phase 2 load generator already is, with its own timer lag reported, so it was reused rather than adding a tool.
- **Benchmarking from the Windows host.** Docker Desktop's port proxy capped Phase 1's measurements at about 100,000
  requests per second, and it would add its own latency to every request.
- **Giving Redis one vCPU because it uses one thread.** It also uses kernel time for networking on other cores, and
  KeyForge would then have twice its CPU budget. Both get the same two vCPUs, and CPU per request is reported alongside.
