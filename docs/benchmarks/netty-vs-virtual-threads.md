# Netty vs virtual threads

Phase 2 of KeyForge has two interchangeable network front ends (`server.type`):

- **Netty**: the Phase 1 server. Netty 4.2 event loops over the NIO transport, with commands run on the connection's
  event loop ([ADR 0002](../adr/0002-threading-model-and-in-memory-store.md),
  [ADR 0004](../adr/0004-flushing-and-backpressure.md)).
- **Virtual threads**: a `ServerSocket` accept loop and one virtual thread per connection doing blocking reads and
  writes ([ADR 0005](../adr/0005-virtual-thread-server-and-pinning.md)).

Both use the same RESP codec, command dispatcher and in-memory store, behave the same way (one write per read batch,
backpressure, `QUIT` and protocol errors), and pass the same integration suite. This document compares them at 1k,
10k and 50k connections.

**Read this first.** These numbers come from a desktop: two cores for the server and two for the load generator, inside
Docker Desktop on Windows. Every cell was run twice (two full passes, 48 runs, zero errors). Medians, throughput at 1k
connections, memory and threads reproduce across passes. Tail latencies (p99 and above) do not: in 5 of the 9
open-loop cells, the two passes disagree about which server has the lower p99. Treat the tails as a description of
this environment, not as a ranking of the servers.

## Summary

| | Netty | Virtual threads |
| --- | --- | --- |
| Max throughput, 1k connections | **91–114k req/s** | 74–96k req/s (16–19% lower in both passes) |
| Max throughput, 10k / 50k connections | 69–87k / 70–84k req/s | 87–90k / 72–86k req/s (no consistent gap) |
| Median latency at 20k req/s | 0.16–0.19 ms | 0.19–0.24 ms |
| Median latency at 40k req/s | **0.15–0.34 ms** | 0.8–1.8 ms in 5 of 6 runs |
| Server CPU at 20k req/s | **0.74–1.06 cores** | 0.99–1.16 cores (9–34% more, every pair) |
| Live heap per idle connection | **1.6–1.9 KiB** | 6.7–9.3 KiB |
| 50k idle connections: live heap / RSS | **118 MiB / 238–260 MiB** | 352–360 MiB / 492–549 MiB |
| OS threads, 1k to 50k connections | 26 | 26 (27 at peak) |
| 50k connections at 60k req/s | fell behind (55k and 47k req/s delivered) | kept up, with 75–812 ms median latency |
| Connection-handling code | pipeline of 3 handlers, writability callbacks | about 50 lines of sequential code |

- **Throughput.** Netty serves 19–23% more requests at 1k connections. At 10k and 50k the two are within the run-to-run noise:
  both servers sit at ~1.9 of their 2 cores, and the load generator is close to its own limit.
- **Latency.** Netty's median stays flat at 0.15–0.2 ms whenever it keeps up. The virtual-thread server matches it
  at 20k req/s, but at 40k req/s its median rises 5–10×. Tails are dominated by GC pauses and environmental stalls
  that hit both servers.
- **Memory** is the clearest and most deterministic difference. A parked virtual thread keeps its 4 KiB receive
  buffer and its 2.9 KB stack chunk, about 4× Netty's cost per connection.
- **Threads.** "One thread per connection" costs no OS threads. Both servers run 26 OS threads at every connection
  count.

## Results

All latencies in milliseconds. Each cell shows `pass 1 / pass 2`.

### Maximum throughput (closed loop)

| Connections | Server | Throughput (req/s) | Server CPU (cores) | Load-generator CPU (cores) |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Netty | 113,599 / 91,307 | 1.96 / 1.92 | 1.67 / 1.82 |
| 1,000 | Virtual threads | 95,615 / 74,000 | 1.97 / 1.93 | 1.51 / 1.70 |
| 10,000 | Netty | 86,796 / 68,794 | 1.85 / 1.83 | 1.82 / 1.82 |
| 10,000 | Virtual threads | 86,804 / 89,821 | 1.91 / 1.91 | 1.73 / 1.71 |
| 50,000 | Netty | 84,469 / 70,356 | 1.90 / 1.89 | 1.87 / 1.85 |
| 50,000 | Virtual threads | 85,804 / 72,214 | 1.89 / 1.90 | 1.82 / 1.81 |

Throughput moved by up to 20% between passes, mostly downwards and for both servers. That is how much the host's
background load varies, and it is why only differences that appear in both passes are claimed. Closed-loop latencies (by Little's law,
roughly connections ÷ throughput) are in `results.csv` but say nothing beyond the throughput.

### Latency at a fixed offered rate (open loop)

*Median 1-s p99* is the median of the 30 per-second p99 values: the tail of a typical second, which a single stall
cannot move. *Stalled s* counts seconds whose throughput was more than 10% off the offered rate, or whose p99 passed
200 ms.

| Conns | Offered | Server | Delivered | p50 | p99 | p99.9 | Median 1-s p99 | Stalled s | Server CPU |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1k | 20k | Netty | 20.0k / 20.0k | 0.16 / 0.17 | 22 / 10 | 50 / 25 | 3.4 / 5.0 | 0 / 0 | 0.74 / 0.78 |
| 1k | 20k | VT | 20.0k / 20.0k | 0.20 / 0.20 | 26 / 7.1 | 46 / 17 | 6.9 / 3.1 | 0 / 0 | 0.99 / 1.01 |
| 1k | 40k | Netty | 40.0k / 40.0k | 0.15 / 0.16 | 20 / 21 | 44 / 52 | 11.6 / 8.8 | 0 / 0 | 1.22 / 1.25 |
| 1k | 40k | VT | 40.0k / 40.0k | 1.55 / 0.81 | 197 / 22 | 320 / 36 | 21.2 / 9.5 | 2 / 0 | 1.21 / 1.16 |
| 1k | 60k | Netty | 60.0k / 60.0k | 0.17 / 0.15 | 192 / 26 | 304 / 43 | 24.3 / 20.8 | 1 / 0 | 1.52 / 1.49 |
| 1k | 60k | VT | 60.0k / 60.0k | 0.64 / 1.05 | 28 / 389 | 58 / 419 | 19.6 / 24.2 | 0 / 4 | 1.54 / 1.64 |
| 10k | 20k | Netty | 20.0k / 20.0k | 0.16 / 0.19 | 13 / 59 | 37 / 116 | 7.0 / 19.6 | 0 / 0 | 0.91 / 1.06 |
| 10k | 20k | VT | 20.0k / 20.0k | 0.19 / 0.23 | 9.3 / 72 | 26 / 136 | 4.6 / 11.7 | 0 / 0 | 1.08 / 1.16 |
| 10k | 40k | Netty | 39.9k / 40.0k | 0.22 / 0.21 | 945 / 101 | 1,195 / 155 | 30.0 / 21.8 | 6 / 0 | 1.51 / 1.52 |
| 10k | 40k | VT | 40.0k / 40.0k | 1.76 / 1.67 | 113 / 48 | 161 / 94 | 24.3 / 16.9 | 0 / 0 | 1.45 / 1.35 |
| 10k | 60k | Netty | 60.0k / 60.1k | 0.21 / 0.18 | 84 / 79 | 144 / 153 | 28.5 / 29.0 | 0 / 0 | 1.41 / 1.46 |
| 10k | 60k | VT | 60.0k / 60.1k | 0.21 / 0.21 | 63 / 94 | 81 / 145 | 30.0 / 31.2 | 0 / 0 | 1.62 / 1.64 |
| 50k | 20k | Netty | 20.0k / 20.0k | 0.17 / 0.16 | 30 / 16 | 55 / 40 | 15.2 / 10.5 | 0 / 0 | 0.97 / 0.90 |
| 50k | 20k | VT | 20.0k / 20.0k | 0.20 / 0.24 | 23 / 22 | 66 / 77 | 7.9 / 15.2 | 0 / 0 | 1.11 / 1.13 |
| 50k | 40k | Netty | 40.0k / 40.0k | 0.34 / 0.17 | 234 / 35 | 309 / 59 | 35.0 / 24.4 | 3 / 0 | 1.54 / 1.41 |
| 50k | 40k | VT | 40.0k / 40.0k | 80 / 0.91 | 2,777 / 111 | 2,934 / 160 | 247 / 23.8 | 16 / 0 | 1.74 / 1.47 |
| 50k | 60k | Netty | **55.4k / 46.8k** | 192 / 29 | 2,506 / 6,031 | 2,988 / 6,778 | 633 / 269 | 25 / 17 | 1.84 / 1.82 |
| 50k | 60k | VT | 61.6k / 60.1k | 75 / 812 | 929 / 1,441 | 1,146 / 1,535 | 182 / 1,025 | 15 / 30 | 1.92 / 1.92 |

Full distributions: `run-1/*.hgrm` and `run-2/*.hgrm`.

### Memory and threads

Live heap is measured after a full GC (`jcmd GC.run`), RSS from `/proc`. *Base* is after the keyspace was loaded and
before connecting; *idle* is with every connection open and no traffic. Ranges cover all 8 runs per row (4 rates × 2
passes).

| Connections | Server | Live heap base → idle (MiB) | Heap per connection | RSS idle (MiB) | RSS peak under load (MiB) | OS threads |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | Netty | 25 → 27 | 1.6–1.7 KiB | 121–130 | 175–210 | 26 |
| 1,000 | Virtual threads | 23 → 32 | 9.0–9.3 KiB | 98–102 | 145–158 | 26 |
| 10,000 | Netty | 25 → 44 | 1.9 KiB | 154–160 | 187–246 | 26 |
| 10,000 | Virtual threads | 23 → 100–106 | 7.8–8.5 KiB | 187–197 | 290–384 | 26 |
| 50,000 | Netty | 25 → 118 | 1.9 KiB | 238–260 | 300–409 | 26 |
| 50,000 | Virtual threads | 23 → 352–360 | 6.7–6.9 KiB | 492–549 | 734–859 | 26–27 |

At 1k connections, Netty's idle RSS is *below* its base: the forced full GC for the base reading let G1 uncommit
memory. RSS at that scale tracks the GC's sizing decisions rather than the connections; live heap does not.

Class histograms with 10k idle connections ([virtual threads](results/2026-10-27/class-histogram-10k-idle-virtual-threads.txt),
[Netty](results/2026-10-27/class-histogram-10k-idle-netty.txt)) show where the memory goes:

- **Virtual threads, about 8.3 KB per connection.** About 4.5 KB of `byte[]`: the 4 KiB receive buffer and the
  256-byte reply buffer. 2.9 KB of `jdk.internal.vm.StackChunk`: the frozen stack of a thread parked in a socket read.
  About 170 B for the `VirtualThread` and 50 B for its continuation. About 300 B for the `ThreadLocal` map holding the
  MDC. The socket objects (`Socket`, `NioSocketImpl`, `FileDescriptor`, cleaner) add a few hundred bytes more.
- **Netty, about 1.9 KB per connection.** Three handler contexts (64 B each), the channel and its JDK
  `SocketChannelImpl` (110 B each), the pipeline, the outbound buffer, the config and addresses. No buffers: an idle
  Netty channel returns its pooled read buffer after every read.

Both servers run the same 26 OS threads at every connection count. Netty has 4 event loops and an accept loop. The
virtual-thread server has 2 carriers, a `MasterPoller`, `VirtualThread-unblocker` and the accept thread. The other
threads belong to the JVM (GC, JIT, ...) and to the store's active-expiry task.

### Sensitivity experiments (one run each)

**Platform-thread pollers** (`-Djdk.pollerMode=1`, virtual threads only): `Read-Poller` and `Write-Poller` platform
threads replace JDK 25's default of read pollers that are themselves virtual threads. The goal was to test whether the
median rise at 40k req/s comes from pollers queueing behind connections on the two carriers. It made things worse:

| Conns | Offered | Delivered | p50 | p99 | Server CPU |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1k | 20k | 20.0k | 0.22 | 22 | 0.94 |
| 1k | 40k | 40.0k | 2.49 | 95 | 1.25 |
| 1k | 60k | 60.1k | 23.0 | 425 | 1.77 |
| 10k | 20k | 20.0k | 0.24 | 24 | 1.02 |
| 10k | 40k | 40.0k | 9.10 | 107 | 1.49 |
| 10k | 60k | 60.5k | 2,257 | 3,727 | 1.92 |

**ZGC on both servers** (`-XX:+UseZGC` instead of G1) at 40k req/s. The goal was to see whether the tails are G1
pauses. They did not shrink. On two cores, ZGC's concurrent threads compete with the server for CPU.

| Conns | Server | p50 | p99 | p99.9 | Server CPU |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1k | Netty | 1.11 | 1,096 | 2,213 | 1.47 |
| 1k | Virtual threads | 3.95 | 308 | 429 | 1.36 |
| 10k | Netty | 0.39 | 394 | 607 | 1.52 |
| 10k | Virtual threads | 2.39 | 47 | 100 | 1.40 |

Raw data: [vt-poller-mode-1](results/2026-10-27/vt-poller-mode-1) and
[zgc-servers](results/2026-10-27/zgc-servers).

## Analysis

### Throughput and CPU: the virtual-thread server spends more CPU per request

At 1k connections both servers are CPU-bound at about 1.95 of 2 cores. The load generator still has headroom (1.5–1.8
cores), and Netty serves 19–23% more requests in both passes. The same picture shows at a fixed 20k req/s: the
virtual-thread server burns more CPU for the same work: 9–34% more in each of the 6 pairs.

The likely cause, consistent with the CPU data but not confirmed by profiling, is the work per request around each
blocking call:

- **Netty.** An event loop gets a batch of ready channels from one `epoll_wait`, then reads, executes and writes each
  of them inline on the same thread.
- **Virtual threads.** Every request goes through a full cycle. The thread parks in `read`, which freezes its stack
  into a heap stack chunk and registers the socket with the poller. Then the poller sees the socket readable and
  unparks the thread. The thread is queued on the scheduler, a carrier mounts it (thawing frames back), and only then
  does it read, execute, write and park again.

The cycle is cheap compared with a platform-thread context switch, but it is not free. Netty skips it entirely.

At 10k and 50k connections the gap disappears, within a run-to-run spread of ±20%. Two effects probably contribute,
and neither was isolated:

- the load generator reaches 1.7–1.9 of its 2 cores, so both servers are measured against the same ceiling;
- Netty's NIO selector does more bookkeeping per wake-up with 2,500–12,500 channels per event loop.

Running the comparison again with Netty's native epoll transport and a faster load-generator host is the way to
separate the two.

### Latency: equal at light load; the virtual-thread median rises under moderate load

At 20k req/s both medians are around 0.2 ms; the virtual-thread server is 0.03–0.05 ms slower. At 40k req/s Netty
stays at 0.15–0.34 ms, while the virtual-thread median rises to 0.8–1.8 ms in 5 of 6 runs; the sixth is a stalled run.
Its per-second medians show this is sustained, not a few bad seconds: in pass 1 at 1k connections, 0.8–3.3 ms in all
but one second, against 0.13–0.17 ms in nearly every second for Netty.

We expected queueing in the scheduler: with 2 carriers, a woken connection waits behind every other runnable virtual
thread. With pollers that are themselves virtual threads, the pollers also wait in that queue. The platform-poller
experiment refutes the simple form of this idea: dedicated poller threads made medians worse (2.5–9 ms at 40k), not
better. The extra hand-off between platform threads costs more than it saves.

One result does not fit and stays unexplained: at 10k connections and 60k req/s the virtual-thread median was
0.21 ms in both passes, as low as Netty's. A plausible story is that at higher load the carriers rarely go idle, so
waking a connection never has to unpark a sleeping carrier first. That needs a profile (JFR CPU sampling with the
virtual-thread scheduler events) before anyone relies on it.

**Tails are not a property of either server here.** p99 and above swing by an order of magnitude between passes of the
same cell (Netty at 10k/40k: 945 ms and then 101 ms), and the passes disagree about the winner in 5 of 9 cells. Three
things feed the tails:

- **G1 pauses.** Over whole runs at 50k connections, young and mixed pauses averaged 19 ms for Netty (15 over 50 ms)
  and 24 ms for virtual threads (67 over 50 ms). The virtual-thread heap holds about 3× more live objects, mostly
  stack chunks and buffers, and it shows.
- **Environmental stalls.** Several stalls lasted seconds with no matching GC pause. In pass 1 of Netty at 10k/40k,
  throughput fell to 24k req/s for two seconds while the longest GC pause was 42 ms. Even at 5k req/s, with both
  processes mostly idle, the load generator's timers fired up to 19 ms late
  ([calibration run](results/2026-10-27/calibration-1k-5000rps)): that is the floor of this Hyper-V-hosted VM.
- **Catch-up bursts.** After any stall, every connection has an overdue request, and the burst of replies and new
  requests delays the load generator's timers (*timer lag* in `results.csv`). Each stall gets amplified a little.

The *median 1-s p99* column ignores isolated stalls. Apart from the overloaded 50k/60k cell and one stalled run
(virtual threads, 50k/40k, pass 1), it keeps the two servers within about 2× of each other in every cell, and which
one is lower changes from cell to cell. Switching the servers to ZGC did not help (see above).

**Overload at 50k connections and 60k req/s.** Netty delivered only 55.4k and then 46.8k req/s, with multi-second
latencies. The virtual-thread server delivered the full rate in both passes, but with medians of 75 ms and then
812 ms, so it was saturated too. With two samples and both servers past capacity, the safe reading is "the
virtual-thread server degraded more gracefully in this one setup", not "it has more capacity". Its closed-loop maximum
at 50k was the same as Netty's.

### Memory: the clearest difference, and part of it is ours to fix

Per connection, the virtual-thread server holds about 4× Netty's live heap, and the measurement is identical in every
run. That makes 240 MiB extra at 50k idle connections, and a correspondingly larger heap for G1 to manage, which
shows in its pause times. The histogram splits the cost:

- **The 4 KiB receive buffer** (`ReceiveBuffer`, ADR 0005) is a choice we made. A parked thread must keep its buffer,
  because the blocking `read` needs a destination. Starting at 512 B and growing on demand would cut about 3.5 KiB per
  connection, at the cost of more reads for pipelining clients. That is a measured, open optimization.
- **The 2.9 KB stack chunk** is inherent to a parked virtual thread with this call depth (the executor, `serve`,
  `Connection.serve`, `ReceiveBuffer.readFrom`, the socket stream, `NioSocketImpl`, park). It could only shrink with a
  shallower call stack.
- **The MDC map** (about 300 B) exists because every connection thread puts `nodeId` in its MDC. A `ScopedValue`, or
  setting the MDC only around log calls, would remove it.

### Threads: no difference

The virtual-thread server runs exactly as many OS threads as Netty at 1k, 10k and 50k connections. Its per-connection
threads cost heap (the stack chunks above), not kernel threads, stacks or scheduler slots. The integration test
`VirtualThreadServerIT.connectionsDoNotCostAPlatformThreadEach` asserts this too.

### Code

`vthreads/Connection.java` is one loop: read, decode, dispatch, encode, write. Backpressure needs no code, because a
blocked `write` stops reading. The Netty front end needs a frame decoder, a handler with writability callbacks and
`autoRead` toggling, a reply encoder, and a separate `EmbeddedChannel` test harness. Both are small, but the
virtual-thread version is the one a newcomer can read top to bottom, and its stack traces show exactly where a stuck
connection is.

## Conclusion

On this hardware, Netty is the better default for KeyForge:

- 19–23% more throughput at 1k connections;
- a flat ~0.2 ms median under moderate load;
- about 4× less memory per connection.

The virtual-thread server is a credible alternative, not a toy:

- the same OS thread count;
- the same throughput at 10k and 50k connections;
- the same behavior on the integration suite, with no pinning on JDK 25;
- markedly simpler code.

Its weak points are the median latency under moderate load, which we cannot explain yet, and memory per connection,
which we partly can fix.

Netty stays the default (`server.type=netty`). Before Phase 6 draws conclusions against real Redis, rerun this on
dedicated Linux hardware, with Netty's epoll transport and a load generator that has headroom.

## Threats to validity

- **Environment.** Docker Desktop on Windows: the Linux VM's vCPUs are scheduled by Hyper-V alongside the Windows host.
  The jitter floor is about 20 ms, and the host's background load shifted throughput by up to 20% between passes.
- **Scale.** Two server cores and two load-generator cores. At 10k and 50k connections the load generator is close to
  saturation in closed loop.
- **Samples.** Two passes per cell, plus one run per sensitivity experiment. Differences that appear in only one pass
  are not claimed.
- **Configuration.** Untuned defaults on both sides. Netty runs 4 event loops on 2 cores over NIO; native epoll is not
  wired into the server (open issue). The virtual-thread server runs 2 carriers. Both use G1 with a 2 GiB heap cap.
- **Workload.** One request in flight per connection, 80% `GET`, 64-byte values, an in-memory store. Pipelining
  clients, large values and the Phase 3 LSM engine (with file I/O, which does not unmount virtual threads) may change
  the picture.
- **Load generator.** It is built on Netty (epoll), which could be suspected of favoring the Netty server. It shares
  no code path with either server beyond the RESP encoder, and its timer lag is reported so that its own
  contribution is visible.

## Setup

**Machine.** A Windows 10 desktop with 4 logical CPUs and 16 GiB of RAM. Everything runs in Linux containers under
Docker Desktop 29.6 (WSL2 VM: 4 vCPUs, 7.7 GiB, kernel 5.15.167). Both servers and the load generator run in image
`eclipse-temurin:25-jdk` (Temurin 25.0.4.1); the jars were built with Temurin 25.0.5.

**Isolation.** Each run starts a fresh server container. The server is pinned to vCPUs 0–1 and the load generator to
vCPUs 2–3 (`--cpuset-cpus`), so they never compete for a core. Both containers sit on one Docker bridge network. No
traffic crosses Docker Desktop's port proxy, which capped Phase 1's measurements at ~100k req/s.

**Server JVMs.** Identical for both: `-Xmx2g -XX:+UseG1GC` and otherwise the JDK defaults. On 2 CPUs that means:

- **Netty**: 4 event loops (Netty's default, 2 × CPUs) plus its accept loop.
- **Virtual threads**: the JDK's default scheduler with 2 carrier threads. On Linux, JDK 25 runs a platform
  `MasterPoller` thread, and its read pollers are themselves virtual threads.

Both processes end up with about 25 OS threads; `jcmd Thread.print` shows the breakdown.

**Load generator** (`keyforge-bench`). Netty 4.2 with the native epoll transport and 2 event loops, running on ZGC so
that its own GC pauses stay out of the measurement. It works like wrk2:

- Each connection has at most one request in flight. There is no pipelining, so every request costs a read and a
  write on both sides.
- Requests are 80% `GET` and 20% `SET` over 100,000 keys with 64-byte values. All keys are set before the run.
- Open loop: the offered rate is split evenly over the connections. Request *k* of a connection is due at
  *start + k × interval*. It is sent then, or as soon as the previous reply arrives if that is later. Its latency is
  measured from when it was *due*, so a server stall counts against every request it delays: no coordinated omission.
  Latencies go into HdrHistogram (3 significant digits).
- *Timer lag*, how late the generator's own timers fire, is recorded separately. It is the load generator's
  contribution to the measured latency.
- Closed loop: each connection sends its next request as soon as a reply arrives. This measures maximum throughput.
  Closed-loop latencies suffer from coordinated omission by construction and are shown only for completeness.

**Server resources.** The load-generator container shares the server's PID namespace (`--pid container:kf-server`).
That lets it read the server's `/proc/1/status` and `/proc/1/stat` every second (RSS, OS threads, CPU time), and
attach `jcmd` to it for the live heap after a full GC (`GC.run` and then `GC.heap_info`). It takes those readings
before connecting and again once every connection is open and idle.

**Timeline of a run.** Start the server, set all keys, take the base readings, open all connections (256 connection
attempts in flight), wait 5 s, take the idle readings, then 10 s of warm-up (discarded) and 30 s of measurement.

**Matrix.** {1k, 10k, 50k} connections × {closed loop, 20k, 40k, 60k req/s offered} × {Netty, virtual threads}. That
is 24 runs per pass, run in two full passes, plus the sensitivity runs and a 5k req/s calibration run.

Raw data is in [results/2026-10-27](results/2026-10-27), one folder per pass or experiment (`run-1`, `run-2`,
`vt-poller-mode-1`, `zgc-servers`, `calibration-1k-5000rps`). Each folder has `results.csv` (one row per run),
`*.hgrm` (full latency distributions), `*.log` (per-second progress), `*.server.log` (server GC log),
`*.client-gc.log` and `environment.txt`.

## Reproducing

```bash
./mvnw -q package -DskipTests
keyforge-bench/scripts/run-benchmarks.sh
```

The script's environment variables (`CONNECTIONS`, `RATES`, `WARMUP`, `DURATION`, `SERVER_CPUS`, `CLIENT_CPUS`,
`SERVER_JAVA_OPTS`, ...) are documented at the top of the script. A full matrix takes about 35 minutes, most of it
spent opening 50k connections: about 80 s per run, because the kernel's search for a free ephemeral port gets slower as
the 64k port range fills.
