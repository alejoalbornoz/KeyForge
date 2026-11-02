# ADR 0016: Fault injection: a network container per node, iptables and Toxiproxy, signals to the server

- Status: accepted
- Date: 2026-10-29
- Phase: 5

## Context

The Phase 5 specification asks for a Docker Compose cluster of five nodes and three families of fault: network
partitions (`docker network disconnect` or iptables), latency and packet loss (`tc netem` or Toxiproxy), and process
kills and pauses (SIGSTOP). Phase 4's Compose file ran three nodes, one container each.

Three facts about the development machine shaped the design. Each was checked before it was relied on.

- **Docker Desktop's WSL 2 kernel has no netem.** Its configuration (`/proc/config.gz`) has
  `# CONFIG_NET_SCH_NETEM is not set`, so `tc qdisc add ... netem` fails with "Specified qdisc kind is unknown", and the
  module cannot be loaded. The `iptables` statistic match (`CONFIG_NETFILTER_XT_MATCH_STATISTIC=y`) is available.
- **iptables works inside containers** granted `NET_ADMIN` (nf_tables backend), and `docker kill -s STOP` stops a
  container's PID 1, which is the JVM.
- **Each `docker exec` takes 0.5–0.8 s** from the Windows host.

A partition must not cut the test's clients off from the nodes, or every operation on a cut-off node would be lost
rather than tested. And when a server is killed and restarted, the fault it was part of should not depend on whether
its container kept its address.

## Decision

**Two containers per node** (`keyforge-chaos/docker-compose.yml`, image `keyforge-chaos/docker/Dockerfile`):

- `kfN-net` owns the node's network namespace: the address, the DNS name `kfN` (a network alias), and the published
  RESP port (6381–6385 on the host). It runs Alpine with iptables, iproute2 and Toxiproxy 2.12.0, and is never
  stopped.
- `kfN` runs the KeyForge server inside that namespace (`network_mode: service:kfN-net`), on the same
  `eclipse-temurin:25-jdk` image as before.

Killing, pausing or restarting a server therefore leaves its address and any network fault in place, the way a process
crash leaves a machine's network alone. The nemesis always has a running container to apply and remove network faults
in. No server code changed, and `leader-failover.sh` still kills the server with `docker compose kill kfN`.

**The runner is a container too** (`runner`, profile `test`, started by `docker compose run`). It runs on the cluster
network, so clients reach `kfN:6379` directly; ADR 0015 explains why the host's port forwarding would not do. It runs
the same JDK image plus the static `docker` command-line client (copied from `docker:29.0-cli`). The host's Docker
socket is mounted into it, and the nemesis's `docker kill`, `docker start` and `docker exec` go to the host's daemon.
The runner jar is mounted from `keyforge-chaos/target`.

**Partitions: iptables in the network container.** A partition is a set of "grudges" (Jepsen's term): for each node, the
peers whose packets it drops (`iptables -A INPUT -s <peer> -j DROP`). Every shape is symmetric, and only node-to-node
traffic is dropped, so clients keep reaching every node. There are five shapes (`Partitions`, `FaultPlanner`):

| Shape | Effect |
| --- | --- |
| `isolate-node` | A random node is cut off from every other node |
| `isolate-leader` | The current leader is cut off, so the old leader keeps believing it leads while a new one is elected |
| `halves` | A random 2 | 3 split |
| `ring` | Each node reaches only its two ring neighbours: every node sees a majority, no two see the same one |
| `bridge` | Two pairs that cannot reach each other, joined by one node that reaches both |

Healing flushes the `INPUT` chain. The redirect that the latency fault relies on lives in the `nat` table and is not
touched.

**Latency: Toxiproxy on the Raft port.** At start-up, `net-init` starts Toxiproxy in the network namespace, creates a
proxy `raft` from port 17000 to `127.0.0.1:7000`, and redirects inbound connections to port 7000 into it
(`iptables -t nat -A PREROUTING -p tcp --dport 7000 -j REDIRECT --to-ports 17000`). Every Raft and forwarding
connection a node accepts therefore passes through its proxy, which only relays bytes until the nemesis adds latency
toxics in both directions (20–300 ms, jitter half of that). Source addresses survive the redirect, so partitions still
apply. Peers are identified by the node ids inside the messages (ADR 0014), so the transport does not mind that every
inbound connection now comes from 127.0.0.1.

**Packet loss: the iptables statistic match.** `iptables -A INPUT -s <peer> -m statistic --mode random --probability p
-j DROP` for every peer: 5–25 % of node-to-node packets are dropped, at random, in the kernel.

**Process faults: signals to the server container.**

- A kill is `docker kill -s KILL kfN`, and healing it is `docker start kfN`.
- A pause is `docker kill -s STOP kfN` (SIGSTOP to the JVM, which a process cannot catch), and healing it is
  `-s CONT`.

Targets are one random node, the leader, a minority (two nodes) or a majority (three).

**The `wipe` self-test** is not a realistic fault. Every node is killed, a majority loses its data (deleted through the
server containers' volumes), restarts first and elects a leader of its own, and then the rest restart. Raft cannot
survive losing a majority's disks, so acknowledged writes disappear and the checker must report the run as not
linearizable. It shows, end to end, that the pipeline catches lost writes. `all` never includes it.

**One fault at a time** (`Nemesis`). Each cycle is a calm pause (`--calm-seconds`, 5 s on average), then a fault drawn
from the enabled kinds by a seeded planner and held for `--fault-seconds` (10 s on average), then healing. Each
duration is drawn between half and one and a half times its mean. Every action is recorded twice in the history: an
`invoke` when it begins and an `info` when it is done. A fault may thus be in effect anywhere from its start's
invocation to its stop's completion, and the checker draws exactly that span on the visualization's timeline. Commands
for several nodes run in parallel. When the run ends, and before it starts, everything is healed on a best-effort
basis: paused servers resumed, stopped ones started, rules flushed and toxics removed.

## Consequences

- One code path works on Docker Desktop for Windows and macOS and on Linux. The same faults would also work on a
  kernel with netem; netem is simply not needed.
- Raft traffic always crosses a userspace proxy, even without toxics. The Phase 4 failover check, rerun on the
  five-node cluster through the proxies, still passes: a new leader in 3.2 s, every acknowledged write on all four
  survivors.
- Latency applies to the connections a node accepts. Its own outbound connections are delayed by the receiver's proxy,
  so delaying every node delays every hop, but delaying one node delays the traffic into it plus the replies on those
  connections. Clients' RESP traffic is never delayed or dropped.
- Faults outlive server restarts, and container addresses never change, so grudges are computed from addresses
  resolved once per run.
- Faults take effect within about a second, and the history records the exact window. From the runner container, a
  kill, pause or resume took 50–250 ms. Latency took 0.25–0.6 s to install on its targets, and packet loss on all five
  nodes about 1.3 s.
- The runner holds the Docker socket, which is root on the Docker host. That is acceptable for a local test harness
  that runs this repository's own code. It is not a pattern for anything deployed.
- Ten containers plus a client container per cluster. During a run they used about 625 MiB in all: 80–150 MiB per
  server (heaps capped at 512 MB) and 5–8 MiB per network container.
- One fault at a time is easier to reason about than Jepsen's composed nemeses, but it never combines, say, a pause
  with a partition. Overlapping faults are a possible extension.
- Clock skew is not injected. Expiry is the only clock-dependent behaviour (ADR 0011), and the workload does not use
  TTLs.

## Alternatives considered

- **`docker network disconnect`.** It is simple, but a disconnected node also loses its clients, a node can only be cut
  off whole (no `ring` or `bridge`), and a container may get a new address when it reconnects.
- **`tc netem` for latency and loss.** It is the standard tool, but it is unavailable on this kernel.
- **Toxiproxy for everything**, including partitions (a `timeout` toxic blackholes a connection). It is portable, but
  a TCP proxy can only stall connections; it cannot drop packets, and it treats connections that are already open
  differently from new ones. iptables gives real packet-level partitions and loss.
- **One shared Toxiproxy container with a proxy per pair of nodes.** It allows per-pair latency, but every node would
  need its own member list pointing at the proxies, and partitions would have to be done in the proxy too.
- **A single container per node with iptables inside** (Phase 4's layout plus `NET_ADMIN`). A killed container cannot
  run `docker exec`, its rules and addresses are lost on restart, and the nemesis would have to re-apply faults to
  restarted nodes.
- **`docker pause`** (the cgroup freezer) instead of SIGSTOP. The effect is similar, but the specification names
  SIGSTOP, and the signal reaches the JVM the way an operator's `kill -STOP` would.
