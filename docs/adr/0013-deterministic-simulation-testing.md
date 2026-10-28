# ADR 0013: Deterministic simulation testing of Raft

- Status: accepted
- Date: 2026-10-28
- Phase: 4

## Context

Consensus bugs live in rare interleavings: a vote that is sent but not yet on disk when the process dies, a stale
response arriving after a newer one, a partition healing during an election. Tests over real threads and sockets
explore few of them and cannot replay a failure. The specification asks for a seeded simulator running 3–5 nodes with
message loss, reordering, duplication, delays, partitions and crashes with restarts. It must check safety invariants
after every step (at most one leader per term, log matching, state machine safety), a failing seed must reproduce
exactly, and 10,000+ seeds must pass.

## Decision

**A discrete-event simulator in test code** (`keyforge-raft/src/test/java/io/keyforge/raft/sim`). It drives real
`RaftNode` cores, the production protocol code, through a simulated driver. Everything nondeterministic comes from one
`SplittableRandom` seeded with the seed:

- the cluster (3–5 nodes) and the fault rates of the run;
- message loss (0–15%), duplication (0–5%), delays (1–35 ms with a 0–5% tail up to 500 ms; delays alone reorder
  messages), and symmetric, split and one-way partitions;
- node crashes and restarts (up to 1.5 per second), including crashes **part-way through the actions of one event**:
  only a prefix of the actions takes effect, for example a persisted vote whose response was never sent;
- per-node clock rates (±5%);
- AppendEntries batch sizes and pipeline windows, PreVote and CheckQuorum on or off;
- snapshot thresholds and durable state machine images that lag the applied index (as LSM flushes do), so restarts
  exercise "snapshot plus log" recovery;
- a client workload of writes and ReadIndex reads that retries rejections and times out.

Time is virtual: a priority queue of events ordered by (time, sequence number). Each node has a simulated disk holding
exactly what its persistence actions wrote.

**Invariants, checked after every step** (`Invariants`):

- *election safety*: at most one leader per term;
- *log matching*: an (index, term) pair always names the same entry with the same predecessor term, checked on every
  persisted entry; by induction, logs that share an entry share their whole prefix;
- *state machine safety*: no two nodes apply different entries at one index; every state rebuilt from a snapshot or a
  durable image equals the state of applying that prefix of the log (hashes of the applied sequence and of the data);
- *leader completeness*: a new leader's log holds every entry applied anywhere;
- *linearizable reads*: a read's index is at least every index applied anywhere when the read began;
- *driver consistency*: each live node's disk mirrors its core's log, term and applied index.

After the fault phase (3–7 s), the network heals and every node restarts. The run then fails unless, within 10 s of
calm, a leader exists, a final write commits and every node applies the same log (liveness).

**Reproduction.** A failure names its seed and the command that replays it
(`-Dit.test=RaftSimulationIT -Dkeyforge.sim.seed=N`), and `-Dkeyforge.sim.trace=N` prints the last 400 events with
every node's status. `aSeedReplaysExactly` runs seeds twice and compares a hash of every processed event.

**The checks must bite.** `invariantsCatchDeliberateBugs` runs three mutations and requires each to break an
invariant on some seed: a driver that sends before it persists, a disk that forgets votes, and reads served by a node
that believes it is leader without ReadIndex's confirmation.

**Where it runs.** `RaftSimulationTest` (500 seeds, the determinism check, the mutations) runs in `mvn test`;
`RaftSimulationIT` runs 10,000 seeds in `mvn verify`. Runs are independent, so seeds run in parallel.

## Consequences

- 10,000 seeds take about 15 s on the development machine: 250 million simulated events, about 24,000 elections,
  730,000 acknowledged writes, 490,000 linearizable reads, 15,000 snapshot installations and 160,000 crashes, with no
  violation.
- The simulator has already paid for itself during development. It found a client-model gap (writes whose entries were
  truncated waited forever) and a real liveness bug in replication flow control (ADR 0009), each reproduced from its
  seed and fixed.
- It tests the core, not the runtime, the file storage or gRPC; those have their own tests (`FileRaftStorageTest`,
  `RaftClusterIT`) and the Docker failover check. Its simulated disk is synchronous, so torn writes are covered by the
  storage tests, not here.
- Determinism constrains the core: no `HashMap` iteration that affects outputs, no wall clock, no threads.

## Alternatives considered

- **Jepsen-style testing of real processes** (Phase 5 builds that). It is irreplaceable for the integrated system, but
  slow, and failures are hard to reproduce.
- **Model checking a TLA+ specification.** It is exhaustive for small configurations, but it checks a model, not this
  Java code.
- **Property-based tests with jqwik over event sequences.** Shrinking is attractive, but generating meaningful
  schedules of a distributed system needs the simulator anyway; seeds plus traces proved enough.
