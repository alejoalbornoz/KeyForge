# ADR 0009: Raft as a pure state machine, driven by a two-thread runtime

- Status: accepted
- Date: 2026-10-28
- Phase: 4

## Context

Phase 4 replicates the store with Raft, written from scratch. The specification asks for a core that is a pure,
single-threaded state machine, `RaftNode.handle(event) -> List<Action>`, with no I/O, no real clock and no threads
inside. Events are messages, timer ticks and client requests; actions are send, persist, apply and reply. The clock and
the transport are injected. It must implement leader election with randomized timeouts, PreVote, log replication with
the consistency check, the rule that a leader only commits entries of its own term (figure 8 of the Raft paper),
persistence of the term, vote and log before responding, snapshots with InstallSnapshot, and log compaction.
Membership changes are out of scope.

## Decision

**The core (`io.keyforge.raft.RaftNode`).** One method, `handle(RaftEvent)`, returns the `RaftAction`s to carry out, in
order. Events are `Tick`, the six `RaftMessage` records (vote request and response, AppendEntries and response,
InstallSnapshot and response), `Propose`, `ReadIndex` and `SnapshotTaken`. Time comes from an injected `Clock`
(monotonic milliseconds) and randomness from an injected `RandomGenerator`, so equal inputs give equal outputs. The core
holds its log in memory (`RaftLog`: the snapshot it starts after, and the entries since), which bounds the log by the
snapshot threshold.

**Ordering contract.** Every `Persist` action (`PersistState`, `AppendLog`, `TruncateLog`, `CompactLog`,
`RestoreSnapshot`) must be durable before any action that follows it runs. A vote is emitted after the
`PersistState` that records it, an AppendEntries success after the `AppendLog` that stores the entries. The core never
needs to know whether the driver batches.

**Elections.** Randomized timeouts in [min, max] (default 1–2 s, heartbeat 100 ms). With **PreVote** (Raft thesis
§9.6) a node whose timer fires first asks for pre-votes for `term + 1` without changing its term. Voters grant one only
if the candidate's log is up to date and they have not heard from a leader within the minimum timeout. Only with a
majority of pre-votes does the node increment its term and run a real election. A partitioned node therefore never
inflates its term and never disrupts a healthy cluster when it returns. **CheckQuorum** adds leader stickiness:
followers that heard from a leader recently ignore higher-term vote requests, and a leader that has not heard from a
majority within the minimum election timeout steps down. Clients of a partitioned leader are then redirected instead of
waiting.

**Replication.** A follower checks `prevLogIndex/prevLogTerm`. On a mismatch it answers with a hint: the start of the
conflicting term, or the end of its log. It truncates a conflicting suffix (never a committed entry; the core throws if
asked to) and skips entries it already has, so duplicated and reordered messages are harmless. It advances its commit
index only up to the last entry verified against the leader, never to the end of a possibly stale log. The leader tracks
each follower like etcd's progress tracker. While the follower's position is unknown (a new leader, a rejection, a lost
message) it sends one batch at a time. Once the follower acknowledges a batch, up to `maxInflightAppends` (8) batches
are pipelined. Only acknowledgements of real batches free the window. A first version cleared an "in flight" flag on
every response: the simulator showed duplicated responses and heartbeat answers multiplying the traffic into a message
storm (seed 6213), and this design replaced it. A batch unanswered for two heartbeat intervals is presumed lost and
replication restarts from the match index.

**Figure 8.** A new leader appends a no-op entry of its term at once. It commits index N only if a majority stores N
**and** the entry at N has the leader's current term; entries of earlier terms commit indirectly beneath it.
`RaftNodeTest.aLeaderNeverCommitsAnEarlierTermsEntryByCountingReplicas` builds the figure-8 situation explicitly.

**Snapshots.** The driver writes a snapshot every `raft.snapshot-threshold` applied entries (default 100,000) and
reports it with `SnapshotTaken`; the core then emits `CompactLog`. A follower whose next entry was compacted away gets
`SendSnapshot`. The core knows snapshots only by index and term; the driver holds the bytes, and the transport streams
them in chunks (ADR 0014). A follower that receives one either acknowledges it (it already holds the snapshot's last
entry, or has committed past it) or discards its log and emits `RestoreSnapshot`.

**The runtime (`io.keyforge.raft.runtime.RaftRuntime`).** Two platform threads:

- `keyforge-raft` owns the core. It drains up to 1,024 queued events and adds a tick every 10 ms. It then executes the
  actions in two phases: every persistence action followed by **one fsync**, then the rest in order (sends, applies,
  replies). Sends only ever move later, so the ordering contract holds, and one fsync covers a whole batch of
  proposals and appends: group commit for the Raft log.
- `keyforge-raft-apply` applies committed entries to the `StateMachine`, completes proposal futures by
  `(index, term)`, serves reads (ADR 0010) and takes snapshots. A slow state machine never delays heartbeats.

A proposal's future fails with `NotLeaderException` (retry-safe) if the node is not the leader or if its entry was
overwritten by a later leader. It fails with `OutcomeUnknownException` if the outcome cannot be known (the node was
stopped, or a leader's snapshot replaced the entry's position).

**Durable storage (`io.keyforge.raft.storage.FileRaftStorage`).** In `raft.data-dir`:

- `state`: term and vote, written to a temporary file, forced and renamed;
- `log-<first index>.seg`: CRC32C-framed records, 64 MiB segments;
- `snapshot-<index>-<term>.snap`: header, body and CRC32C; only the newest is kept;
- `LOCK`: an OS file lock.

Recovery follows ADR 0007: a torn record at the very end of the newest segment is truncated; damage anywhere else is a
`StorageCorruptionException`. Truncation deletes later segments before cutting the file, and installing a snapshot
writes it before resetting the log. A crash in between leaves a state that recovery completes (it discards a log that
does not continue the snapshot).

## Consequences

- The protocol logic is testable without threads, sockets or sleeps. `RaftNodeTest` (26 scenarios) drives it event by
  event, and the deterministic simulator (ADR 0013) runs it through 10,000 hostile schedules in about 15 s.
- The core allocates a fresh action list per event and copies entry slices into immutable lists. Under load the
  dominant costs are the fsync and the network; this has not been measured as a bottleneck.
- The log since the last snapshot lives on the heap: about 100,000 entries with the default threshold.
- Every acknowledged write costs one Raft-log fsync on a majority (batched across concurrent writes) and a network round
  trip. That is the price of surviving power loss on a majority. The LSM engine's own WAL is gone in cluster mode
  (ADR 0012), so there is still one log, not two.
- A leader keeps the term-of-office no-op even when idle, so the first read of a new term waits for it to commit
  (ADR 0010).
- Membership is fixed at startup (`cluster.members`); adding or removing nodes needs joint consensus or single-server
  changes in a later phase.

## Alternatives considered

- **A threaded, I/O-performing Raft implementation** (timers, sockets and locks inside the protocol object). It is
  simpler at first sight, but untestable deterministically: interleavings can only be explored by running real
  threads, and failures do not reproduce.
- **Logical ticks instead of an injected clock** (etcd counts ticks). It works as well; a clock in milliseconds keeps
  the configuration in time units and lets the simulator give each node a skewed clock rate.
- **Stop-and-wait replication only.** It is simple and robust, but needs one round trip per batch. With fsync on the
  follower that limits a single connection's throughput, while gRPC keeps messages in order, which makes pipelining
  cheap.
- **Fsync inside the core's actions one by one.** It is correct, but each vote, append and truncation would sync on its
  own. Batching the persistence of a whole drained batch is the standard Ready loop of etcd/raft.
- **Snapshot bytes inside the core.** The core would hold a copy of the state machine. Keeping only the index and term
  and letting the driver stream files keeps memory flat.
