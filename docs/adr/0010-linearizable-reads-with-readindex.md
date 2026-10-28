# ADR 0010: Linearizable reads with ReadIndex, not leader leases

- Status: accepted
- Date: 2026-10-28
- Phase: 4

## Context

A read served from a replica's local state can be stale: a deposed leader that does not yet know it was deposed still
answers, and a follower may lag. The specification requires linearizable reads through ReadIndex and asks this ADR to
explain why leases are not used. Reads must not go through the log either: appending every read would cost an fsync on
a majority.

## Decision

**ReadIndex (Raft thesis §6.4) on the leader.**

1. When a `ReadIndex` event arrives, the leader records its commit index as the read index. If it has not yet
   committed an entry of its own term (its no-op, ADR 0009), it may not know the latest commit index, so the read
   waits until the no-op commits.
2. It then confirms it is still the leader with a heartbeat round. Rounds are numbered. Every AppendEntries carries the
   number of the latest round started, and responses echo it. A response from the current term at round `r`, even a
   rejection, proves that the follower accepted this leader after round `r` began. Once a majority (the leader
   included) has acknowledged a round started after the read arrived, no other leader can have been elected before
   that moment, so nothing committed before the read is missing from the read index.
3. The core emits `ReadReady(readIndex)`. The runtime queues it behind the apply tasks already issued, so the read
   completes once the state machine has applied the read index; the command then reads the local store.

Reads that arrive while a round is in flight share the next round, so the number of heartbeat rounds is bounded by one
per round trip, whatever the read rate. If leadership is lost, pending reads fail with `ReadRejected` and the command is
forwarded to the new leader (ADR 0014). A follower never serves reads itself; it forwards them, like writes.

## Consequences

- Every read costs one heartbeat round trip to a majority, but no disk write and no log entry. Under load, one round
  confirms a whole batch of reads.
- Reads are linearizable with no assumption about clocks. The simulator checks this property on every read: the read
  index must be at least every index applied anywhere when the read began. It also checks that a variant that skips the
  confirmation round (`UNCONFIRMED_LEADER_READS`) breaks this on hundreds of seeds; the check is not vacuous.
- The first read after an election waits for the new leader's no-op to commit.
- Multi-key reads (`MGET`, `EXISTS`) read one consistent LSM snapshot after the barrier, so they stay atomic.
- All reads go through the leader. Follower reads (a follower asks the leader for a read index, then serves locally) are
  a possible optimisation, not implemented.

## Alternatives considered

- **Leader leases** (the leader serves reads locally while its lease, renewed by heartbeats, has not expired). A lease
  is only safe if no other leader can be elected before it expires, and that bound is measured with clocks. It assumes
  bounded clock drift and, worse, bounded pauses: a leader that stops for a GC pause, a VM migration or a page-fault
  storm longer than the lease resumes believing it still holds it and serves stale reads, while the rest of the cluster
  has elected a new leader. KeyForge runs on the JVM, in containers, on hosts like the Docker Desktop VM where the Phase 2
  benchmarks measured 10–20 ms timer jitter. Those are exactly the conditions under which lease assumptions fail
  silently. A stale read that looks valid is worse than a slower correct one. ReadIndex needs no timing assumption for
  safety (only for liveness), at the cost of one network round trip per batch of reads.
- **Reads through the log.** They are simple and linearizable, but every read becomes a log entry: an fsync on a
  majority and log growth for an operation that changes nothing.
- **Local reads on any node** (Redis replica style). They are fast, but neither linearizable nor even monotonic across
  nodes.
