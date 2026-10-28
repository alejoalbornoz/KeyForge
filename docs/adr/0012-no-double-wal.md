# ADR 0012: The Raft log is the only write-ahead log; the LSM store records its applied position

- Status: accepted; amends ADR 0006 and ADR 0007 for cluster mode
- Date: 2026-10-28
- Phase: 4

## Context

A standalone node is durable through the LSM engine's WAL (ADR 0007). In a cluster every write is already in the Raft
log, fsynced on a majority, before it is applied. Logging it again in the store's WAL would cost a second write and
fsync per entry for nothing. The specification requires "no double WAL": in cluster mode the Raft log is the source of
durability, and the storage engine persists `lastAppliedIndex` and is rebuilt from snapshot plus log. ADR 0006 already
suggested bypassing the WAL and recording the applied index in the manifest.

## Decision

**Replicated mode of the LSM engine** (`LsmKeyValueStore.Options.replicated`, always on in cluster mode):

- **No WAL.** No WAL file is created, written or replayed. Memtable contents are not durable until flushed.
- **Log positions travel with writes.** The state machine calls `put/putAll/compute/delete/clear(position, ...)` with
  the `(index, log time)` of the entry being applied, and `markApplied(position)` for entries that change nothing (a
  leader's no-op). The writer thread records the position in the active memtable right after the write, in the same
  batch, so the memtable holds exactly the effects of the entries up to its position. This matters for
  non-idempotent writes: if a flush could capture an `INCR`'s effect but not its position, replaying the entry after a
  crash would count it twice. `LsmReplicatedModeTest.aCrashKeepsExactlyTheEntriesUpToTheDurablePosition` checks this
  with counters across arbitrary flush boundaries.
- **The manifest persists the applied position** (format version 2 adds `appliedIndex` and `appliedTime`; version 1 is
  still read). When a flush installs a memtable, the manifest records that memtable's position. Memtables flush oldest
  first, so the position is always an entry boundary below which everything is in SSTables. `FLUSHDB` writes the
  manifest with its own position at once, so the clear can never be replayed onto stale data.
- **Closing flushes**, so a clean restart replays nothing. After a crash, the store comes back at its durable position.

**Startup is "snapshot plus log".** `RaftRuntime.start` asks the state machine for its durable applied index. If that
is behind the latest Raft snapshot (the store was lost, or the log was compacted past it), the store is restored from
the snapshot first. The Raft core then starts with applied and commit index at that position, and the remaining log
entries are applied as the leader confirms them committed. If the store is *ahead* of the Raft log (Raft data deleted,
store kept), startup fails rather than guessing.

**Snapshots** are the state machine's export: every live key with its value and absolute expiry, from one consistent
LSM snapshot, plus the log time (`KeyValueStateMachine.writeSnapshot`). They are taken on the apply thread every
`raft.snapshot-threshold` entries, which is the only time the position is quiescent. Restoring (`LsmKeyValueStore.restore`)
clears the store with position zero, made durable at once; loads the entries in batches; records the snapshot's
position; and flushes. A crash part-way leaves a store at position zero, which the next start restores again.

## Consequences

- Each write is fsynced once, in the Raft log. In the cluster ITs no `.wal` file exists anywhere after hundreds of
  writes and restarts (`ClusterIT.theRaftLogIsTheOnlyWriteAheadLog`).
- After a crash, a node replays from the Raft log whatever its memtables held: at most a few memtables
  (`store.memtable-size-bytes`, 64 MiB by default). The Raft log must keep those entries. Snapshots are taken from the
  applied state and the log is compacted only up to the snapshot, so a store behind the snapshot is restored from it.
- A snapshot is a full export: O(data) time and disk every `raft.snapshot-threshold` entries, during which the apply
  thread is busy (Raft itself keeps heartbeating). The snapshot file doubles the disk footprint of the data.
- The store's `fsync-policy` and group-commit settings do not apply in cluster mode; the Raft log is always fsynced
  before acknowledging (ADR 0009).
- A data directory should not switch between standalone and cluster mode. A standalone store's leftover WAL would be
  replayed once and its keys would count as state at position zero.

## Alternatives considered

- **Keep the store's WAL and apply with `fsync` off.** It is a second write of every byte, and recovery would still need
  the applied index to know where to resume.
- **Persist `lastAppliedIndex` on every apply** (a small file or a key in the store). It needs a write per entry, and
  ordering it with the data writes is exactly the atomicity problem positions in memtables solve.
- **Use the LSM store itself as the Raft snapshot** (hard-link the SSTables at a flushed position, a "checkpoint" as
  RocksDB-based systems do). It is cheaper for large data sets, but needs reference-counted table files shared between
  the store and the snapshot, and a way to stream them. It is worth doing if snapshot cost shows up in benchmarks.
- **Rebuild the store from the full log on every start.** It needs no positions at all, but forbids log compaction.
