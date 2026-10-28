# Architecture Decision Records

One file per decision: context, decision, consequences, alternatives considered.

| ADR | Decision | Phase |
| --- | --- | --- |
| [0001](0001-resp-codec.md) | A transport-agnostic, incremental RESP2 codec | 1 |
| [0002](0002-threading-model-and-in-memory-store.md) | Run commands on I/O threads against a segmented, lock-based store | 1 |
| [0003](0003-key-expiration.md) | Absolute expiry instants, lazy expiry and sampled active expiry | 1 |
| [0004](0004-flushing-and-backpressure.md) | Flush once per read, and stop reading while the client is not reading | 1 |
| [0005](0005-virtual-thread-server-and-pinning.md) | Serve each connection on its own virtual thread with blocking I/O; rely on JEP 491 for monitors | 2 |
| [0006](0006-lsm-storage-engine.md) | An LSM engine with one writer thread, sequence-number snapshots and memory-mapped SSTables | 3 |
| [0007](0007-wal-fsync-policies-and-recovery.md) | WAL records, `always` and `group-commit` fsync policies, and crash recovery that truncates only torn tails | 3 |
| [0008](0008-size-tiered-compaction.md) | Size-tiered compaction of age-adjacent tables, rather than leveled compaction | 3 |
