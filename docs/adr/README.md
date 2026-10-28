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
| [0009](0009-raft-as-a-pure-state-machine.md) | Raft as a pure state machine (events in, actions out), driven by a two-thread runtime with batched fsync | 4 |
| [0010](0010-linearizable-reads-with-readindex.md) | Linearizable reads with ReadIndex, not leader leases | 4 |
| [0011](0011-deterministic-expiry-under-raft.md) | Leader-assigned expiry, log time, and expiry decisions every replica agrees on | 4 |
| [0012](0012-no-double-wal.md) | The Raft log is the only write-ahead log; the LSM store records its applied position | 4 |
| [0013](0013-deterministic-simulation-testing.md) | Deterministic simulation testing of Raft | 4 |
| [0014](0014-grpc-transport-and-client-forwarding.md) | gRPC transport with a bidirectional stream per node pair, and forwarding to the leader | 4 |
