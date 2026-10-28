# ADR 0002: Run commands on I/O threads against a segmented, lock-based store

- Status: accepted; for the LSM engine, the store's internals are superseded by [ADR 0006](0006-lsm-storage-engine.md),
  which serialises writes on one writer thread and lets reads run lock-free on snapshots. The `KeyValueStore` contract
  and the command-execution model below are unchanged.
- Date: 2026-10-27
- Phase: 1

## Context

Redis executes all commands on one thread, which makes every command, multi-key ones included, trivially atomic.
KeyForge serves connections from several Netty event-loop threads now, and from virtual threads in Phase 2. Later phases
add an LSM engine with a concurrent memtable and group-commit WAL (Phase 3), and Raft (Phase 4), whose single apply
thread writes while ReadIndex reads run concurrently.

We need:

- atomic single-key commands (`INCR` must not lose updates);
- atomic multi-key commands: a concurrent `MGET` must never see half of an `MSET`;
- throughput that scales with cores;
- a storage interface the LSM engine can implement later without changing the command layer.

## Decision

**Commands run synchronously on the thread that decoded them.** `CommandDispatcher` is transport-agnostic and
thread-safe. Netty calls it on the connection's event loop; the virtual-thread server will call it on the connection's
virtual thread. Nothing is handed off between threads, so no queue sits on the hot path.

**`KeyValueStore` is the only interface the command layer uses.**

- Keys and values are `byte[]`. The store takes ownership of what it receives, and callers must not mutate what it
  returns, so neither side makes defensive copies.
- Expiry is an absolute epoch-millisecond instant (see ADR 0003).
- Read-modify-write is a generic `compute(key, function)`. `INCR`/`DECR` parse and format integers in the command
  layer, so the storage layer knows nothing about Redis' integer semantics. The function runs while the key is locked;
  if it throws, nothing changes.
- Every method is atomic and linearizable, including multi-key ones. That is a contract the LSM engine must meet too,
  for example with a single sequence number per write batch.

**`InMemoryKeyValueStore` is a segmented hash table.** Keys hash (Arrays.hashCode, then a multiplicative mix, high
bits) to one of 256 segments by default. Each segment is a plain `HashMap` guarded by its own `ReentrantLock`.

- Single-key operations lock one segment, so different connections rarely contend.
- Multi-key operations (`MGET`, `MSET`, multi-key `DEL`/`EXISTS`) sort the distinct segment indexes and lock them in
  ascending order. That makes them atomic, and the global lock order rules out deadlock between them.
- `DBSIZE` and `FLUSHDB` lock every segment in the same ascending order.

`ReentrantLock` rather than `synchronized`: a multi-key operation holds a variable number of locks, which nested
`synchronized` blocks cannot express. Since JDK 24 (JEP 491) `synchronized` no longer pins virtual threads, so this
choice is about expressiveness, not pinning. Phase 2's ADR covers pinning in detail.

## Consequences

- Throughput scales across event loops. The cost is one uncontended lock/unlock (two CAS operations) per key.
- `MSET`/`MGET` are as atomic as in Redis. A concurrency test runs one writer doing `putAll` of 64 keys spread over
  many segments against four readers doing `getAll`; it asserts zero torn reads.
- `DBSIZE` and `FLUSHDB` briefly stop the world for that store. Both are rare administrative commands.
- Command execution time adds directly to the latency of the other connections on the same event loop. That is fine for
  O(1) and O(keys) commands. A future O(N) command (`KEYS`, `SCAN` over a large range) would have to run in slices or
  off the event loop.
- `compute`'s function must be short and must not call back into the store (the lock is not reentrant across
  segments).

## Alternatives considered

- **A single command-executor thread (the Redis model).** Atomicity would be free, but every request would hop from
  the I/O thread to the executor and every reply would hop back, and throughput would be capped at one core. It also
  fights Phase 3's design (concurrent memtable, group commit amortising fsync across concurrent writers).
- **`ConcurrentHashMap`.** Per-key atomicity comes for free (`compute`), but there is no way to make `MSET`/`MGET`
  atomic, and no O(1) random sampling for active expiry (ADR 0003).
- **One global `ReadWriteLock`** (single-key commands take the read lock, multi-key commands the write lock). Every
  command would hit one shared reader counter, a cache line contended by every core. If `MGET` took only the read lock
  it would not be linearizable: it could read `k2` before a `SET k2` and `k1` after a later `SET k1`, and no single
  instant explains that result.
