# ADR 0006: An LSM engine with one writer thread, sequence-number snapshots and memory-mapped SSTables

- Status: accepted
- Date: 2026-10-28
- Phase: 3

## Context

Phase 3 replaces the in-memory hash table with a durable log-structured merge tree, written from scratch: a write-ahead
log, a concurrent memtable (`ConcurrentSkipListMap`), immutable SSTables (about 4 KB data blocks, a sparse index, a
Bloom filter at about 1% false positives, a footer with a magic number and a format version), a manifest of live tables
that is replaced atomically, tombstones, and size-tiered compaction. The engine sits behind the existing
`KeyValueStore` interface, whose contract stays as ADR 0002 and ADR 0003 set it:

- every method is atomic and linearizable, multi-key ones included: a concurrent `getAll` never sees half of a
  `putAll`;
- `compute` is the read-modify-write primitive (`INCR`), and conditional writes (`SET NX/XX`) and delete counts depend
  on the current value;
- the store owns the arrays it receives and callers do not modify the ones it returns, so neither side copies;
- expiry is an absolute epoch-millisecond instant.

Commands run on the thread that decoded them: a Netty event loop, or a virtual thread with `server.type=virtual-threads`.
ADR 0005 noted that blocking *file* I/O such as `fsync` does not unmount a virtual thread; the JDK compensates by adding
carrier threads. An `fsync` per write on connection threads would block event loops and inflate the carrier pool.

The WAL format, fsync policies and recovery are in ADR 0007, compaction in ADR 0008.

## Decision

**All writes go through one dedicated platform thread, the writer** (`keyforge-lsm-writer`). `put`, `putAll`,
`compute`, `delete` and `clear` enqueue a request, and the caller parks on a future until it completes. On a virtual
thread, parking releases the carrier. The writer takes up to 1,024 queued requests as a batch. For each one it:

1. evaluates it against the latest state, including earlier writes of the same batch: the `NX`/`XX` condition, the
   `compute` function, which keys a `DEL` actually removes;
2. assigns one sequence number to the whole write, so a multi-key write is one atomic unit;
3. appends one WAL record to an in-memory buffer and inserts the entries into the active memtable.

Then it commits the batch: one `write` call for all records, an `fsync` under the `always` policy, publication of the
batch's last sequence number, and completion of the callers' futures. Under `group-commit`, a separate WAL sync thread
does the fsyncs (ADR 0007). A request whose evaluation throws (a failing
`compute` function) fails alone, but its reply also waits for the commit, since it may depend on earlier writes in the
batch. All writes are totally ordered and every one is atomic, without a single lock on the request path, and file I/O
never runs on a connection thread.

**Reads take no lock and see a consistent snapshot.** The memtable keeps every version of a key, ordered by key and then
by descending sequence number. A read takes a reference to the current `Version` (below), then reads the published
sequence number, and ignores memtable entries above it: those belong to a batch the writer has inserted but not yet
committed. SSTables need no filtering, because a memtable is flushed only after all its writes are published. A
`getAll`, `countExisting`, `scan` or `size` uses one snapshot for every key it touches, which makes it linearizable: it
observes exactly the writes published before the moment it read the sequence number. The order matters: taking the
version first guarantees that any table in it holds only published data.

**Versions and reference counts.** A `Version` is an immutable list of the active memtable, the immutable memtables
waiting for a flush (at most 2; beyond that, writes stall until the flush thread catches up), and the SSTables, newest
first. Readers take and drop a reference with a compare-and-set. Each SSTable is counted too: whoever opened it holds
one reference and every version that lists it holds one. When a compaction or `clear` makes a table obsolete, the last
release unmaps it and deletes the file.

**SSTables are memory-mapped** with the FFM API (`FileChannel.map` into a shared `Arena`). Reading a block is a copy out
of the page cache, not a system call, so it never makes the JDK compensate for a blocked carrier. The sparse index (the
last key of each data block), the Bloom filter and the table statistics are decoded onto the heap at open. Each block
carries a CRC32C, checked on every read.

```text
file   = data-block* index-block filter-block meta-block footer          (every block: content + crc32c)
entry  = [type][keyLength:varint][key][seq:long] ([expireAt:long][valueLength:varint][value] for a put)
footer = index, filter and meta offsets and lengths, their crc32c, [formatVersion:int][magic:long "KEYFORGE"]
```

Every entry keeps its sequence number, so merges resolve versions by sequence number, whatever the files' ages. The
Bloom filter hashes keys with MurmurHash3 (x64, 128 bits) and probes 7 bits out of 10 per key (Kirsch-Mitzenmacher
double hashing), about 0.8% false positives. Keys compare as unsigned bytes everywhere.

**The data directory.** `LOCK` (an OS file lock, so that a second process cannot open the same directory), `MANIFEST`,
`NNNNNN.wal` and `NNNNNN.sst`, numbered from one shared counter. The manifest lists the live tables (newest first,
with sizes and sequence ranges) plus the lowest WAL still needed, the next file number and the last sequence number. A
new manifest is written to `MANIFEST.tmp`, fsynced, renamed over the old one with `ATOMIC_MOVE`, and the directory is
fsynced. Windows cannot open a directory to fsync it; NTFS journals the rename.

**Flushes and compactions run on their own threads** (`keyforge-lsm-flush`, `keyforge-lsm-compaction`), so a long
compaction never delays a flush. When the active memtable passes `store.memtable-size-bytes`, the writer syncs and
closes its WAL, opens a new WAL and memtable, and hands the old memtable to the flush thread. That thread writes it
(newest version of each key) into a table, installs a new version, rewrites the manifest and deletes the old WAL.
Installation is serialised by one lock that readers never take. `clear()` (`FLUSHDB`) switches to an empty memtable and
WAL and an empty manifest, and bumps an epoch, so a flush or compaction already in flight discards its output.

**Deletes and expiry.** A delete writes a tombstone. Reads treat an expired value as absent. Flushes and compactions
turn expired values into tombstones (they still shadow older versions), and drop tombstones and expired values only when
nothing older can exist: when flushing into an empty table list, or when a compaction includes the oldest table
(ADR 0008). `size()` (`DBSIZE`) merges every source and counts live keys.

**Ordered scans.** `LsmKeyValueStore.scan(from, to, limit)` merges the memtables and tables of one snapshot. It is not
part of `KeyValueStore`, because the in-memory engine is a hash table; the JMH benchmarks and the tests use it.

**Engine selection.** `store.engine=lsm` (the default) or `memory`. `store.data-dir`, `store.wal.*` and
`store.memtable-size-bytes` configure the LSM engine; `store.segments` and `store.active-expire-interval-ms` still
configure the in-memory one. `CONFIG GET appendonly` answers `yes` for the LSM engine.

## Consequences

- Multi-key atomicity, read-modify-write and conditional writes need no locks at all, and the model and concurrency
  tests check them against a reference model and under concurrent load, including torn-read detection for `getAll` and
  `scan` while memtables rotate and tables merge underneath.
- Writes are serialised on one core. Each write also costs a hand-off to the writer and back, tens of microseconds when
  the writer is idle. Batching amortises the hand-off under load. The JMH put benchmark measures both
  (`docs/benchmarks/lsm-engine.md`).
- A caller blocks until its write commits. On a virtual thread that only parks the thread. On a Netty event loop it
  blocks every connection of that loop for the duration: a hand-off with group commit, a whole fsync with `always`.
  Avoiding that needs an asynchronous store API and out-of-order completion in both servers. It is an open issue.
- The `compute` function runs on the writer thread: a slow function delays every write, and a function that calls back
  into the store deadlocks. The interface already forbade both.
- Reads scale with cores. The cost is a compare-and-set on the version's counter per read, the one shared write on the
  read path. Measure before optimising, for example with RocksDB-style thread-local version caching.
- The memtable keeps superseded versions. A hot `INCR` counter fills it faster than distinct keys would, and only a
  flush collapses the versions.
- `DBSIZE` is O(number of entries) rather than O(1), and does not count expired keys, which the interface allows. It
  runs on the calling thread.
- There is no block cache: the OS page cache does that job through the mapping. A table read copies one 4 KiB block
  onto the heap.
- Forward constraints for Phase 4: the writer is the natural Raft apply thread. "No double WAL" can be met by
  bypassing the WAL in cluster mode and recording `lastAppliedIndex` in the manifest, which already records sequence
  numbers. Expiry is applied during flushes and compactions with the local clock. That only removes values every
  replica already reads as absent, but physical removal is node-local, as ADR 0003 left open.

## Alternatives considered

- **Striped key locks with concurrent memtable inserts and ordered publication** (closer to RocksDB's concurrent
  memtable writes). Inserts would run on many cores. But `compute` and conditional writes need the key to stay locked
  until the write is published, which means holding locks across the WAL wait, or else tracking pending versions per
  key. And each caller would still wait on the shared WAL. It is more machinery for a gain the benchmarks have not
  shown to be needed.
- **LevelDB-style leader/follower group commit without a dedicated thread.** The first queued writer performs the batch
  for everyone. That puts the `write` and `fsync` on a connection thread, which is exactly what ADR 0005 warns against.
- **Keeping only the newest version per key in the memtable.** Readers would then see writes that are not yet durable,
  or half of a multi-key batch.
- **One global read-write lock.** ADR 0002 rejected it: a shared reader counter, and non-linearizable multi-key reads
  unless they take the write lock.
- **Positional `FileChannel` reads instead of mapping.** Unmapping would be simpler, but every block read would be a
  system call, and on virtual threads a compensated blocking operation. Windows refuses to delete a mapped file, which
  is why tables are reference-counted.
- **An exact `DBSIZE` counter.** Every put would first have to look the key up, even in SSTables, to learn whether the
  key is new.
- **Adding `scan` to `KeyValueStore`.** The in-memory engine cannot scan in order without sorting. No command needs a
  scan yet.
