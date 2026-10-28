# LSM engine microbenchmarks (JMH)

Put, get and scan costs of the LSM storage engine (`io.keyforge.storage.lsm.LsmKeyValueStore`), measured in-process
with JMH: no network, no RESP, no server. The engine's design is in [ADR 0006](../adr/0006-lsm-storage-engine.md),
[ADR 0007](../adr/0007-wal-fsync-policies-and-recovery.md) and [ADR 0008](../adr/0008-size-tiered-compaction.md).

## How to run

```bash
./mvnw -q package -DskipTests
java -cp keyforge-bench/target/keyforge-bench-0.1.0-SNAPSHOT-all.jar org.openjdk.jmh.Main 'io.keyforge.bench.jmh.Lsm' \
     -rf json -rff lsm-jmh.json
```

Each benchmark and parameter combination runs in a fresh fork (`-Xms2g -Xmx2g`): 3 warm-up iterations of 2 s, then 5
measured iterations of 2 s. Stores live in a temporary directory with default options (64 MiB memtable, group commit
every 10 ms or 1 MiB) unless the parameters say otherwise. Keys are `key:` plus 12 decimal digits (16 bytes); values
are 100 bytes. `JmhBenchmarksIT` runs every benchmark once, briefly, during `./mvnw verify`, so the benchmarks keep
compiling and running.

| Benchmark | What it measures |
| --- | --- |
| `LsmPutBenchmark.put`, `put16Threads` | Unconditional puts of random keys from 1 or 16 threads, for `fsyncPolicy` = `group-commit` and `always`. Memtables fill, rotate and are flushed, and tables are compacted, during the measurement. |
| `LsmGetBenchmark.getHit`, `getMiss` | Lookups of random existing keys, and of absent keys that fall inside the stored key range, over 1,000,000 keys. `location=memtable` keeps everything in the memtable; `location=sstable` flushes and compacts into one table first. |
| `LsmScanBenchmark.scan` | 100 consecutive keys from a random start over 1,000,000 keys. `layout=compacted`: one table. `layout=layered`: an older table with every key, a newer table rewriting every fourth key, and a memtable rewriting every tenth. |

## Environment

Run on 2026-10-28 on the development machine: Intel Core i3-9100F (4 cores, no SMT, 3.6 GHz), 16 GB RAM, Windows 10
Pro 19045, NTFS on a Kingston A400 SATA SSD (DRAM-less), JDK 25.0.5 (Temurin), JMH 1.37. Raw output:
[results/2026-10-28/jmh](results/2026-10-28/jmh) (`lsm-jmh.*` for the full suite, `lsm-jmh-get.*` for the get
benchmark re-run described below).

## Results

| Benchmark | Parameters | Result |
| --- | --- | --- |
| put, 1 thread | `group-commit` | **29,846 ± 4,134 ops/s** (about 34 µs per put) |
| put, 1 thread | `always` | **1,291 ± 103 ops/s** (about 0.77 ms per put: one fsync each) |
| put, 16 threads | `group-commit` | 74,814 ops/s on average; iterations ranged from 21,695 to 111,996 |
| put, 16 threads | `always` | 4,964 ops/s on average; about 7,000 to 7,500 until compactions started, then about 1,500 to 1,600 |
| getHit | memtable | 1,990 ± 93 ns |
| getHit | sstable | 1,814 ± 48 ns |
| getMiss | memtable | 1,961 ± 103 ns |
| getMiss | sstable | **158 ± 40 ns** |
| scan, 100 keys | compacted | 11.8 ± 2.7 µs |
| scan, 100 keys | layered (3 sources) | 28.8 ± 1.5 µs |

The get rows come from `lsm-jmh-get.json`. In the full-suite run, `getMiss` probed keys beyond the last stored key. A
table rejects those with its min/max key check before the Bloom filter (66 ns), and the skip list finds them at its
right edge (183 ns). The benchmark now uses absent keys inside the range; the full-suite file keeps the old numbers.

## Analysis

**A single writer's put costs a thread hand-off, not an fsync, under group commit.** About 34 µs per put from one thread
is the round trip: the caller enqueues, the writer thread wakes, writes the WAL, publishes, and wakes the caller. Two
thread wake-ups on Windows dominate it; the encoding, the memtable insert and the `write` call are a few microseconds.
With 16 threads, batching amortises the wake-ups and one WAL write serves many puts: 2.5 times the single-thread rate
on average, up to 3.7 times. The spread comes from flushes and compactions sharing four cores with 16 writer threads.

**`always` is bounded by the disk's fsync latency, and batching multiplies it.** One thread waits for one fsync per put
(about 0.77 ms on this SSD). With 16 threads, about 5.5 puts share each fsync (7,000 to 7,500 ops/s), until
compactions start writing large tables. On this DRAM-less SATA SSD, fsync latency then rises several-fold. Measuring
that same effect led to a design change: the first version of `group-commit` ran its periodic fsync on the writer
thread. While a flush was writing, WAL fsyncs took 25 to 340 ms each, and single-thread put throughput fell from about
28,000 to between 200 and 20,000 ops/s. Group-commit fsyncs now run on their own thread (ADR 0007): throughput no longer
moves when a flush starts (28,240 to 31,108 ops/s across all five iterations, including flushes).

**Bloom filters make SSTable misses cheap.** A miss in a table costs 158 ns: one MurmurHash3 of the key and up to seven
bit probes. A hit costs about 1.8 µs: the Bloom check, a binary search over about 30,000 block keys, copying a 4 KiB
block out of the mapping, a CRC32C of the block, and a linear scan of about 30 entries.

**The memtable is the slow path for random lookups.** A skip list of a million entries takes about 2 µs per lookup, hit
or miss. That is about 20 levels of pointer chasing, mostly cache misses. That is slightly slower than an SSTable hit
and twelve times slower than an SSTable miss. With the default 64 MiB memtable, about 275,000 entries of this size
(about 244 bytes each by the engine's estimate), it is somewhat cheaper. If lookups that miss the memtable matter, a
hash index beside the skip list would help.

**Scans pay for every source they merge.** A 100-key scan of one compacted table costs 11.8 µs (about 120 ns per key:
three or four block copies and an entry decode per key). With an older table, a newer table and a memtable to merge,
and shadowed versions to skip, it costs 28.8 µs. That is the read amplification of size-tiered compaction that ADR 0008
anticipates.

## Caveats

- One fork per configuration on a desktop machine; the put error bars are wide because flushes and compactions run
  during the measurement, which is the point of measuring through them.
- Absolute fsync costs are specific to this SSD and NTFS. Under Docker Desktop or a Linux NVMe disk, `always` will look
  very different. The relative effects (batching, slow fsyncs during flushes) should not change.
- These are engine-level numbers. Through the network, a request also pays RESP decoding, the event loop or virtual
  thread, and the round trip, which the Phase 2 benchmark measured at roughly 0.2 ms median under load
  ([netty-vs-virtual-threads.md](netty-vs-virtual-threads.md)).
