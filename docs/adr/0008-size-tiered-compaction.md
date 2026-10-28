# ADR 0008: Size-tiered compaction of age-adjacent tables, rather than leveled compaction

- Status: accepted
- Date: 2026-10-28
- Phase: 3

## Context

Flushes keep adding SSTables. Without merging them, every lookup that misses the memtable would probe a growing number
of tables, and overwritten values and tombstones would never be reclaimed. The specification asks for size-tiered
compaction first, and for this ADR to compare it with leveled compaction on read, write and space amplification.

Notation:

- N: the data size;
- M: the flushed memtable size;
- T: the growth factor between tiers or levels. For size-tiered compaction, T is the number of similar tables merged at
  once (the minimum threshold, 4 here); for leveled compaction it is the size ratio between levels (10 in LevelDB and
  RocksDB);
- L: the number of tiers or levels, about log_T(N / M).

As an example, take N = 64 GiB and M = 64 MiB. Then N / M = 1,024, L = 5 tiers for size-tiered compaction (T = 4) and
L = 3 levels for leveled compaction (T = 10).

| | Size-tiered | Leveled |
| --- | --- | --- |
| **Write amplification**: bytes compaction writes per byte flushed | Each byte is rewritten once per tier it moves through: about L (5 in the example). | Moving data into level i+1 rewrites the overlapping part of level i+1, up to T times the moved data: about T × L / 2 to T × L (15 to 30). |
| **Read amplification**, point lookup: tables that may hold the key | Up to T - 1 tables per tier: about (T - 1) × L (15). Bloom filters at about 1% cut that to about one block read for a hit, plus some filter checks. | One table per level plus the level-0 files: about L + a few (3 to 7). Bloom filters as well. |
| **Read amplification**, range scan: sorted runs merged | Every table, about (T - 1) × L (15). Bloom filters do not help a scan. | About L plus the level-0 files. |
| **Space amplification**: disk bytes per live byte | Old versions survive until their tier merges with the newer one. Worst case about 2× when the two largest tiers hold the same keys. Merging the largest tables needs as much free space again while it runs. | About 1 + 1 / T (1.1×): at most about a tenth of the data sits above the last level, and compactions work on small key ranges. |

Leveled compaction wins on read and space amplification and pays for it with roughly 3 to 6 times more compaction
writes. Size-tiered compaction is the reverse. KeyForge's workload is Redis-like: write-heavy, dominated by point reads
and writes, with many overwrites (`INCR`, `SET` of the same keys), and with no command that scans. Bloom filters remove
most of size-tiered compaction's point-read penalty.

## Decision

**Size-tiered compaction, restricted to runs of tables that are adjacent in age** (`SizeTieredCompactionPicker`, close
to RocksDB's universal compaction):

- Tables are kept newest first. A candidate run is a stretch of adjacent tables whose sizes all lie within 0.5 to 1.5
  times the run's average size. Tables below 16 MiB all count as one tier (Cassandra's `min_sstable_size` idea), so
  small flushes and post-restart tables merge early.
- A run needs at least 4 tables (`compactionMinThreshold`) and is cut at 32. Among qualifying runs, the one with the
  smallest average size wins: it is the cheapest merge and removes the most tables per byte written.
- A merge replaces the run in place with one table that holds the newest version of every key. One compaction runs at a
  time, on its own thread, after every flush and for as long as some run qualifies.

**Why adjacent runs only.** Merging only adjacent tables keeps every table's sequence-number range disjoint from the
others' and ordered by age. So a point lookup can stop at the first table that holds the key (ADR 0006's `Version.find`).
And "nothing older exists" is a positional fact: the merge includes the oldest table. Cassandra's size-tiered
compaction merges any similar-sized tables, and then has to consult every overlapping table, by per-cell timestamps,
before it can drop a tombstone.

**Tombstones and expired values are dropped only when that is safe**, that is, when the run includes the oldest table
(or a flush writes into an empty store). Otherwise an expired value is rewritten as a tombstone, which must stay: it
still shadows any older version below it.

**Manual major compaction.** `LsmKeyValueStore.compact()` flushes and merges every table into one, dropping all
tombstones and expired values. The tests and benchmarks use it; no command exposes it yet.

## Consequences

- Compaction writes little, which suits a single compaction thread and slow disks.
- Point reads stay at about one block read, but every lookup that misses checks one Bloom filter per table, up to about
  15 in the example. Scans merge every table (`LsmScanBenchmark` compares a compacted and a layered store).
- Tombstones and overwritten values in the oldest, largest table survive until a run reaches it, which happens about
  every T times its size of new data. A delete-heavy workload can hold a lot of dead data meanwhile. Cassandra mitigates
  this with single-table compactions triggered by the share of droppable tombstones; that is an open item here.
- A merge of the largest tables needs free space equal to its inputs, and its output is a single file of that size.
- Compaction I/O is not rate-limited, and a long compaction does not delay flushes (separate threads), but it does
  compete with them for the disk.
- Switching to leveled compaction later would not change the file format: tables already carry per-entry sequence
  numbers and key ranges.

## Alternatives considered

- **Leveled compaction** (LevelDB, RocksDB's default). It has better read and space amplification, but 3 to 6 times
  more compaction writes for this workload. It also needs key-range partitioning, splitting output into many small
  files, and choosing overlapping files per level: more code for advantages that matter mostly for scans and large data
  sets.
- **Cassandra-style size-tiered compaction over non-adjacent tables.** Buckets form more freely, but every point lookup
  would have to consult every table and keep the highest sequence number, and dropping a tombstone would need per-key
  checks against every overlapping table.
- **Time-window compaction**, which buckets tables by write time and drops whole windows once they expire. It is ideal
  for append-only data with TTLs, which is not the general workload here.
- **Tiered upper levels with a leveled last level** (RocksDB's hybrid options). It is a later optimisation if space
  amplification becomes the problem.
