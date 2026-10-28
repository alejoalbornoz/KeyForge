# ADR 0007: WAL records, `always` and `group-commit` fsync policies, and crash recovery that truncates only torn tails

- Status: accepted
- Date: 2026-10-28
- Phase: 3

## Context

The specification fixes the record format, `[length:int][crc32c:int][payload]`, and two fsync policies:

- `ALWAYS`;
- `GROUP_COMMIT`, a batched fsync every N ms or N bytes.

On startup the engine must load the manifest and replay the WAL. A CRC mismatch in the last record (a torn write) is
truncated; one in the middle of the log is a fatal error. Tests must kill the process mid-write and check that every
acknowledged write survives. Phase 2 found that `fsync` on a virtual thread holds the carrier (ADR 0005). ADR 0006 puts
all writes on one writer thread.

## Decision

**One WAL file per memtable, written only by the writer thread.** Each write produces one record:

```text
record  = [length:int][crc32c(payload):int][payload]          big-endian
payload = [seq:long][count:varint] entry*
entry   = [type:byte][keyLength:varint][key] ([expireAt:long][valueLength:varint][value] for a put)
```

The writer encodes a batch's records into one buffer and writes them with one `write` call. When the memtable fills,
the writer syncs and closes the WAL, then creates the next one and fsyncs the directory. Only the newest WAL can
therefore end in a torn record. The flush thread deletes a WAL once a manifest that no longer needs it is durable.

**`always`: acknowledge after an fsync that covers the write.** The writer syncs after writing each batch and only then
publishes and completes it. Writes that queue up while an fsync runs form the next batch and share its fsync. That is
natural group commit, with no added delay, as with Redis' `appendfsync always` (one fsync per event-loop iteration) or
LevelDB's `sync=true`. It survives power loss.

**`group-commit` (the default): acknowledge after the write, fsync every `interval` or every `bytes`.** The writer
publishes and acknowledges a batch once `write` returned, so the data is in the operating system's page cache. A
separate thread, `keyforge-lsm-wal-sync`, forces the WAL as soon as `store.wal.group-commit-bytes` (default 1 MiB) are
unsynced, or when `store.wal.group-commit-interval-ms` (default 10 ms) has passed since the last fsync. While the WAL
is clean it parks without a deadline, and the writer wakes it with the first write. A process crash, `kill -9`
included, loses nothing that was acknowledged, because the kernel still holds the data. A power loss or kernel crash
can lose at most the last window, plus the duration of a slow fsync.

The fsync runs off the writer thread because acknowledgements do not depend on it, and because fsyncs get slow exactly
when the disk is busy. The first version synced inline on the writer. The JMH put benchmark then showed WAL fsyncs of
25 to 340 ms each while a memtable flush was writing its SSTable (a few ms otherwise, on the development machine's SATA
SSD), and single-client throughput fell from about 28,000 to between 200 and 20,000 puts per second for as long as the
flush ran. `WalWriter.sync` and `close` are synchronised, so a rotation can sync and close a WAL that the sync thread is
forcing. The writer still syncs inline when it rotates the WAL, so that only the newest WAL can end in a torn record.

**Recovery** (`LsmKeyValueStore.open`):

1. Lock the directory; read the manifest. If there is no manifest, a directory that holds WAL or SSTable files is
   corrupt; an empty one is a new store.
2. Map every table the manifest lists, and check its footer, checksums, size and sequence range against the entry.
3. Replay, in order, every WAL numbered at or above the manifest's minimum into fresh memtables.
4. Flush what was replayed into new tables, then create a new WAL.
5. Write a manifest that lists the new tables and names the new WAL as the minimum.
6. Delete older WALs, tables the manifest does not list (output of interrupted flushes or compactions) and
   `MANIFEST.tmp`.

The sequence counter resumes at the larger of the manifest's last sequence and the highest one replayed. A crash at any
point repeats the work on the next start: nothing is deleted until a manifest that no longer needs it is durable.

**Torn tail or corruption.** A damaged record is a torn tail only if nothing but zero bytes follows it:

- the record or its header is cut short by the end of the file;
- its CRC fails and only zero bytes follow it (file systems may extend a file before writing its data);
- its length is not positive and only zeros follow.

Recovery truncates the newest WAL there and logs a warning. Anything else fails with `CorruptionException`:

- a damaged record followed by more data;
- a torn tail in a WAL that is not the newest;
- a record whose CRC is valid but whose payload does not decode;
- an SSTable with a bad magic number, version or checksum;
- a manifest entry whose table is missing or mismatched.

A record whose length field points past the end of the file cannot be told apart from a cut-short append, so it is
treated as a torn tail.

## Consequences

- Acknowledged writes survive `kill -9` with either policy. Two tests check that by killing processes:
  - `CrashRecoveryIT` kills a child JVM that writes with four threads, three rounds per policy, while memtables rotate
    and tables are flushed and compacted. It checks that each thread's keys equal the state after its last
    acknowledged operation, or after the single one in flight.
  - `KillRecoveryIT` kills the real server, mid-traffic from four Jedis clients, and restarts it.

  `Process.destroyForcibly` is SIGKILL on Linux and TerminateProcess on Windows. Power-loss durability with `always` is
  argued from the fsync ordering; no test pulls the plug.
- Under `group-commit`, acknowledgements never wait for an fsync, except for the one that closes each WAL at rotation.
- `always` costs one fsync per batch. On the Windows development machine that is several milliseconds, about 50
  acknowledged writes per second per thread in the crash test when each thread writes alone. Throughput then rises
  with concurrency, as more writes share each fsync. A Netty event loop that waits for an `always` write is blocked for
  that fsync (ADR 0006).
- Startup time grows with the WAL that was not yet flushed: at most about three memtables. Each restart flushes that
  WAL into a new table, which compaction later absorbs.
- A corrupted length field that happens to point past the end of the file truncates the log silently. Detecting that
  case would need a block-structured log with resynchronisation points.
- The tests that pin the rules down are `WalTest` (round trips, torn headers and records, zero-filled tails, CRC
  failures in the middle, older WALs) and `WalTornTailPropertyTest` (cutting a log at every possible byte recovers
  exactly the complete records before the cut).

## Alternatives considered

- **Fsync on the connection thread for each write.** It blocks event loops and carriers, and gives up batching.
- **Group-commit fsyncs on the writer thread.** It was simpler, and it was the first version. It stalled every write
  behind each slow fsync, as described above.
- **`group-commit` that holds every acknowledgement until a timer-driven fsync** (a classic commit delay). Every write,
  even a lone one, would wait for the window. `always` already batches concurrent writers without that floor, so the
  only difference would be fewer fsyncs per second.
- **LevelDB's log of fixed 32 KiB blocks.** It can resynchronise past a damaged record and tell a corrupted length from
  a torn tail. That is more format and code than the specified record framing needs.
- **Skipping damaged records in the middle.** Recovery would silently lose acknowledged writes, or apply later ones
  without earlier ones.
- **One long-lived WAL with checkpoints instead of one file per memtable.** Reclaiming space would need log truncation
  at checkpoints rather than deleting whole files.
