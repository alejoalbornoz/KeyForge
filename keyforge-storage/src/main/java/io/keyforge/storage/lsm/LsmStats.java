package io.keyforge.storage.lsm;

/**
 * What the LSM engine's background work has done since the store was opened, and the current shape of the tree. A
 * snapshot from {@link LsmKeyValueStore#stats()}; metrics read it at scrape time.
 *
 * @param flushes               memtables written out as SSTables
 * @param flushedBytes          bytes of the SSTables those flushes wrote
 * @param compactions           compactions installed
 * @param compactionInputBytes  bytes of the tables compactions read
 * @param compactionOutputBytes bytes of the tables compactions wrote
 * @param compactionNanos       time spent compacting
 * @param tables                SSTables in the current version
 * @param tableBytes            their total size on disk
 * @param memtableBytes         approximate bytes in the active and the immutable memtables
 * @param immutableMemtables    memtables waiting to be flushed
 */
public record LsmStats(
        long flushes,
        long flushedBytes,
        long compactions,
        long compactionInputBytes,
        long compactionOutputBytes,
        long compactionNanos,
        int tables,
        long tableBytes,
        long memtableBytes,
        int immutableMemtables) {
}
