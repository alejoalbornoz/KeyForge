package io.keyforge.storage.lsm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An immutable snapshot of the store's structure: the active memtable, the immutable memtables waiting to be flushed
 * (newest first) and the SSTables (newest first). Sequence-number ranges never overlap and grow from the oldest table to
 * the active memtable, so the first component that holds a key holds its newest version.
 *
 * <p>A version is reference-counted. The store's current version holds one reference; every read takes one for its
 * duration ({@link #tryRetain}) so that a compaction finishing meanwhile cannot unmap a table under it. A version holds a
 * reference to each of its tables and drops them when its own count reaches zero.
 */
final class Version {

    final Memtable active;
    final List<Memtable> immutables;
    final List<SSTable> tables;
    private final AtomicInteger refs = new AtomicInteger(1);

    /** The tables must be referenced by the caller (through the current version or directly) while this runs. */
    Version(Memtable active, List<Memtable> immutables, List<SSTable> tables) {
        this.active = active;
        this.immutables = List.copyOf(immutables);
        this.tables = List.copyOf(tables);
        this.tables.forEach(SSTable::retain);
    }

    /** Takes a reference unless the version has already been released. */
    boolean tryRetain() {
        while (true) {
            int count = refs.get();
            if (count == 0) {
                return false;
            }
            if (refs.compareAndSet(count, count + 1)) {
                return true;
            }
        }
    }

    void release() {
        int remaining = refs.decrementAndGet();
        if (remaining == 0) {
            tables.forEach(SSTable::release);
        } else if (remaining < 0) {
            throw new IllegalStateException("Version released too often");
        }
    }

    /**
     * The newest version of {@code key} visible at {@code snapshot}, which may be a tombstone or expired, or
     * {@code null}. Tables are not filtered by snapshot: everything in them was published before this version was
     * installed.
     */
    InternalEntry find(byte[] key, long snapshot) {
        InternalEntry entry = active.get(key, snapshot);
        if (entry != null) {
            return entry;
        }
        for (Memtable memtable : immutables) {
            entry = memtable.get(key, snapshot);
            if (entry != null) {
                return entry;
            }
        }
        for (SSTable table : tables) {
            entry = table.get(key);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    /** The newest version of every key in {@code [from, to)} visible at {@code snapshot}, tombstones included. */
    Iterator<InternalEntry> iterator(byte[] from, byte[] to, long snapshot) {
        List<Iterator<InternalEntry>> sources = new ArrayList<>(1 + immutables.size() + tables.size());
        sources.add(active.iterator(from, to, snapshot));
        for (Memtable memtable : immutables) {
            sources.add(memtable.iterator(from, to, snapshot));
        }
        for (SSTable table : tables) {
            sources.add(table.iterator(from, to));
        }
        return new MergingIterator(sources);
    }

    /** The WAL files still needed: those of the active and immutable memtables. Returns the smallest number. */
    long minWalNumber() {
        long min = active.walNumber();
        for (Memtable memtable : immutables) {
            min = Math.min(min, memtable.walNumber());
        }
        return min;
    }
}
