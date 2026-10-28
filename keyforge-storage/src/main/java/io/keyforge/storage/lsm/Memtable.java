package io.keyforge.storage.lsm;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The in-memory sorted buffer in front of the SSTables: a {@link ConcurrentSkipListMap} from (key, sequence number) to
 * value, ordered by key and then by <em>descending</em> sequence number, so the newest version of a key comes first.
 *
 * <p>Every version is kept, not just the newest. Readers see only writes up to the store's published sequence number
 * (see {@link LsmKeyValueStore}); the writer inserts a batch's entries before publishing it, so a reader must be able
 * to find the previous version while newer, unpublished ones sit in front of it. A flush writes only the newest version
 * of each key.
 *
 * <p>One thread inserts (the store's writer thread, or the recovering thread); any number read concurrently.
 */
final class Memtable {

    /**
     * Rough per-entry heap cost on top of the key and value bytes: skip-list node and index nodes, the key and value
     * objects, and two array headers.
     */
    static final int ENTRY_OVERHEAD_BYTES = 128;

    private final ConcurrentSkipListMap<VersionedKey, Slot> entries = new ConcurrentSkipListMap<>();
    private final long walNumber;
    /** Written by the single inserting thread, read by others. */
    private volatile long approximateBytes;
    private volatile int entryCount;

    /** @param walNumber the WAL file holding this memtable's writes, or 0 if it was rebuilt by recovery */
    Memtable(long walNumber) {
        this.walNumber = walNumber;
    }

    long walNumber() {
        return walNumber;
    }

    long approximateBytes() {
        return approximateBytes;
    }

    int entryCount() {
        return entryCount;
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    void add(InternalEntry entry) {
        entries.put(new VersionedKey(entry.key(), entry.seq()), new Slot(entry.value(), entry.expireAt()));
        long size = entry.key().length + (entry.value() == null ? 0 : entry.value().length) + ENTRY_OVERHEAD_BYTES;
        approximateBytes = approximateBytes + size;
        entryCount = entryCount + 1;
    }

    /** The newest version of {@code key} with a sequence number of at most {@code snapshot}, or {@code null}. */
    InternalEntry get(byte[] key, long snapshot) {
        Map.Entry<VersionedKey, Slot> found = entries.ceilingEntry(new VersionedKey(key, snapshot));
        if (found == null || !Arrays.equals(found.getKey().key, key)) {
            return null;
        }
        return toEntry(found);
    }

    /**
     * Every version with a sequence number of at most {@code snapshot} and a key in {@code [from, to)}, in (key
     * ascending, sequence descending) order. {@code null} bounds are open.
     */
    Iterator<InternalEntry> iterator(byte[] from, byte[] to, long snapshot) {
        Iterator<Map.Entry<VersionedKey, Slot>> source = (from == null
                ? entries
                : entries.tailMap(new VersionedKey(from, Long.MAX_VALUE), true)).entrySet().iterator();
        return new Iterator<>() {
            private InternalEntry next = advance();

            private InternalEntry advance() {
                while (source.hasNext()) {
                    Map.Entry<VersionedKey, Slot> candidate = source.next();
                    if (to != null && Arrays.compareUnsigned(candidate.getKey().key, to) >= 0) {
                        return null;
                    }
                    if (candidate.getKey().seq <= snapshot) {
                        return toEntry(candidate);
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public InternalEntry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                InternalEntry current = next;
                next = advance();
                return current;
            }
        };
    }

    private static InternalEntry toEntry(Map.Entry<VersionedKey, Slot> entry) {
        Slot slot = entry.getValue();
        return new InternalEntry(entry.getKey().key, entry.getKey().seq, slot.value, slot.expireAt);
    }

    /** Orders by key (unsigned lexicographic), then by sequence number, newest first. */
    private record VersionedKey(byte[] key, long seq) implements Comparable<VersionedKey> {
        @Override
        public int compareTo(VersionedKey other) {
            int byKey = Arrays.compareUnsigned(key, other.key);
            return byKey != 0 ? byKey : Long.compare(other.seq, seq);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof VersionedKey that && seq == that.seq && Arrays.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(key) + Long.hashCode(seq);
        }
    }

    /** A value ({@code null} for a tombstone) and its absolute expiry. */
    private record Slot(byte[] value, long expireAt) {
    }
}
