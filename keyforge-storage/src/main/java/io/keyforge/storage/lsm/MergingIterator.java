package io.keyforge.storage.lsm;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Merges sorted entry streams into one stream holding only the newest version of each key, tombstones included.
 *
 * <p>Each source yields entries in (key ascending, sequence descending) order. Sources are listed newest first; when
 * two sources hold the same key with the same sequence number, which only happens if they overlap, the earlier source
 * wins.
 */
final class MergingIterator implements Iterator<InternalEntry> {

    private static final Comparator<Head> ORDER = (a, b) -> {
        int byKey = Arrays.compareUnsigned(a.entry.key(), b.entry.key());
        if (byKey != 0) {
            return byKey;
        }
        int bySeq = Long.compare(b.entry.seq(), a.entry.seq());
        return bySeq != 0 ? bySeq : Integer.compare(a.rank, b.rank);
    };

    private final PriorityQueue<Head> heads;
    private byte[] lastKey;
    private InternalEntry next;

    MergingIterator(List<Iterator<InternalEntry>> sources) {
        heads = new PriorityQueue<>(Math.max(1, sources.size()), ORDER);
        for (int rank = 0; rank < sources.size(); rank++) {
            Iterator<InternalEntry> source = sources.get(rank);
            if (source.hasNext()) {
                heads.add(new Head(source.next(), rank, source));
            }
        }
        next = advance();
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

    private InternalEntry advance() {
        while (!heads.isEmpty()) {
            Head head = heads.poll();
            InternalEntry entry = head.entry;
            if (head.source.hasNext()) {
                head.entry = head.source.next();
                heads.add(head);
            }
            if (lastKey == null || !Arrays.equals(lastKey, entry.key())) {
                lastKey = entry.key();
                return entry;
            }
        }
        return null;
    }

    private static final class Head {
        InternalEntry entry;
        final int rank;
        final Iterator<InternalEntry> source;

        Head(InternalEntry entry, int rank, Iterator<InternalEntry> source) {
            this.entry = entry;
            this.rank = rank;
            this.source = source;
        }
    }
}
