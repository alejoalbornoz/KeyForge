package io.keyforge.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The core's view of the log: the snapshot it starts after, and the entries that follow. The driver keeps the durable
 * copy in step through {@link RaftAction.Persist} actions.
 */
final class RaftLog {

    private long snapshotIndex;
    private long snapshotTerm;
    /** {@code entries.get(i)} has index {@code snapshotIndex + 1 + i}. */
    private final ArrayList<LogEntry> entries;

    RaftLog(long snapshotIndex, long snapshotTerm, List<LogEntry> entries) {
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.entries = new ArrayList<>(entries);
    }

    long snapshotIndex() {
        return snapshotIndex;
    }

    long snapshotTerm() {
        return snapshotTerm;
    }

    long lastIndex() {
        return snapshotIndex + entries.size();
    }

    long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.getLast().term();
    }

    /** Whether the term of {@code index} is known: the snapshot's last entry or an entry in the log. */
    boolean hasTerm(long index) {
        return index >= snapshotIndex && index <= lastIndex();
    }

    /** The term of {@code index}, which must satisfy {@link #hasTerm}. */
    long term(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        return get(index).term();
    }

    LogEntry get(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("index " + index + " is outside the log (" + snapshotIndex + ", "
                    + lastIndex() + "]");
        }
        return entries.get(offset(index));
    }

    /** The entries in {@code [from, toExclusive)}, as an immutable copy. */
    List<LogEntry> slice(long from, long toExclusive) {
        if (from > toExclusive) {
            throw new IllegalArgumentException("from " + from + " > to " + toExclusive);
        }
        if (from == toExclusive) {
            return List.of();
        }
        get(from);
        get(toExclusive - 1);
        return List.copyOf(entries.subList(offset(from), offset(toExclusive)));
    }

    /** Up to {@code maxCount} entries from {@code from}, stopping once their data passes {@code maxBytes}. */
    List<LogEntry> sliceForAppend(long from, int maxCount, long maxBytes) {
        long last = lastIndex();
        if (from > last) {
            return List.of();
        }
        int start = offset(from);
        int end = start;
        long bytes = 0;
        while (end < entries.size() && end - start < maxCount && (end == start || bytes < maxBytes)) {
            bytes += entries.get(end).data().length;
            end++;
        }
        return List.copyOf(entries.subList(start, end));
    }

    /** First index of the run of entries with the same term that contains {@code index}. */
    long firstIndexOfTermAt(long index) {
        long term = term(index);
        long first = index;
        while (first - 1 > snapshotIndex && get(first - 1).term() == term) {
            first--;
        }
        return first;
    }

    void append(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalArgumentException("appending " + entry + " after index " + lastIndex());
        }
        entries.add(entry);
    }

    /** Removes every entry at or after {@code index}. */
    void truncateFrom(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalArgumentException("cannot truncate into the snapshot at " + snapshotIndex);
        }
        if (index <= lastIndex()) {
            entries.subList(offset(index), entries.size()).clear();
        }
    }

    /** Drops the entries up to and including {@code index}, which a snapshot now covers. */
    void compactThrough(long index) {
        long term = term(index);
        entries.subList(0, offset(index) + 1).clear();
        snapshotIndex = index;
        snapshotTerm = term;
    }

    /** Discards the whole log: it now starts after a snapshot ending at {@code index}. */
    void reset(long index, long term) {
        entries.clear();
        snapshotIndex = index;
        snapshotTerm = term;
    }

    private int offset(long index) {
        return (int) (index - snapshotIndex - 1);
    }
}
