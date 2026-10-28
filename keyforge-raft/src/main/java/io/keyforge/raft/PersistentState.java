package io.keyforge.raft;

import java.util.List;

/**
 * Everything Raft keeps on disk, as recovered at startup: the term and vote, the snapshot the log starts after, and the
 * log entries that follow it.
 *
 * @param votedFor      the candidate voted for in {@code term}, or {@code null}
 * @param snapshotIndex index of the last entry the latest snapshot covers; 0 if there is none
 * @param snapshotTerm  term of that entry; 0 if there is no snapshot
 * @param entries       the entries after {@code snapshotIndex}, contiguous
 */
public record PersistentState(long term, String votedFor, long snapshotIndex, long snapshotTerm,
                              List<LogEntry> entries) {

    public static final PersistentState EMPTY = new PersistentState(0, null, 0, 0, List.of());

    public PersistentState {
        entries = List.copyOf(entries);
        long expected = snapshotIndex + 1;
        for (LogEntry entry : entries) {
            if (entry.index() != expected++) {
                throw new IllegalArgumentException("log entries are not contiguous after index " + snapshotIndex
                        + ": found " + entry.index() + " where " + (expected - 1) + " was expected");
            }
        }
    }

    public long lastIndex() {
        return snapshotIndex + entries.size();
    }
}
