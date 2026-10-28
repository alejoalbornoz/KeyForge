package io.keyforge.raft;

import java.util.Arrays;
import java.util.Objects;

/**
 * One entry of the replicated log. Entries are immutable once created; nobody modifies {@code data}.
 *
 * @param index position in the log, starting at 1
 * @param term  term of the leader that created the entry
 * @param kind  a client command, or the no-op a new leader appends to commit entries of earlier terms
 * @param data  the command, opaque to Raft; empty for a no-op
 */
public record LogEntry(long index, long term, Kind kind, byte[] data) {

    private static final byte[] EMPTY = new byte[0];

    public enum Kind {
        /** Appended by a new leader; carries no data. */
        NOOP,
        /** A client command for the state machine. */
        COMMAND
    }

    public LogEntry {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(data, "data");
        if (index < 1 || term < 1) {
            throw new IllegalArgumentException("index and term must be positive: " + index + ", " + term);
        }
    }

    public static LogEntry noop(long index, long term) {
        return new LogEntry(index, term, Kind.NOOP, EMPTY);
    }

    public static LogEntry command(long index, long term, byte[] data) {
        return new LogEntry(index, term, Kind.COMMAND, data);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LogEntry entry && index == entry.index && term == entry.term && kind == entry.kind
                && Arrays.equals(data, entry.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term, kind, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        return "LogEntry[" + index + "@" + term + (kind == Kind.NOOP ? " noop" : ", " + data.length + " bytes") + "]";
    }
}
