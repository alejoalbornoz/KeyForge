package io.keyforge.raft.sim;

import java.util.ArrayList;
import java.util.List;

import io.keyforge.raft.LogEntry;
import io.keyforge.raft.PersistentState;

/**
 * What survives a simulated crash: the term and vote, the log, the latest snapshot, and an optional durable image of
 * the state machine (like the LSM engine's flushed tables, which may lag the applied index).
 *
 * <p>The log may begin before the snapshot: a snapshot is written first and the log compacted only once the core
 * processes {@link io.keyforge.raft.RaftEvent.SnapshotTaken}, and a crash can fall in between.
 */
final class SimDisk {

    long term;
    String votedFor;

    /** Index and term of the entry just before the first entry of {@link #log}. */
    long logBaseIndex;
    long logBaseTerm;
    final ArrayList<LogEntry> log = new ArrayList<>();

    long snapshotIndex;
    long snapshotTerm;
    byte[] snapshot;

    /** A durable copy of the state machine, or {@code null}. */
    KvMachine image;

    long lastIndex() {
        return logBaseIndex + log.size();
    }

    long lastTerm() {
        return log.isEmpty() ? logBaseTerm : log.getLast().term();
    }

    /** The term of the entry at {@code index}, which must be the base or in the log. */
    long termAt(long index) {
        if (index == logBaseIndex) {
            return logBaseTerm;
        }
        return entry(index).term();
    }

    LogEntry entry(long index) {
        if (index <= logBaseIndex || index > lastIndex()) {
            throw new InvariantViolation("disk log (" + logBaseIndex + ", " + lastIndex() + "] has no entry " + index);
        }
        return log.get((int) (index - logBaseIndex - 1));
    }

    void append(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (entry.index() != lastIndex() + 1) {
                throw new InvariantViolation("appending " + entry + " to a disk log ending at " + lastIndex());
            }
            log.add(entry);
        }
    }

    void truncateFrom(long index) {
        if (index <= logBaseIndex) {
            throw new InvariantViolation("truncating at " + index + " below the log base " + logBaseIndex);
        }
        while (lastIndex() >= index) {
            log.removeLast();
        }
    }

    void compactThrough(long index, long term) {
        if (snapshot == null || snapshotIndex < index) {
            throw new InvariantViolation("compacting through " + index + " without a snapshot covering it");
        }
        if (index > logBaseIndex) {
            if (index <= lastIndex() && entry(index).term() != term) {
                throw new InvariantViolation("compacting through " + index + "@" + term + " but the disk has "
                        + entry(index));
            }
            while (!log.isEmpty() && log.getFirst().index() <= index) {
                log.removeFirst();
            }
            logBaseIndex = index;
            logBaseTerm = term;
        }
    }

    void installSnapshot(long index, long term, byte[] bytes) {
        snapshotIndex = index;
        snapshotTerm = term;
        snapshot = bytes;
        log.clear();
        logBaseIndex = index;
        logBaseTerm = term;
    }

    /** What a restarting node recovers: entries the snapshot covers are dropped. */
    PersistentState load() {
        List<LogEntry> entries = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.index() > snapshotIndex) {
                entries.add(entry);
            } else if (entry.index() == snapshotIndex && entry.term() != snapshotTerm) {
                throw new InvariantViolation("snapshot " + snapshotIndex + "@" + snapshotTerm + " disagrees with "
                        + entry);
            }
        }
        if (snapshotIndex < logBaseIndex) {
            throw new InvariantViolation("the log starts after " + logBaseIndex + " but the snapshot ends at "
                    + snapshotIndex);
        }
        return new PersistentState(term, votedFor, snapshotIndex, snapshotTerm, entries);
    }
}
