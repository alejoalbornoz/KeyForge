package io.keyforge.raft.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.keyforge.raft.LogEntry;

/**
 * The replicated state machine. The runtime calls it from one thread, the apply thread, in log order.
 *
 * <p>{@link #apply} must be deterministic: every replica applies the same entries in the same order and must reach the
 * same state, so it may depend only on the state and the entry, never on the local clock or anything else node-local.
 */
public interface StateMachine {

    /**
     * The index of the last entry whose effects survive a restart. Read once at startup: the runtime replays the log
     * from the next entry (or restores the latest snapshot first, if the state machine is behind it).
     */
    long durableAppliedIndex();

    /**
     * Applies a committed entry, a command or a leader's no-op, and returns the result for the client that proposed
     * it. A command the state machine rejects (a type error, say) returns an error result; throwing means the state
     * machine itself failed and stops the node.
     */
    Object apply(LogEntry entry);

    /** Writes the state as of the last applied entry. */
    void writeSnapshot(OutputStream out) throws IOException;

    /** Replaces the whole state with a snapshot of the entries up to {@code index}. */
    void restoreSnapshot(long index, long term, InputStream in) throws IOException;
}
