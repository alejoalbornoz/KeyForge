package io.keyforge.raft.runtime;

/**
 * A proposal was accepted into the log, but this node can no longer tell whether it committed (its state was replaced
 * by a snapshot, or the node stopped). Retrying it could apply it twice.
 */
public final class OutcomeUnknownException extends RuntimeException {

    public OutcomeUnknownException(String message) {
        super(message, null, false, false);
    }
}
