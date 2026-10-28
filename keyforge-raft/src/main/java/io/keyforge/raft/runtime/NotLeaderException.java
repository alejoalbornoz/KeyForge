package io.keyforge.raft.runtime;

/**
 * This node cannot serve the request because it is not the leader, or lost leadership before the request took
 * effect. The request had no effect, so retrying it elsewhere is safe.
 */
public final class NotLeaderException extends RuntimeException {

    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super(leaderHint == null ? "no leader is known" : "the leader is " + leaderHint, null, false, false);
        this.leaderHint = leaderHint;
    }

    /** The leader this node knows of, or {@code null}. */
    public String leaderHint() {
        return leaderHint;
    }
}
