package io.keyforge.raft;

/**
 * A read-only view of a node's Raft state, for monitoring and tests.
 *
 * @param leaderId the leader this node knows of, or {@code null}
 */
public record RaftStatus(String nodeId, Role role, long term, String leaderId, long commitIndex, long lastApplied,
                         long lastLogIndex, long lastLogTerm, long snapshotIndex) {
}
