package io.keyforge.raft;

/**
 * An input to {@link RaftNode#handle}: a timer tick, a message from another node, or a request from the node's own
 * driver.
 */
public sealed interface RaftEvent permits RaftEvent.Tick, RaftEvent.Propose, RaftEvent.ReadIndex,
        RaftEvent.SnapshotTaken, RaftMessage {

    /** The canonical tick. */
    Tick TICK = new Tick();

    /** Time may have passed: check the election timeout, heartbeats and the quorum check. */
    record Tick() implements RaftEvent {
    }

    /**
     * A client command to append to the log. Answered with {@link RaftAction.ProposalAccepted} or
     * {@link RaftAction.ProposalRejected}.
     *
     * @param requestId chosen by the driver, echoed in the answer
     */
    record Propose(long requestId, byte[] command) implements RaftEvent {
    }

    /**
     * A linearizable read (ReadIndex). Answered with {@link RaftAction.ReadReady} once the leader has confirmed it was
     * still the leader after the request arrived, or with {@link RaftAction.ReadRejected}.
     */
    record ReadIndex(long requestId) implements RaftEvent {
    }

    /**
     * The driver has durably stored a snapshot of the state machine covering every entry up to {@code index}; the log
     * up to there can be discarded.
     */
    record SnapshotTaken(long index, long term) implements RaftEvent {
    }
}
