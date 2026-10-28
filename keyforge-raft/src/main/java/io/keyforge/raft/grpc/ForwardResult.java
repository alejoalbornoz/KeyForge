package io.keyforge.raft.grpc;

/** The outcome of a client request forwarded to another node. */
public sealed interface ForwardResult {

    /** The receiver executed the request; {@code reply} is its answer, opaque to the transport. */
    record Reply(byte[] reply) implements ForwardResult {
    }

    /** The receiver is not the leader; {@code leaderHint} is the leader it knows of, or {@code null}. */
    record NotLeader(String leaderHint) implements ForwardResult {
    }
}
