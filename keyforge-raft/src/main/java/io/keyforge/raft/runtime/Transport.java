package io.keyforge.raft.runtime;

import java.util.function.Consumer;

import io.keyforge.raft.RaftMessage;

/**
 * Carries Raft messages between nodes. Delivery is best effort: a transport may drop, delay or duplicate messages, and
 * Raft tolerates all of it. Production uses gRPC ({@link io.keyforge.raft.grpc.GrpcTransport}); tests use an in-memory
 * network.
 */
public interface Transport extends AutoCloseable {

    /** Starts accepting messages for this node and hands each one to {@code inbound}, from any thread. */
    void start(Consumer<RaftMessage> inbound);

    /**
     * Sends a message without blocking for long. Only the runtime's Raft thread calls it. An
     * {@link RaftMessage.InstallSnapshot} carries a payload to stream.
     */
    void send(RaftMessage message);

    @Override
    void close();
}
