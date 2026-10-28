package io.keyforge.raft.grpc;

/**
 * Executes a client request another node forwarded here. The transport calls it on a virtual thread, so it may block
 * until the request is committed.
 */
@FunctionalInterface
public interface ForwardHandler {

    ForwardResult handle(String from, byte[] request);
}
