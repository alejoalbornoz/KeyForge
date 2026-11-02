package io.keyforge.raft.grpc;

/**
 * A forwarded request was not sent: there was no established connection to the peer. Unlike a failure after sending,
 * it certainly had no effect, so it may be retried on another node.
 */
public final class ForwardNotSentException extends RuntimeException {

    public ForwardNotSentException(String peer, String connectionState) {
        super("no connection to " + peer + " (" + connectionState + "); the request was not sent", null, false, false);
    }
}
