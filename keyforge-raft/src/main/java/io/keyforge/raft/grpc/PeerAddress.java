package io.keyforge.raft.grpc;

import java.util.Objects;

/** Where a peer's Raft transport listens. */
public record PeerAddress(String host, int port) {

    public PeerAddress {
        Objects.requireNonNull(host, "host");
        if (host.isBlank() || port < 1 || port > 65535) {
            throw new IllegalArgumentException("invalid peer address " + host + ":" + port);
        }
    }

    /** Parses {@code host:port}. */
    public static PeerAddress parse(String text) {
        int colon = text.lastIndexOf(':');
        if (colon <= 0 || colon == text.length() - 1) {
            throw new IllegalArgumentException("expected host:port, got '" + text + "'");
        }
        try {
            return new PeerAddress(text.substring(0, colon).strip(), Integer.parseInt(text.substring(colon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("expected host:port, got '" + text + "'");
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
