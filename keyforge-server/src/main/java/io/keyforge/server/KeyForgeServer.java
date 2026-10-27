package io.keyforge.server;

/** A network front end that accepts RESP connections and hands requests to a command dispatcher. */
public interface KeyForgeServer extends AutoCloseable {

    /** Binds the listening socket and starts serving. Returns once the server accepts connections. */
    void start() throws InterruptedException;

    /** The bound port; useful when the configured port is 0. Only valid after {@link #start()}. */
    int port();

    /** Stops accepting, closes open connections and releases threads. */
    @Override
    void close();
}
