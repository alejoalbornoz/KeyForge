package io.keyforge.server.command;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection state. A session is only touched by the thread serving its connection, so it needs no
 * synchronization.
 */
public final class ClientSession {

    private static final AtomicLong NEXT_ID = new AtomicLong(1);

    private final long id = NEXT_ID.getAndIncrement();
    private String name;
    private boolean closeRequested;

    public long id() {
        return id;
    }

    /** The name set with {@code CLIENT SETNAME}, or {@code null}. */
    public String name() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    /** Whether the connection should be closed once the current reply is written ({@code QUIT}). */
    public boolean isCloseRequested() {
        return closeRequested;
    }

    void requestClose() {
        closeRequested = true;
    }
}
