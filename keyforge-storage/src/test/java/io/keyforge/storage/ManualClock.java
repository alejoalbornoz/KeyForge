package io.keyforge.storage;

import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.atomic.AtomicLong;

/** A clock that only moves when a test says so. */
public final class ManualClock implements InstantSource {

    private final AtomicLong millis;

    public ManualClock(long startMillis) {
        millis = new AtomicLong(startMillis);
    }

    public void advance(long deltaMillis) {
        millis.addAndGet(deltaMillis);
    }

    @Override
    public long millis() {
        return millis.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }
}
