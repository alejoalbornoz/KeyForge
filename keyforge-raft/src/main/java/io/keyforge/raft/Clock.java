package io.keyforge.raft;

/**
 * A monotonic millisecond clock, the only notion of time the Raft core has. Production uses {@link #system()}; the
 * deterministic simulator advances a virtual clock instead.
 *
 * <p>It only measures intervals (election timeouts, heartbeats), so it need not be related to wall-clock time.
 */
@FunctionalInterface
public interface Clock {

    /** Milliseconds since an arbitrary origin; never decreases. */
    long millis();

    /** {@link System#nanoTime()} in milliseconds: immune to wall-clock jumps. */
    static Clock system() {
        return () -> System.nanoTime() / 1_000_000;
    }
}
