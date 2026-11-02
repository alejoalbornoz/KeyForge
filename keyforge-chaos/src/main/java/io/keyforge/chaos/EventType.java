package io.keyforge.chaos;

import java.util.Locale;

/**
 * The type of a history event, as in Jepsen. Every operation is an {@link #INVOKE} followed by exactly one completion
 * of the same process.
 */
public enum EventType {
    /** A client is about to send the operation. */
    INVOKE,
    /** The operation took effect, with this result. */
    OK,
    /** The operation certainly did not take effect: it was never sent, or the server said it did not run it. */
    FAIL,
    /**
     * Indeterminate: the operation may or may not have taken effect, now or later (a timeout, a lost connection, an
     * error that says the outcome is unknown). Never recorded as a failure.
     */
    INFO;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
