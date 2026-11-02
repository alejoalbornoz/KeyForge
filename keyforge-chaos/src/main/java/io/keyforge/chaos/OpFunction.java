package io.keyforge.chaos;

import java.util.Locale;

/** The operations the workload runs, one key each. */
public enum OpFunction {
    /** {@code GET key}: the value, or nil. */
    GET,
    /** {@code SET key value}: always an integer value, so that {@code INCR} applies to every key. */
    SET,
    /** {@code INCR key}: the value after the increment (a missing key counts as 0). */
    INCR;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
