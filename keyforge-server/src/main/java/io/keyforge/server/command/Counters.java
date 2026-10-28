package io.keyforge.server.command;

import io.keyforge.protocol.RespNumbers;

/**
 * Redis' integer semantics for {@code INCR} and {@code DECR}, shared by the standalone keyspace and the replicated state
 * machine so both answer the same way.
 */
public final class Counters {

    public static final String NOT_AN_INTEGER = Arguments.NOT_AN_INTEGER;
    public static final String OVERFLOW = "ERR increment or decrement would overflow";

    private Counters() {
    }

    /**
     * The new value of a counter.
     *
     * @param current the stored value, or {@code null} for an absent key (counts as 0)
     * @throws NumberFormatException if {@code current} is not a decimal 64-bit integer
     * @throws ArithmeticException   if the result overflows
     */
    public static byte[] add(byte[] current, long delta) {
        long value = current == null ? 0 : RespNumbers.parseLong(current);
        return RespNumbers.toBytes(Math.addExact(value, delta));
    }

    /** The Redis error for a failure of {@link #add}. */
    public static String errorFor(RuntimeException failure) {
        return failure instanceof ArithmeticException ? OVERFLOW : NOT_AN_INTEGER;
    }
}
