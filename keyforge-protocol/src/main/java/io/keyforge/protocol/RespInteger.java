package io.keyforge.protocol;

/** A RESP integer ({@code :42\r\n}). */
public record RespInteger(long value) implements RespValue {

    public static final RespInteger ZERO = new RespInteger(0);
    public static final RespInteger ONE = new RespInteger(1);

    public static RespInteger of(long value) {
        if (value == 0) {
            return ZERO;
        }
        return value == 1 ? ONE : new RespInteger(value);
    }
}
