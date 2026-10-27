package io.keyforge.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * A binary-safe RESP bulk string ({@code $5\r\nhello\r\n}).
 *
 * <p>The array is not copied: whoever builds the value hands the array over and must not modify it afterwards. The null
 * bulk string is {@link RespNull#BULK_STRING}, not a {@code RespBulkString}.
 */
public record RespBulkString(byte[] value) implements RespValue {

    public RespBulkString {
        Objects.requireNonNull(value, "value");
    }

    public static RespBulkString of(String value) {
        return new RespBulkString(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Decodes the content as UTF-8. */
    public String utf8() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RespBulkString that && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "RespBulkString[" + RespFormat.printable(value) + "]";
    }
}
