package io.keyforge.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Decimal integers as Redis reads and writes them.
 *
 * <p>Parsing follows Redis' {@code string2ll}: an optional {@code -}, then digits with no leading zero, fitting in a
 * signed 64-bit integer. Leading {@code +}, whitespace, {@code -0} and the empty string are all rejected. The same rule
 * applies to protocol headers ({@code *3}, {@code $5}), to command arguments ({@code EX 10}) and to stored values that
 * {@code INCR} reads.
 */
public final class RespNumbers {

    /** Length of the longest decimal long, {@code -9223372036854775808}. */
    public static final int MAX_LONG_LENGTH = 20;

    private static final long[] POWERS_OF_TEN = {
            1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L,
            10_000_000_000L, 100_000_000_000L, 1_000_000_000_000L, 10_000_000_000_000L, 100_000_000_000_000L,
            1_000_000_000_000_000L, 10_000_000_000_000_000L, 100_000_000_000_000_000L, 1_000_000_000_000_000_000L
    };

    private static final byte[] MIN_VALUE_BYTES = Long.toString(Long.MIN_VALUE).getBytes(StandardCharsets.US_ASCII);

    private RespNumbers() {
    }

    /**
     * Parses the whole array as a decimal long.
     *
     * @throws NumberFormatException if the bytes are not a canonical decimal long
     */
    public static long parseLong(byte[] bytes) {
        return parseLong(ByteBuffer.wrap(bytes), 0, bytes.length);
    }

    /**
     * Parses {@code buffer[from, to)} (absolute indexes) as a decimal long without moving the buffer's position.
     *
     * @throws NumberFormatException if the bytes are not a canonical decimal long
     */
    public static long parseLong(ByteBuffer buffer, int from, int to) {
        int length = to - from;
        if (length <= 0 || length > MAX_LONG_LENGTH) {
            throw invalid();
        }
        if (length == 1 && buffer.get(from) == '0') {
            return 0;
        }
        int i = from;
        boolean negative = buffer.get(i) == '-';
        if (negative && ++i == to) {
            throw invalid();
        }
        int first = buffer.get(i++) - '0';
        if (first < 1 || first > 9) {
            throw invalid();
        }
        // Accumulate as a negative number: its range includes Long.MIN_VALUE.
        long result = -first;
        for (; i < to; i++) {
            int digit = buffer.get(i) - '0';
            if (digit < 0 || digit > 9 || result < Long.MIN_VALUE / 10) {
                throw invalid();
            }
            result *= 10;
            if (result < Long.MIN_VALUE + digit) {
                throw invalid();
            }
            result -= digit;
        }
        if (negative) {
            return result;
        }
        if (result == Long.MIN_VALUE) {
            throw invalid();
        }
        return -result;
    }

    /** The canonical decimal form of {@code value}, as stored by {@code INCR}. */
    public static byte[] toBytes(long value) {
        ByteArrayRespSink sink = new ByteArrayRespSink(MAX_LONG_LENGTH);
        write(value, sink);
        return sink.toByteArray();
    }

    /** Number of bytes {@link #write} produces for {@code value}. */
    public static int length(long value) {
        if (value == Long.MIN_VALUE) {
            return MIN_VALUE_BYTES.length;
        }
        return value < 0 ? 1 + digitCount(-value) : digitCount(value);
    }

    /** Writes the decimal form of {@code value} without allocating. */
    public static void write(long value, RespSink sink) {
        if (value == Long.MIN_VALUE) {
            sink.writeBytes(MIN_VALUE_BYTES);
            return;
        }
        if (value < 0) {
            sink.writeByte('-');
            value = -value;
        }
        for (int exponent = digitCount(value) - 1; exponent >= 0; exponent--) {
            long power = POWERS_OF_TEN[exponent];
            int digit = (int) (value / power);
            sink.writeByte('0' + digit);
            value -= digit * power;
        }
    }

    private static int digitCount(long nonNegative) {
        int digits = 1;
        while (digits < POWERS_OF_TEN.length && nonNegative >= POWERS_OF_TEN[digits]) {
            digits++;
        }
        return digits;
    }

    private static NumberFormatException invalid() {
        return new NumberFormatException("not a canonical 64-bit decimal integer");
    }
}
