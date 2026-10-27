package io.keyforge.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Incremental decoder for any RESP2 value, including nested arrays. Clients use it to read replies; tests use it to
 * check the encoder.
 *
 * <p>Like {@link RespRequestDecoder} it is stateful and resumable: partially received arrays are kept on a stack, so
 * input split at any byte is decoded without rescanning consumed bytes. Instances are not thread-safe.
 */
public final class RespDecoder {

    private final int maxBulkLength;
    private final int maxArrayLength;

    /** Arrays whose elements are still arriving; the innermost is on top. */
    private final Deque<PartialArray> openArrays = new ArrayDeque<>();
    private int pendingBulkLength = -1;

    public RespDecoder() {
        this(RespRequestDecoder.DEFAULT_MAX_BULK_LENGTH, Integer.MAX_VALUE);
    }

    public RespDecoder(int maxBulkLength, int maxArrayLength) {
        this.maxBulkLength = maxBulkLength;
        this.maxArrayLength = maxArrayLength;
    }

    /**
     * Decodes the next value from {@code in}, advancing its position past consumed bytes.
     *
     * @return the next complete value, or {@code null} if more input is needed
     * @throws RespProtocolException if the input is not valid RESP2
     */
    public RespValue decode(ByteBuffer in) {
        while (true) {
            int before = in.position();
            RespValue value = pendingBulkLength >= 0 ? readBulkPayload(in) : readLine(in);
            if (value == null) {
                if (in.position() == before) {
                    return null; // incomplete: wait for more input
                }
                continue; // consumed a bulk or array header: keep going
            }
            value = attachToOpenArrays(value);
            if (value != null) {
                return value;
            }
        }
    }

    /**
     * Reads one line-based value or header. Returns null either without consuming anything (incomplete line) or after
     * consuming a header that opened a bulk string or an array.
     */
    private RespValue readLine(ByteBuffer in) {
        if (!in.hasRemaining()) {
            return null;
        }
        int start = in.position();
        int crlf = RespLines.findCrlf(in, start + 1);
        if (crlf < 0) {
            RespLines.checkIncompleteLine(in, "line too long");
            return null;
        }
        byte type = in.get(start);
        RespValue value = switch (type) {
            case '+' -> new RespSimpleString(text(in, start + 1, crlf));
            case '-' -> new RespError(text(in, start + 1, crlf));
            case ':' -> new RespInteger(RespLines.parseNumber(in, start + 1, crlf, "invalid integer"));
            case '$' -> {
                long length = RespLines.parseNumber(in, start + 1, crlf, "invalid bulk length");
                if (length == -1) {
                    yield RespNull.BULK_STRING;
                }
                if (length < 0 || length > maxBulkLength) {
                    throw new RespProtocolException("invalid bulk length");
                }
                pendingBulkLength = (int) length;
                yield null;
            }
            case '*' -> {
                long count = RespLines.parseNumber(in, start + 1, crlf, "invalid multibulk length");
                if (count == -1) {
                    yield RespNull.ARRAY;
                }
                if (count < 0 || count > maxArrayLength) {
                    throw new RespProtocolException("invalid multibulk length");
                }
                if (count == 0) {
                    yield RespArray.EMPTY;
                }
                openArrays.push(new PartialArray((int) count));
                yield null;
            }
            default -> throw new RespProtocolException("unexpected type byte '" + (char) (type & 0xFF) + "'");
        };
        in.position(crlf + 2);
        return value;
    }

    private RespValue readBulkPayload(ByteBuffer in) {
        if (in.remaining() < pendingBulkLength + 2L) {
            return null;
        }
        byte[] payload = new byte[pendingBulkLength];
        in.get(payload);
        if (in.get() != '\r' | in.get() != '\n') {
            throw new RespProtocolException("expected CRLF after bulk string");
        }
        pendingBulkLength = -1;
        return new RespBulkString(payload);
    }

    /** Adds a completed value to the innermost open array, closing arrays that become full. */
    private RespValue attachToOpenArrays(RespValue value) {
        while (!openArrays.isEmpty()) {
            PartialArray innermost = openArrays.peek();
            innermost.elements.add(value);
            if (innermost.elements.size() < innermost.expected) {
                return null;
            }
            openArrays.pop();
            value = new RespArray(innermost.elements);
        }
        return value;
    }

    private static String text(ByteBuffer in, int from, int to) {
        byte[] bytes = new byte[to - from];
        in.get(from, bytes);
        for (byte b : bytes) {
            if (b == '\n') {
                throw new RespProtocolException("bare LF inside a line");
            }
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class PartialArray {
        final int expected;
        final List<RespValue> elements;

        PartialArray(int expected) {
            this.expected = expected;
            this.elements = new ArrayList<>(Math.min(expected, 1024));
        }
    }
}
