package io.keyforge.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Incremental decoder for client requests, used by servers.
 *
 * <p>Accepts the two request forms Redis accepts: arrays of bulk strings ({@code *2\r\n$3\r\nGET\r\n$1\r\nk\r\n}),
 * which is what client libraries send, and inline commands ({@code GET k\r\n}), which is what people type into telnet.
 * Each request comes out as a {@code byte[][]} of its arguments; no {@link RespValue} objects are built.
 *
 * <p>The decoder is stateful. Input may arrive split at any byte: {@link #decode} consumes what it can, remembers how
 * far into a multi-bulk request it got (expected argument count, arguments so far, pending bulk length), and resumes on
 * the next call without rescanning anything it already consumed. A bulk string's payload is consumed only once it has
 * fully arrived, so a header announcing a huge length allocates nothing until the bytes are actually there.
 *
 * <p>Instances are not thread-safe: use one per connection.
 */
public final class RespRequestDecoder {

    /** Redis' default {@code proto-max-bulk-len}. */
    public static final int DEFAULT_MAX_BULK_LENGTH = 512 * 1024 * 1024;
    public static final int DEFAULT_MAX_ARRAY_LENGTH = 1024 * 1024;

    /** Argument slots allocated up front; larger requests grow as their arguments actually arrive. */
    private static final int PREALLOCATED_ARGUMENTS = 1024;

    private final int maxBulkLength;
    private final int maxArrayLength;

    /** Non-null while a multi-bulk request is in progress. */
    private byte[][] arguments;
    private int expectedArguments;
    private int argumentCount;
    /** Length announced by the last bulk header whose payload has not been consumed yet, or -1. */
    private int pendingBulkLength = -1;

    public RespRequestDecoder() {
        this(DEFAULT_MAX_BULK_LENGTH, DEFAULT_MAX_ARRAY_LENGTH);
    }

    public RespRequestDecoder(int maxBulkLength, int maxArrayLength) {
        if (maxBulkLength < 0 || maxArrayLength < 1) {
            throw new IllegalArgumentException("invalid limits: bulk " + maxBulkLength + ", array " + maxArrayLength);
        }
        this.maxBulkLength = maxBulkLength;
        this.maxArrayLength = maxArrayLength;
    }

    /**
     * Decodes the next request from {@code in}, starting at its position.
     *
     * <p>Consumed bytes are skipped by advancing the position. Returns {@code null} when more input is needed; the
     * unconsumed tail must be presented again, followed by new bytes, on the next call. Call repeatedly until it returns
     * {@code null} to drain pipelined requests.
     *
     * @return the request's arguments (never empty), or {@code null} if the buffer holds no complete request
     * @throws RespProtocolException if the input is not a valid request; the stream cannot be resumed afterwards
     */
    public byte[][] decode(ByteBuffer in) {
        while (true) {
            if (arguments == null) {
                if (!in.hasRemaining()) {
                    return null;
                }
                if (in.get(in.position()) != '*') {
                    byte[][] inline = decodeInline(in);
                    if (inline == null || inline.length > 0) {
                        return inline;
                    }
                    continue; // blank line: Redis ignores it
                }
                if (!startMultiBulk(in)) {
                    return null;
                }
                if (arguments == null) {
                    continue; // "*0" or "*-1": an empty request, ignored like Redis does
                }
            }
            return continueMultiBulk(in);
        }
    }

    /** Whether the decoder is in the middle of a request, i.e. has consumed bytes of a request it has not returned. */
    public boolean hasPartialRequest() {
        return arguments != null;
    }

    private boolean startMultiBulk(ByteBuffer in) {
        int start = in.position();
        int crlf = RespLines.findCrlf(in, start + 1);
        if (crlf < 0) {
            RespLines.checkIncompleteLine(in, "too big mbulk count string");
            return false;
        }
        long count = RespLines.parseNumber(in, start + 1, crlf, "invalid multibulk length");
        if (count > maxArrayLength) {
            throw new RespProtocolException("invalid multibulk length");
        }
        in.position(crlf + 2);
        if (count > 0) {
            expectedArguments = (int) count;
            arguments = new byte[Math.min(expectedArguments, PREALLOCATED_ARGUMENTS)][];
            argumentCount = 0;
        }
        return true;
    }

    private byte[][] continueMultiBulk(ByteBuffer in) {
        while (argumentCount < expectedArguments) {
            if (pendingBulkLength < 0 && !readBulkHeader(in)) {
                return null;
            }
            if (in.remaining() < pendingBulkLength + 2L) {
                return null;
            }
            byte[] argument = new byte[pendingBulkLength];
            in.get(argument);
            if (in.get() != '\r' | in.get() != '\n') {
                throw new RespProtocolException("expected CRLF after bulk string");
            }
            pendingBulkLength = -1;
            if (argumentCount == arguments.length) {
                arguments = Arrays.copyOf(arguments, (int) Math.min(expectedArguments, 2L * arguments.length));
            }
            arguments[argumentCount++] = argument;
        }
        byte[][] request = arguments;
        arguments = null;
        return request;
    }

    private boolean readBulkHeader(ByteBuffer in) {
        if (!in.hasRemaining()) {
            return false;
        }
        int start = in.position();
        byte type = in.get(start);
        if (type != '$') {
            throw new RespProtocolException("expected '$', got '" + (char) (type & 0xFF) + "'");
        }
        int crlf = RespLines.findCrlf(in, start + 1);
        if (crlf < 0) {
            RespLines.checkIncompleteLine(in, "too big bulk count string");
            return false;
        }
        long length = RespLines.parseNumber(in, start + 1, crlf, "invalid bulk length");
        if (length < 0 || length > maxBulkLength) {
            throw new RespProtocolException("invalid bulk length");
        }
        pendingBulkLength = (int) length;
        in.position(crlf + 2);
        return true;
    }

    private static byte[][] decodeInline(ByteBuffer in) {
        int start = in.position();
        int limit = in.limit();
        int newline = -1;
        for (int i = start; i < limit; i++) {
            if (in.get(i) == '\n') {
                newline = i;
                break;
            }
        }
        if (newline < 0) {
            RespLines.checkIncompleteLine(in, "too big inline request");
            return null;
        }
        int end = newline > start && in.get(newline - 1) == '\r' ? newline - 1 : newline;
        in.position(newline + 1);
        return InlineCommandParser.parse(in, start, end);
    }
}
