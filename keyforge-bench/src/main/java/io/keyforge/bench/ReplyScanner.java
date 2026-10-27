package io.keyforge.bench;

import io.netty.buffer.ByteBuf;

/**
 * Skips over complete RESP replies without materialising them, so the load generator allocates nothing per reply.
 * Handles the reply types {@code GET} and {@code SET} produce: simple strings, errors, integers and bulk strings
 * (including the null bulk string).
 */
final class ReplyScanner {

    /** The buffer does not hold a complete reply yet; nothing was consumed. */
    static final int INCOMPLETE = -1;
    /** A non-error reply was consumed. */
    static final int OK = 0;
    /** An error reply was consumed. */
    static final int ERROR = 1;

    private ReplyScanner() {
    }

    /**
     * Consumes one reply from {@code in}, starting at its reader index.
     *
     * @return {@link #OK}, {@link #ERROR}, or {@link #INCOMPLETE} if the reply has not fully arrived
     * @throws IllegalStateException for a reply type the load generator never expects, such as an array
     */
    static int skipReply(ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        if (start == end) {
            return INCOMPLETE;
        }
        int lineFeed = in.indexOf(start + 1, end, (byte) '\n');
        if (lineFeed < 0) {
            return INCOMPLETE;
        }
        byte type = in.getByte(start);
        switch (type) {
            case '+', ':' -> {
                in.readerIndex(lineFeed + 1);
                return OK;
            }
            case '-' -> {
                in.readerIndex(lineFeed + 1);
                return ERROR;
            }
            case '$' -> {
                long length = parseLength(in, start + 1, lineFeed - 1);
                long next = length < 0 ? lineFeed + 1L : lineFeed + 1L + length + 2;
                if (next > end) {
                    return INCOMPLETE;
                }
                in.readerIndex((int) next);
                return OK;
            }
            default -> throw new IllegalStateException("Unexpected reply type '" + (char) (type & 0xFF) + "'");
        }
    }

    /** Parses the decimal number in {@code [from, to)}. */
    private static long parseLength(ByteBuf in, int from, int to) {
        boolean negative = in.getByte(from) == '-';
        long value = 0;
        for (int i = negative ? from + 1 : from; i < to; i++) {
            byte digit = in.getByte(i);
            if (digit < '0' || digit > '9') {
                throw new IllegalStateException("Malformed bulk length");
            }
            value = value * 10 + (digit - '0');
        }
        return negative ? -value : value;
    }
}
