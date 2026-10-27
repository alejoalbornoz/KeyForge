package io.keyforge.protocol;

import java.nio.ByteBuffer;

/** Line-level helpers shared by the decoders. All indexes are absolute; no method moves the buffer's position. */
final class RespLines {

    /**
     * Longest line (header, simple string or inline command) a decoder waits for before giving up. This is Redis'
     * {@code PROTO_INLINE_MAX_SIZE}.
     */
    static final int MAX_LINE_LENGTH = 64 * 1024;

    private RespLines() {
    }

    /**
     * Returns the index of the CR of the first CRLF at or after {@code from}, or -1 if the buffer does not hold a
     * complete line yet.
     *
     * @throws RespProtocolException if a CR is followed by anything other than LF
     */
    static int findCrlf(ByteBuffer in, int from) {
        int limit = in.limit();
        for (int i = from; i < limit; i++) {
            if (in.get(i) == '\r') {
                if (i + 1 >= limit) {
                    return -1;
                }
                if (in.get(i + 1) != '\n') {
                    throw new RespProtocolException("expected LF after CR");
                }
                return i;
            }
        }
        return -1;
    }

    /** Fails if the unterminated line at the buffer's position has grown past {@link #MAX_LINE_LENGTH}. */
    static void checkIncompleteLine(ByteBuffer in, String error) {
        if (in.remaining() > MAX_LINE_LENGTH) {
            throw new RespProtocolException(error);
        }
    }

    static long parseNumber(ByteBuffer in, int from, int to, String error) {
        try {
            return RespNumbers.parseLong(in, from, to);
        } catch (NumberFormatException e) {
            throw new RespProtocolException(error);
        }
    }
}
