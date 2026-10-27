package io.keyforge.protocol;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encodes {@link RespValue}s and client commands into RESP2 bytes.
 *
 * <p>Encoding writes straight into a {@link RespSink} and allocates nothing for ASCII strings, integers and bulk
 * strings, which covers every reply on the hot path.
 */
public final class RespEncoder {

    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] NULL_BULK_STRING = {'$', '-', '1', '\r', '\n'};
    private static final byte[] NULL_ARRAY = {'*', '-', '1', '\r', '\n'};

    private RespEncoder() {
    }

    public static void encode(RespValue value, RespSink sink) {
        switch (value) {
            case RespSimpleString simple -> writeLine('+', simple.value(), sink);
            case RespError error -> writeLine('-', error.message(), sink);
            case RespInteger integer -> {
                sink.writeByte(':');
                RespNumbers.write(integer.value(), sink);
                sink.writeBytes(CRLF);
            }
            case RespBulkString bulk -> writeBulkString(bulk.value(), sink);
            case RespArray array -> {
                writeHeader('*', array.elements().size(), sink);
                for (RespValue element : array.elements()) {
                    encode(element, sink);
                }
            }
            case RespNull nil -> sink.writeBytes(nil == RespNull.BULK_STRING ? NULL_BULK_STRING : NULL_ARRAY);
        }
    }

    public static byte[] encode(RespValue value) {
        ByteArrayRespSink sink = new ByteArrayRespSink((int) Math.min(encodedLength(value), 1 << 20));
        encode(value, sink);
        return sink.toByteArray();
    }

    /** Exact number of bytes {@link #encode(RespValue, RespSink)} writes for {@code value}. */
    public static long encodedLength(RespValue value) {
        return switch (value) {
            case RespSimpleString simple -> 3L + utf8Length(simple.value());
            case RespError error -> 3L + utf8Length(error.message());
            case RespInteger integer -> 3L + RespNumbers.length(integer.value());
            case RespBulkString bulk -> bulkStringLength(bulk.value().length);
            case RespArray array -> {
                long length = 3L + RespNumbers.length(array.elements().size());
                for (RespValue element : array.elements()) {
                    length += encodedLength(element);
                }
                yield length;
            }
            case RespNull nil -> NULL_BULK_STRING.length;
        };
    }

    /** Encodes a command the way clients send it: an array of bulk strings. */
    public static void encodeCommand(List<byte[]> arguments, RespSink sink) {
        writeHeader('*', arguments.size(), sink);
        for (byte[] argument : arguments) {
            writeBulkString(argument, sink);
        }
    }

    public static byte[] encodeCommand(List<byte[]> arguments) {
        ByteArrayRespSink sink = new ByteArrayRespSink();
        encodeCommand(arguments, sink);
        return sink.toByteArray();
    }

    /** Encodes a command given as UTF-8 strings, e.g. {@code encodeCommand("SET", "k", "v")}. */
    public static byte[] encodeCommand(String... arguments) {
        ByteArrayRespSink sink = new ByteArrayRespSink();
        writeHeader('*', arguments.length, sink);
        for (String argument : arguments) {
            writeBulkString(argument.getBytes(StandardCharsets.UTF_8), sink);
        }
        return sink.toByteArray();
    }

    private static void writeBulkString(byte[] content, RespSink sink) {
        writeHeader('$', content.length, sink);
        sink.writeBytes(content);
        sink.writeBytes(CRLF);
    }

    private static long bulkStringLength(int contentLength) {
        return 5L + RespNumbers.length(contentLength) + contentLength;
    }

    private static void writeHeader(char type, long count, RespSink sink) {
        sink.writeByte(type);
        RespNumbers.write(count, sink);
        sink.writeBytes(CRLF);
    }

    private static void writeLine(char type, String text, RespSink sink) {
        sink.writeByte(type);
        if (isAscii(text)) {
            for (int i = 0; i < text.length(); i++) {
                sink.writeByte(text.charAt(i));
            }
        } else {
            sink.writeBytes(text.getBytes(StandardCharsets.UTF_8));
        }
        sink.writeBytes(CRLF);
    }

    private static int utf8Length(String text) {
        return isAscii(text) ? text.length() : text.getBytes(StandardCharsets.UTF_8).length;
    }

    private static boolean isAscii(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) >= 0x80) {
                return false;
            }
        }
        return true;
    }
}
