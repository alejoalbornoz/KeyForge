package io.keyforge.storage.lsm;

import java.util.Arrays;

/**
 * Reads what {@link ByteWriter} wrote. Every read is bounds-checked: running past the end, or a varint that is too long,
 * means the data is corrupt and throws {@link CorruptionException}. Not thread-safe.
 */
final class ByteReader {

    private final byte[] buffer;
    private final int limit;
    private final String source;
    private int position;

    ByteReader(byte[] buffer, String source) {
        this(buffer, 0, buffer.length, source);
    }

    ByteReader(byte[] buffer, int offset, int length, String source) {
        this.buffer = buffer;
        this.position = offset;
        this.limit = offset + length;
        this.source = source;
    }

    byte[] array() {
        return buffer;
    }

    int position() {
        return position;
    }

    boolean hasRemaining() {
        return position < limit;
    }

    int remaining() {
        return limit - position;
    }

    int readByte() {
        require(1);
        return buffer[position++] & 0xFF;
    }

    int readInt() {
        require(4);
        int value = ((buffer[position] & 0xFF) << 24)
                | ((buffer[position + 1] & 0xFF) << 16)
                | ((buffer[position + 2] & 0xFF) << 8)
                | (buffer[position + 3] & 0xFF);
        position += 4;
        return value;
    }

    long readLong() {
        require(8);
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (buffer[position++] & 0xFF);
        }
        return value;
    }

    int readVarInt() {
        int value = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            int b = readByte();
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw corrupt("varint longer than 5 bytes");
    }

    /** A varint that must be a valid length: not negative and not past the end of the data. */
    int readLength() {
        int length = readVarInt();
        if (length < 0 || length > remaining()) {
            throw corrupt("length " + length + " exceeds the " + remaining() + " remaining bytes");
        }
        return length;
    }

    long readVarLong() {
        long value = 0;
        for (int shift = 0; shift < 70; shift += 7) {
            int b = readByte();
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw corrupt("varint longer than 10 bytes");
    }

    byte[] readBytes(int length) {
        require(length);
        byte[] bytes = Arrays.copyOfRange(buffer, position, position + length);
        position += length;
        return bytes;
    }

    void skip(int length) {
        require(length);
        position += length;
    }

    CorruptionException corrupt(String problem) {
        return new CorruptionException(source + ": " + problem);
    }

    private void require(int length) {
        if (length < 0 || length > limit - position) {
            throw corrupt("unexpected end of data");
        }
    }
}
