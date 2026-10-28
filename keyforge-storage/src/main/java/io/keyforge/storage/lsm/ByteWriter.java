package io.keyforge.storage.lsm;

import java.util.Arrays;

/**
 * A growable byte buffer for encoding WAL records, blocks and the manifest. Multi-byte integers are big-endian; varints
 * are unsigned LEB128. Not thread-safe.
 */
final class ByteWriter {

    /** Largest array the JVM reliably allocates. */
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 16;

    private byte[] buffer;
    private int size;

    ByteWriter(int initialCapacity) {
        buffer = new byte[initialCapacity];
    }

    int size() {
        return size;
    }

    /** The backing array; bytes {@code [0, size())} are valid. */
    byte[] array() {
        return buffer;
    }

    byte[] toByteArray() {
        return Arrays.copyOf(buffer, size);
    }

    void reset() {
        size = 0;
    }

    /** Drops everything written after {@code newSize}. */
    void truncate(int newSize) {
        if (newSize < 0 || newSize > size) {
            throw new IllegalArgumentException("Cannot truncate " + size + " bytes to " + newSize);
        }
        size = newSize;
    }

    void writeByte(int value) {
        ensure(1);
        buffer[size++] = (byte) value;
    }

    void writeInt(int value) {
        ensure(4);
        putInt(size, value);
        size += 4;
    }

    /** Overwrites four bytes at {@code position}, which must already have been written. */
    void putInt(int position, int value) {
        buffer[position] = (byte) (value >>> 24);
        buffer[position + 1] = (byte) (value >>> 16);
        buffer[position + 2] = (byte) (value >>> 8);
        buffer[position + 3] = (byte) value;
    }

    void writeLong(long value) {
        ensure(8);
        for (int shift = 56; shift >= 0; shift -= 8) {
            buffer[size++] = (byte) (value >>> shift);
        }
    }

    void writeVarInt(int value) {
        ensure(5);
        while ((value & ~0x7F) != 0) {
            buffer[size++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[size++] = (byte) value;
    }

    void writeVarLong(long value) {
        ensure(10);
        while ((value & ~0x7FL) != 0) {
            buffer[size++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[size++] = (byte) value;
    }

    void writeBytes(byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
    }

    void writeBytes(byte[] bytes, int offset, int length) {
        ensure(length);
        System.arraycopy(bytes, offset, buffer, size, length);
        size += length;
    }

    /** Reserves {@code length} bytes, to be filled in later with {@link #putInt}, and returns their position. */
    int skip(int length) {
        ensure(length);
        int position = size;
        size += length;
        return position;
    }

    private void ensure(int extra) {
        long required = (long) size + extra;
        if (required <= buffer.length) {
            return;
        }
        if (required > MAX_CAPACITY) {
            throw new IllegalArgumentException("Encoded data would exceed " + MAX_CAPACITY + " bytes");
        }
        long grown = Math.max(required, Math.min((long) buffer.length * 2, MAX_CAPACITY));
        buffer = Arrays.copyOf(buffer, (int) Math.max(grown, 16));
    }
}
