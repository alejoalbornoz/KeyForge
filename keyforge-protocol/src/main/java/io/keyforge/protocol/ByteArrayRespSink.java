package io.keyforge.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/** A growable in-memory {@link RespSink}. Not thread-safe. */
public final class ByteArrayRespSink implements RespSink {

    private byte[] buffer;
    private int size;

    public ByteArrayRespSink() {
        this(256);
    }

    public ByteArrayRespSink(int initialCapacity) {
        buffer = new byte[Math.max(16, initialCapacity)];
    }

    @Override
    public void writeByte(int value) {
        ensureCapacity(1);
        buffer[size++] = (byte) value;
    }

    @Override
    public void writeBytes(byte[] source, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(source, offset, buffer, size, length);
        size += length;
    }

    public int size() {
        return size;
    }

    /** Forgets the content but keeps the allocated buffer. */
    public void reset() {
        size = 0;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, size);
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(buffer, 0, size);
    }

    private void ensureCapacity(int extra) {
        int required = size + extra;
        if (required < 0) {
            throw new OutOfMemoryError("RESP output larger than 2 GiB");
        }
        if (required > buffer.length) {
            int doubled = buffer.length << 1;
            buffer = Arrays.copyOf(buffer, doubled < 0 ? Integer.MAX_VALUE - 8 : Math.max(doubled, required));
        }
    }
}
