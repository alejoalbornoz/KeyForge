package io.keyforge.server.vthreads;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * The bytes one connection has received but not decoded yet.
 *
 * <p>A virtual thread blocked in {@code read} keeps its buffer for as long as the client is idle, so the buffer starts
 * small: tens of thousands of idle connections must stay cheap. It grows only when a single partial request fills it
 * (a large bulk string; the decoder consumes a bulk payload only once all of it has arrived), and it drops back to the
 * initial size as soon as it is drained.
 *
 * <p>The buffer is always in read mode: {@code [position, limit)} holds the unconsumed bytes. Not thread-safe.
 */
final class ReceiveBuffer {

    /** The largest array most JVMs allocate. */
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;

    private final int initialCapacity;
    private ByteBuffer buffer;

    ReceiveBuffer(int initialCapacity) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
        }
        this.initialCapacity = initialCapacity;
        this.buffer = ByteBuffer.allocate(initialCapacity).flip();
    }

    /** The received bytes not consumed yet. Consume by advancing the position; read nothing beyond the limit. */
    ByteBuffer unconsumed() {
        return buffer;
    }

    /**
     * Blocks until {@code in} delivers bytes, and appends them after the unconsumed ones.
     *
     * @return the number of bytes appended, or -1 at the end of the stream
     * @throws IOException if reading fails, or if a single request would need a buffer larger than 2 GiB
     */
    int readFrom(InputStream in) throws IOException {
        makeRoom();
        int end = buffer.limit();
        int read = in.read(buffer.array(), end, buffer.capacity() - end);
        if (read > 0) {
            buffer.limit(end + read);
        }
        return read;
    }

    int capacity() {
        return buffer.capacity();
    }

    private void makeRoom() throws IOException {
        if (!buffer.hasRemaining()) {
            if (buffer.capacity() > initialCapacity) {
                buffer = ByteBuffer.allocate(initialCapacity);
            }
            buffer.clear().flip();
            return;
        }
        if (buffer.position() > 0) {
            buffer.compact().flip();
        }
        if (buffer.limit() == buffer.capacity()) {
            if (buffer.capacity() == MAX_CAPACITY) {
                throw new IOException("Request larger than " + MAX_CAPACITY + " bytes");
            }
            ByteBuffer larger = ByteBuffer.allocate((int) Math.min(2L * buffer.capacity(), MAX_CAPACITY));
            buffer = larger.put(buffer).flip();
        }
    }
}
