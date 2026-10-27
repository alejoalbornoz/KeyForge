package io.keyforge.server.vthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class ReceiveBufferTest {

    @Test
    void appendsReadsAfterTheUnconsumedBytes() throws IOException {
        ReceiveBuffer buffer = new ReceiveBuffer(16);
        ChunkedInputStream in = new ChunkedInputStream("abcdef", "ghi");

        assertThat(buffer.readFrom(in)).isEqualTo(6);
        consume(buffer, 4);
        assertThat(buffer.readFrom(in)).isEqualTo(3);

        assertThat(unconsumed(buffer)).isEqualTo("efghi");
    }

    @Test
    void compactsInsteadOfGrowingWhenConsumedBytesFreeEnoughRoom() throws IOException {
        ReceiveBuffer buffer = new ReceiveBuffer(8);
        ChunkedInputStream in = new ChunkedInputStream("abcdefgh", "ijklmn");

        buffer.readFrom(in);
        consume(buffer, 6);
        buffer.readFrom(in);

        assertThat(unconsumed(buffer)).isEqualTo("ghijklmn");
        assertThat(buffer.capacity()).isEqualTo(8);
    }

    @Test
    void growsWhenOnePartialRequestFillsIt() throws IOException {
        ReceiveBuffer buffer = new ReceiveBuffer(4);
        ChunkedInputStream in = new ChunkedInputStream("abcd", "efgh", "ijkl");

        buffer.readFrom(in);
        buffer.readFrom(in);
        buffer.readFrom(in);

        assertThat(unconsumed(buffer)).isEqualTo("abcdefghijkl");
        assertThat(buffer.capacity()).isEqualTo(16);
    }

    @Test
    void shrinksBackToItsInitialSizeOnceDrained() throws IOException {
        ReceiveBuffer buffer = new ReceiveBuffer(4);
        ChunkedInputStream in = new ChunkedInputStream("abcd", "efgh", "x");
        buffer.readFrom(in);
        buffer.readFrom(in);
        assertThat(buffer.capacity()).isEqualTo(8);

        consume(buffer, 8);
        buffer.readFrom(in);

        assertThat(buffer.capacity()).isEqualTo(4);
        assertThat(unconsumed(buffer)).isEqualTo("x");
    }

    @Test
    void reportsTheEndOfTheStream() throws IOException {
        ReceiveBuffer buffer = new ReceiveBuffer(4);

        assertThat(buffer.readFrom(new ChunkedInputStream(new byte[0][]))).isEqualTo(-1);
        assertThat(buffer.unconsumed().hasRemaining()).isFalse();
    }

    @Test
    void rejectsANonPositiveInitialCapacity() {
        assertThatThrownBy(() -> new ReceiveBuffer(0)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void consume(ReceiveBuffer buffer, int count) {
        ByteBuffer bytes = buffer.unconsumed();
        bytes.position(bytes.position() + count);
    }

    private static String unconsumed(ReceiveBuffer buffer) {
        ByteBuffer bytes = buffer.unconsumed().duplicate();
        byte[] content = new byte[bytes.remaining()];
        bytes.get(content);
        return new String(content, StandardCharsets.US_ASCII);
    }

    /** Delivers one chunk per {@code read} call, like a socket receiving one segment at a time. */
    static final class ChunkedInputStream extends InputStream {
        private final byte[][] chunks;
        private int next;
        private int offsetInChunk;

        ChunkedInputStream(String... chunks) {
            this.chunks = new byte[chunks.length][];
            for (int i = 0; i < chunks.length; i++) {
                this.chunks[i] = chunks[i].getBytes(StandardCharsets.UTF_8);
            }
        }

        ChunkedInputStream(byte[]... chunks) {
            this.chunks = chunks;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            if (next == chunks.length) {
                return -1;
            }
            byte[] chunk = chunks[next];
            int count = Math.min(length, chunk.length - offsetInChunk);
            System.arraycopy(chunk, offsetInChunk, target, offset, count);
            offsetInChunk += count;
            if (offsetInChunk == chunk.length) {
                next++;
                offsetInChunk = 0;
            }
            return count;
        }
    }
}
