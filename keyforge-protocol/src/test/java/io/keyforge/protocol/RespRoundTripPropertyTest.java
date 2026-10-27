package io.keyforge.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Size;

/**
 * Encoder and decoders must agree on every value, and decoding must not depend on how the byte stream is split into
 * reads.
 */
class RespRoundTripPropertyTest {

    @Property
    void decodingAnEncodedValueGivesItBack(@ForAll("values") RespValue value) {
        byte[] encoded = RespEncoder.encode(value);
        ByteBuffer buffer = ByteBuffer.wrap(encoded);

        assertThat(new RespDecoder().decode(buffer)).isEqualTo(value);
        assertThat(buffer.hasRemaining()).isFalse();
        assertThat(RespEncoder.encodedLength(value)).isEqualTo(encoded.length);
    }

    @Property
    void valueStreamsDecodeTheSameWhateverTheFragmentation(
            @ForAll("valueStreams") List<RespValue> values,
            @ForAll("chunkSizes") List<Integer> chunkSizes) {
        ByteArrayRespSink sink = new ByteArrayRespSink();
        values.forEach(value -> RespEncoder.encode(value, sink));
        RespDecoder decoder = new RespDecoder();
        List<RespValue> decoded = new ArrayList<>();

        feedInChunks(sink.toByteArray(), chunkSizes, buffer -> {
            RespValue value;
            while ((value = decoder.decode(buffer)) != null) {
                decoded.add(value);
            }
        });

        assertThat(decoded).isEqualTo(values);
    }

    @Property
    void requestStreamsDecodeTheSameWhateverTheFragmentation(
            @ForAll("requestStreams") List<List<byte[]>> requests,
            @ForAll("chunkSizes") List<Integer> chunkSizes) {
        ByteArrayRespSink sink = new ByteArrayRespSink();
        requests.forEach(request -> RespEncoder.encodeCommand(request, sink));
        byte[] stream = sink.toByteArray();
        RespRequestDecoder decoder = new RespRequestDecoder();
        List<byte[][]> decoded = new ArrayList<>();

        feedInChunks(stream, chunkSizes, buffer -> {
            byte[][] request;
            while ((request = decoder.decode(buffer)) != null) {
                decoded.add(request);
            }
        });

        // Encoding is injective, so re-encoding what was decoded must reproduce the original stream exactly.
        ByteArrayRespSink reencoded = new ByteArrayRespSink();
        decoded.forEach(request -> RespEncoder.encodeCommand(Arrays.asList(request), reencoded));
        assertThat(decoded).hasSameSizeAs(requests);
        assertThat(reencoded.toByteArray()).isEqualTo(stream);
        assertThat(decoder.hasPartialRequest()).isFalse();
    }

    /**
     * Delivers {@code stream} the way a socket would: in chunks, into a buffer that keeps the unconsumed tail
     * (compact) between reads.
     */
    private static void feedInChunks(byte[] stream, List<Integer> chunkSizes, Consumer<ByteBuffer> drain) {
        ByteBuffer buffer = ByteBuffer.allocate(stream.length);
        int offset = 0;
        int chunk = 0;
        buffer.flip();
        while (offset < stream.length) {
            int size = Math.min(chunkSizes.get(chunk++ % chunkSizes.size()), stream.length - offset);
            buffer.compact().put(stream, offset, size).flip();
            offset += size;
            drain.accept(buffer);
        }
        assertThat(buffer.hasRemaining()).as("unconsumed bytes at end of stream").isFalse();
    }

    @Provide
    Arbitrary<RespValue> values() {
        return Arbitraries.recursive(
                RespRoundTripPropertyTest::leaves,
                inner -> Arbitraries.oneOf(inner, inner.list().ofMaxSize(4).map(RespArray::new)),
                0, 4);
    }

    @Provide
    Arbitrary<List<RespValue>> valueStreams() {
        return values().list().ofMinSize(1).ofMaxSize(8);
    }

    @Provide
    Arbitrary<List<List<byte[]>>> requestStreams() {
        Arbitrary<byte[]> argument = Arbitraries.oneOf(
                Arbitraries.bytes().array(byte[].class).ofMaxSize(16),
                // Arguments full of protocol characters, to catch any framing that peeks at payload bytes.
                Arbitraries.of((byte) '\r', (byte) '\n', (byte) '$', (byte) '*', (byte) 0)
                        .array(byte[].class).ofMaxSize(8));
        return argument.list().ofMinSize(1).ofMaxSize(6).list().ofMinSize(1).ofMaxSize(8);
    }

    @Provide
    Arbitrary<List<Integer>> chunkSizes() {
        return Arbitraries.integers().between(1, 40).list().ofMinSize(1).ofMaxSize(20);
    }

    private static Arbitrary<RespValue> leaves() {
        Arbitrary<String> lineText = Arbitraries.strings()
                .withCharRange(' ', '~')
                .withChars('\t', 'ñ', '€', 'ж')
                .ofMaxLength(24);
        return Arbitraries.oneOf(
                lineText.map(RespSimpleString::new),
                lineText.map(RespError::new),
                Arbitraries.longs().map(RespInteger::new),
                Arbitraries.bytes().array(byte[].class).ofMaxSize(48).map(RespBulkString::new),
                Arbitraries.of(RespNull.BULK_STRING, RespNull.ARRAY));
    }

    @Property(tries = 50)
    void largeBulkStringsSurviveTinyChunks(@ForAll @Size(min = 1000, max = 5000) List<Byte> payload) {
        byte[] bytes = new byte[payload.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = payload.get(i);
        }
        byte[] stream = RespEncoder.encodeCommand(List.of("SET".getBytes(), "k".getBytes(), bytes));
        RespRequestDecoder decoder = new RespRequestDecoder();
        List<byte[][]> decoded = new ArrayList<>();

        feedInChunks(stream, List.of(7, 1, 3), buffer -> {
            byte[][] request;
            while ((request = decoder.decode(buffer)) != null) {
                decoded.add(request);
            }
        });

        assertThat(decoded).hasSize(1);
        assertThat(decoded.getFirst()[2]).isEqualTo(bytes);
    }
}
