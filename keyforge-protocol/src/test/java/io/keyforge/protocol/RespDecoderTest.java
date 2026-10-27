package io.keyforge.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class RespDecoderTest {

    @Test
    void decodesEveryValueType() {
        assertThat(decodeAll("+OK\r\n-ERR boom\r\n:-12\r\n$5\r\nhello\r\n$-1\r\n*-1\r\n*0\r\n"))
                .containsExactly(
                        RespSimpleString.OK,
                        RespError.err("boom"),
                        RespInteger.of(-12),
                        RespBulkString.of("hello"),
                        RespNull.BULK_STRING,
                        RespNull.ARRAY,
                        RespArray.EMPTY);
    }

    @Test
    void decodesNestedArrays() {
        assertThat(decodeAll("*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n$1\r\nx\r\n"))
                .containsExactly(RespArray.of(
                        RespArray.of(RespInteger.of(1), RespInteger.of(2)),
                        RespArray.of(RespBulkString.of("x"))));
    }

    @Test
    void resumesAfterEveryPossibleSplit() {
        byte[] stream = ascii("*3\r\n$5\r\nhello\r\n*1\r\n:7\r\n$-1\r\n+PONG\r\n");
        List<RespValue> expected = List.of(
                RespArray.of(RespBulkString.of("hello"), RespArray.of(RespInteger.of(7)), RespNull.BULK_STRING),
                RespSimpleString.PONG);
        for (int split = 0; split <= stream.length; split++) {
            RespDecoder decoder = new RespDecoder();
            ByteBuffer buffer = ByteBuffer.allocate(stream.length);
            List<RespValue> values = new ArrayList<>();

            buffer.put(stream, 0, split).flip();
            drain(decoder, buffer, values);
            buffer.compact().put(stream, split, stream.length - split).flip();
            drain(decoder, buffer, values);

            assertThat(values).as("split at %d", split).isEqualTo(expected);
        }
    }

    @Test
    void rejectsUnknownTypeBytes() {
        assertThatThrownBy(() -> decodeAll("?what\r\n"))
                .isInstanceOf(RespProtocolException.class)
                .hasMessage("unexpected type byte '?'");
    }

    @Test
    void rejectsInvalidIntegers() {
        assertThatThrownBy(() -> decodeAll(":12a\r\n"))
                .isInstanceOf(RespProtocolException.class)
                .hasMessage("invalid integer");
    }

    @Test
    void rejectsBareLineFeedsInsideSimpleStrings() {
        assertThatThrownBy(() -> decodeAll("+a\nb\r\n")).isInstanceOf(RespProtocolException.class);
    }

    private static List<RespValue> decodeAll(String input) {
        RespDecoder decoder = new RespDecoder();
        ByteBuffer buffer = ByteBuffer.wrap(ascii(input));
        List<RespValue> values = new ArrayList<>();
        drain(decoder, buffer, values);
        assertThat(buffer.hasRemaining()).as("unconsumed input").isFalse();
        return values;
    }

    private static void drain(RespDecoder decoder, ByteBuffer buffer, List<RespValue> into) {
        RespValue value;
        while ((value = decoder.decode(buffer)) != null) {
            into.add(value);
        }
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.ISO_8859_1);
    }
}
