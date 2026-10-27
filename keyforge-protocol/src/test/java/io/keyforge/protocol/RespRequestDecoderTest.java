package io.keyforge.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class RespRequestDecoderTest {

    @Test
    void decodesAnArrayOfBulkStrings() {
        assertThat(decodeAll("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")).containsExactly(args("GET", "key"));
    }

    @Test
    void decodesEveryPipelinedRequestInOneBuffer() {
        String pipeline = "*1\r\n$4\r\nPING\r\n"
                + "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
                + "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";

        assertThat(decodeAll(pipeline)).containsExactly(args("PING"), args("SET", "k", "v"), args("GET", "k"));
    }

    @Test
    void bulkStringsAreBinarySafe() {
        byte[] payload = {'a', '\r', '\n', 0, (byte) 0xFF, '$', '*'};
        byte[] request = RespEncoder.encodeCommand(List.of(bytes("SET"), bytes("k"), payload));

        assertThat(decodeAll(request)).containsExactly(new byte[][] {bytes("SET"), bytes("k"), payload});
    }

    @Test
    void acceptsEmptyBulkStrings() {
        assertThat(decodeAll("*2\r\n$4\r\nECHO\r\n$0\r\n\r\n")).containsExactly(args("ECHO", ""));
    }

    @Test
    void resumesWhenARequestIsSplitAtEveryPossibleByte() {
        byte[] stream = ascii("*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\nPING\r\n");
        for (int split = 0; split <= stream.length; split++) {
            RespRequestDecoder decoder = new RespRequestDecoder();
            ByteBuffer buffer = ByteBuffer.allocate(stream.length);
            List<byte[][]> requests = new ArrayList<>();

            buffer.put(stream, 0, split).flip();
            drain(decoder, buffer, requests);
            buffer.compact().put(stream, split, stream.length - split).flip();
            drain(decoder, buffer, requests);

            assertThat(requests).as("split at %d", split).containsExactly(args("SET", "hello", "world"), args("PING"));
            assertThat(buffer.hasRemaining()).isFalse();
        }
    }

    @Test
    void consumesHeadersOfAnIncompleteRequestAndRemembersThem() {
        RespRequestDecoder decoder = new RespRequestDecoder();
        ByteBuffer buffer = ByteBuffer.wrap(ascii("*2\r\n$3\r\nGET\r\n$3\r\nke"));

        assertThat(decoder.decode(buffer)).isNull();
        assertThat(decoder.hasPartialRequest()).isTrue();
        // Everything up to the incomplete payload is consumed; the payload waits until it has fully arrived.
        assertThat(remainingAscii(buffer)).isEqualTo("ke");

        ByteBuffer rest = ByteBuffer.wrap(ascii("key\r\n"));
        assertThat(decoder.decode(rest)).isEqualTo(args("GET", "key"));
        assertThat(decoder.hasPartialRequest()).isFalse();
    }

    @Test
    void decodesInlineCommands() {
        assertThat(decodeAll("SET key value\r\nGET key\nPING\r\n"))
                .containsExactly(args("SET", "key", "value"), args("GET", "key"), args("PING"));
    }

    @Test
    void inlineCommandsCollapseWhitespace() {
        assertThat(decodeAll("  SET \t key   value  \r\n")).containsExactly(args("SET", "key", "value"));
    }

    @Test
    void inlineCommandsSupportQuotesAndEscapes() {
        assertThat(decodeAll("SET \"hello world\" 'it\\'s' \"a\\nb\\x41\\\\\"\r\n"))
                .containsExactly(args("SET", "hello world", "it's", "a\nbA\\"));
    }

    @Test
    void inlineQuotedEmptyArgument() {
        assertThat(decodeAll("ECHO \"\"\r\n")).containsExactly(args("ECHO", ""));
    }

    @Test
    void ignoresBlankInlineLinesAndEmptyMultiBulkRequests() {
        assertThat(decodeAll("\r\n   \r\n*0\r\n*-1\r\nPING\r\n")).containsExactly(args("PING"));
    }

    @Test
    void waitsForTheEndOfAnInlineLine() {
        RespRequestDecoder decoder = new RespRequestDecoder();
        ByteBuffer buffer = ByteBuffer.wrap(ascii("GET ke"));

        assertThat(decoder.decode(buffer)).isNull();
        assertThat(buffer.position()).isZero();
    }

    @Test
    void largeRequestsGrowTheirArgumentArray() {
        int count = 5_000;
        List<byte[]> arguments = new ArrayList<>();
        arguments.add(bytes("MSET"));
        for (int i = 0; i < count; i++) {
            arguments.add(bytes("k" + i));
        }

        byte[][] decoded = new RespRequestDecoder().decode(ByteBuffer.wrap(RespEncoder.encodeCommand(arguments)));

        assertThat(decoded.length).isEqualTo(count + 1);
        assertThat(decoded[count]).isEqualTo(bytes("k" + (count - 1)));
    }

    @Test
    void rejectsAnInvalidMultiBulkLength() {
        assertProtocolError("*abc\r\n", "invalid multibulk length");
        assertProtocolError("*+1\r\n", "invalid multibulk length");
        assertProtocolError("*99999999999\r\n", "invalid multibulk length");
    }

    @Test
    void rejectsArraysOverTheConfiguredLimit() {
        RespRequestDecoder decoder = new RespRequestDecoder(1024, 2);

        assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(ascii("*3\r\n"))))
                .isInstanceOf(RespProtocolException.class)
                .hasMessage("invalid multibulk length");
    }

    @Test
    void rejectsInvalidBulkLengths() {
        assertProtocolError("*1\r\n$-1\r\n", "invalid bulk length");
        assertProtocolError("*1\r\n$x\r\n", "invalid bulk length");

        RespRequestDecoder small = new RespRequestDecoder(4, 16);
        assertThatThrownBy(() -> small.decode(ByteBuffer.wrap(ascii("*1\r\n$5\r\n"))))
                .isInstanceOf(RespProtocolException.class)
                .hasMessage("invalid bulk length");
    }

    @Test
    void rejectsElementsThatAreNotBulkStrings() {
        assertProtocolError("*1\r\n:1\r\n", "expected '$', got ':'");
    }

    @Test
    void rejectsAPayloadNotFollowedByCrlf() {
        assertProtocolError("*1\r\n$3\r\nGETX\r\n", "expected CRLF after bulk string");
    }

    @Test
    void rejectsACarriageReturnWithoutLineFeedInAHeader() {
        assertProtocolError("*1\rX", "expected LF after CR");
    }

    @Test
    void rejectsUnbalancedQuotes() {
        assertProtocolError("SET \"key value\r\n", "unbalanced quotes in request");
        assertProtocolError("SET 'key\r\n", "unbalanced quotes in request");
        assertProtocolError("SET \"key\"value\r\n", "unbalanced quotes in request");
    }

    @Test
    void rejectsOverlongLinesThatNeverEnd() {
        byte[] noNewline = new byte[64 * 1024 + 1];
        Arrays.fill(noNewline, (byte) 'a');
        assertProtocolError(noNewline, "too big inline request");

        byte[] hugeHeader = new byte[64 * 1024 + 2];
        Arrays.fill(hugeHeader, (byte) '1');
        hugeHeader[0] = '*';
        assertProtocolError(hugeHeader, "too big mbulk count string");
    }

    private static void assertProtocolError(String input, String message) {
        assertProtocolError(ascii(input), message);
    }

    private static void assertProtocolError(byte[] input, String message) {
        assertThatThrownBy(() -> decodeAll(input))
                .isInstanceOf(RespProtocolException.class)
                .hasMessage(message);
    }

    private static List<byte[][]> decodeAll(String input) {
        return decodeAll(ascii(input));
    }

    private static List<byte[][]> decodeAll(byte[] input) {
        RespRequestDecoder decoder = new RespRequestDecoder();
        ByteBuffer buffer = ByteBuffer.wrap(input);
        List<byte[][]> requests = new ArrayList<>();
        drain(decoder, buffer, requests);
        assertThat(buffer.hasRemaining()).as("unconsumed input").isFalse();
        return requests;
    }

    private static void drain(RespRequestDecoder decoder, ByteBuffer buffer, List<byte[][]> into) {
        byte[][] request;
        while ((request = decoder.decode(buffer)) != null) {
            into.add(request);
        }
    }

    static byte[][] args(String... arguments) {
        return Arrays.stream(arguments).map(RespRequestDecoderTest::bytes).toArray(byte[][]::new);
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String remainingAscii(ByteBuffer buffer) {
        byte[] rest = new byte[buffer.remaining()];
        buffer.duplicate().get(rest);
        return new String(rest, StandardCharsets.ISO_8859_1);
    }
}
