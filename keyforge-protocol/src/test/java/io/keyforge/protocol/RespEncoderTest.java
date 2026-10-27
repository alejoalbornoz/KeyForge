package io.keyforge.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

class RespEncoderTest {

    @Test
    void encodesSimpleStrings() {
        assertEncodes(RespSimpleString.OK, "+OK\r\n");
        assertEncodes(new RespSimpleString(""), "+\r\n");
    }

    @Test
    void encodesNonAsciiSimpleStringsAsUtf8() {
        assertEncodes(new RespSimpleString("ñandú"), "+ñandú\r\n");
    }

    @Test
    void simpleStringsCannotContainLineBreaks() {
        assertThatThrownBy(() -> new RespSimpleString("a\r\nb")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encodesErrors() {
        assertEncodes(RespError.err("unknown command 'foo'"), "-ERR unknown command 'foo'\r\n");
        assertEncodes(new RespError("WRONGTYPE Operation against a key"), "-WRONGTYPE Operation against a key\r\n");
    }

    @Test
    void errorsReplaceLineBreaksWithSpaces() {
        assertEncodes(RespError.err("bad 'x\r\ny'"), "-ERR bad 'x  y'\r\n");
    }

    @Test
    void encodesIntegers() {
        assertEncodes(RespInteger.of(0), ":0\r\n");
        assertEncodes(RespInteger.of(42), ":42\r\n");
        assertEncodes(RespInteger.of(-7), ":-7\r\n");
        assertEncodes(RespInteger.of(Long.MAX_VALUE), ":9223372036854775807\r\n");
        assertEncodes(RespInteger.of(Long.MIN_VALUE), ":-9223372036854775808\r\n");
    }

    @Test
    void encodesBulkStrings() {
        assertEncodes(RespBulkString.of("hello"), "$5\r\nhello\r\n");
        assertEncodes(RespBulkString.of(""), "$0\r\n\r\n");
        assertEncodes(new RespBulkString(new byte[] {'\r', '\n', 0}), "$3\r\n\r\n\0\r\n");
    }

    @Test
    void encodesNulls() {
        assertEncodes(RespNull.BULK_STRING, "$-1\r\n");
        assertEncodes(RespNull.ARRAY, "*-1\r\n");
    }

    @Test
    void encodesNestedArrays() {
        RespArray value = RespArray.of(
                RespInteger.of(1),
                RespArray.of(RespBulkString.of("a"), RespNull.BULK_STRING),
                RespArray.EMPTY);

        assertEncodes(value, "*3\r\n:1\r\n*2\r\n$1\r\na\r\n$-1\r\n*0\r\n");
    }

    @Test
    void encodesCommandsAsArraysOfBulkStrings() {
        assertThat(RespEncoder.encodeCommand("SET", "k", "v"))
                .isEqualTo(utf8("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"));
    }

    @Test
    void encodedLengthIsExact() {
        List<RespValue> values = List.of(
                RespSimpleString.OK, new RespSimpleString("ñ"), RespError.err("x"), RespInteger.of(-123),
                RespBulkString.of("hello"), RespNull.BULK_STRING, RespNull.ARRAY,
                RespArray.of(RespInteger.of(1), RespArray.of(RespBulkString.of("nested"))));
        for (RespValue value : values) {
            assertThat(RespEncoder.encodedLength(value)).as("%s", value).isEqualTo(RespEncoder.encode(value).length);
        }
    }

    private static void assertEncodes(RespValue value, String expected) {
        assertThat(new String(RespEncoder.encode(value), StandardCharsets.UTF_8)).isEqualTo(expected);
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
