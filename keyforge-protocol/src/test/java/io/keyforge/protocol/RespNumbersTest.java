package io.keyforge.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class RespNumbersTest {

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "-1", "42", "9223372036854775807", "-9223372036854775808"})
    void parsesCanonicalIntegers(String text) {
        assertThat(RespNumbers.parseLong(ascii(text))).isEqualTo(Long.parseLong(text));
    }

    /** Everything Redis' string2ll rejects, even where Java's Long.parseLong would accept it. */
    @ParameterizedTest
    @ValueSource(strings = {"", "-", "+1", "01", "-0", "00", " 1", "1 ", "1.0", "1e3", "abc", "0x10",
            "9223372036854775808", "-9223372036854775809", "123456789012345678901"})
    void rejectsNonCanonicalIntegers(String text) {
        assertThatThrownBy(() -> RespNumbers.parseLong(ascii(text))).isInstanceOf(NumberFormatException.class);
    }

    @Property
    void formattingMatchesTheJdkAndParsesBack(@ForAll long value) {
        byte[] formatted = RespNumbers.toBytes(value);

        assertThat(new String(formatted, StandardCharsets.US_ASCII)).isEqualTo(Long.toString(value));
        assertThat(RespNumbers.length(value)).isEqualTo(formatted.length);
        assertThat(RespNumbers.parseLong(formatted)).isEqualTo(value);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }
}
