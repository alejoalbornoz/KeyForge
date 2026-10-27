package io.keyforge.server.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespSimpleString;

class CommandTableTest {

    private static final Command NOOP = (session, args) -> RespSimpleString.OK;

    @Test
    void looksUpNamesIgnoringAsciiCase() {
        CommandTable table = new CommandTable(List.of(new CommandSpec("get", 2, NOOP), new CommandSpec("set", -3, NOOP)));

        assertThat(table.lookup(b("GET")).name()).isEqualTo("get");
        assertThat(table.lookup(b("gEt")).name()).isEqualTo("get");
        assertThat(table.lookup(b("set")).name()).isEqualTo("set");
        assertThat(table.lookup(b("gett"))).isNull();
        assertThat(table.lookup(b(""))).isNull();
    }

    @Test
    void findsEveryCommandOfALargeTable() {
        List<CommandSpec> specs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            specs.add(new CommandSpec("cmd" + i, 1, NOOP));
        }
        CommandTable table = new CommandTable(specs);

        for (int i = 0; i < 300; i++) {
            assertThat(table.lookup(b("CMD" + i)).name()).isEqualTo("cmd" + i);
        }
        assertThat(table.size()).isEqualTo(300);
    }

    @Test
    void rejectsDuplicatesAndUppercaseNames() {
        assertThatThrownBy(() -> new CommandTable(List.of(new CommandSpec("get", 2, NOOP), new CommandSpec("get", 2, NOOP))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommandSpec("GET", 2, NOOP)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void arityFollowsRedisConventions() {
        CommandSpec exact = new CommandSpec("get", 2, NOOP);
        CommandSpec atLeast = new CommandSpec("del", -2, NOOP);

        assertThat(exact.acceptsArgumentCount(2)).isTrue();
        assertThat(exact.acceptsArgumentCount(3)).isFalse();
        assertThat(atLeast.acceptsArgumentCount(1)).isFalse();
        assertThat(atLeast.acceptsArgumentCount(2)).isTrue();
        assertThat(atLeast.acceptsArgumentCount(10)).isTrue();
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }
}
