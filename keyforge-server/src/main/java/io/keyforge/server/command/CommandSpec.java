package io.keyforge.server.command;

import java.util.Locale;
import java.util.Objects;

/**
 * A registered command.
 *
 * @param name    lowercase name, as Redis reports it in errors
 * @param arity   Redis-style arity, counting the command name: {@code n > 0} means exactly {@code n} arguments,
 *                {@code n < 0} means at least {@code -n}
 * @param command the implementation
 */
public record CommandSpec(String name, int arity, Command command) {

    public CommandSpec {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(command, "command");
        if (!name.equals(name.toLowerCase(Locale.ROOT)) || name.chars().anyMatch(c -> c >= 0x80)) {
            throw new IllegalArgumentException("Command names must be lowercase ASCII: " + name);
        }
        if (arity == 0) {
            throw new IllegalArgumentException("arity cannot be 0");
        }
    }

    public boolean acceptsArgumentCount(int count) {
        return arity > 0 ? count == arity : count >= -arity;
    }
}
