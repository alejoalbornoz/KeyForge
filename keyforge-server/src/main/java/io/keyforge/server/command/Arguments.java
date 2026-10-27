package io.keyforge.server.command;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import io.keyforge.protocol.RespNumbers;

/** Helpers for reading command arguments. */
final class Arguments {

    static final String SYNTAX_ERROR = "ERR syntax error";
    static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";

    private Arguments() {
    }

    /** Case-insensitive ASCII comparison with a keyword, without allocating. */
    static boolean is(byte[] argument, String keyword) {
        if (argument.length != keyword.length()) {
            return false;
        }
        for (int i = 0; i < argument.length; i++) {
            if (toLowerCase(argument[i]) != toLowerCase((byte) keyword.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static byte toLowerCase(byte b) {
        return b >= 'A' && b <= 'Z' ? (byte) (b + ('a' - 'A')) : b;
    }

    static long parseLong(byte[] argument) {
        try {
            return RespNumbers.parseLong(argument);
        } catch (NumberFormatException e) {
            throw new CommandException(NOT_AN_INTEGER);
        }
    }

    static String utf8(byte[] argument) {
        return new String(argument, StandardCharsets.UTF_8);
    }

    /** {@code arguments[from..]} as a list view, without copying. */
    static List<byte[]> tail(byte[][] arguments, int from) {
        return Arrays.asList(arguments).subList(from, arguments.length);
    }

    /** Builds a "wrong number of arguments" error exactly as Redis words it. */
    static CommandException wrongArity(String commandName) {
        return new CommandException("ERR wrong number of arguments for '" + commandName + "' command");
    }

    /** Builds the error for an unknown subcommand of a container command such as {@code CONFIG}. */
    static CommandException unknownSubcommand(byte[] subcommand, String container) {
        return new CommandException("ERR unknown subcommand '" + truncate(subcommand, 128)
                + "'. Try " + container + " HELP.");
    }

    /** The first {@code maxBytes} bytes of an argument as text, like C's {@code %.*s}. */
    static String truncate(byte[] argument, int maxBytes) {
        return new String(argument, 0, Math.min(argument.length, Math.max(0, maxBytes)), StandardCharsets.UTF_8);
    }
}
