package io.keyforge.server.command;

/**
 * A command failure that becomes an error reply. The message is the full error, code included
 * ({@code "ERR syntax error"}). Thrown on ordinary bad input, so it skips the stack trace.
 */
public final class CommandException extends RuntimeException {

    public CommandException(String message) {
        super(message, null, false, false);
    }
}
