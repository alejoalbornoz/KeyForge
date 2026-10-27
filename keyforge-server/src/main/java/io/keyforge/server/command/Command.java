package io.keyforge.server.command;

import io.keyforge.protocol.RespValue;

/** The implementation of one command. */
@FunctionalInterface
public interface Command {

    /**
     * Executes the command. The dispatcher has already checked the argument count against the command's arity.
     *
     * @param arguments the full request; {@code arguments[0]} is the command name as the client sent it
     * @return the reply; errors may be returned as a {@code RespError} or thrown as a {@link CommandException}
     */
    RespValue execute(ClientSession session, byte[][] arguments);
}
