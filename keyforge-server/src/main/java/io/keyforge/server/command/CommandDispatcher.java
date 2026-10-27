package io.keyforge.server.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespValue;

/**
 * Turns a decoded request into a reply: looks the command up, checks its arity and runs it. Transport-agnostic and
 * thread-safe; every server implementation calls it directly on the thread that read the request.
 */
public final class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    /** Redis quotes at most this many bytes of the command name and of its arguments in "unknown command" errors. */
    private static final int MAX_QUOTED_BYTES = 128;

    private final CommandTable commands;

    public CommandDispatcher(CommandTable commands) {
        this.commands = commands;
    }

    /**
     * Executes one request.
     *
     * @param arguments a non-empty request; {@code arguments[0]} is the command name
     */
    public RespValue dispatch(ClientSession session, byte[][] arguments) {
        CommandSpec spec = commands.lookup(arguments[0]);
        if (spec == null) {
            return unknownCommand(arguments);
        }
        if (!spec.acceptsArgumentCount(arguments.length)) {
            return new RespError(Arguments.wrongArity(spec.name()).getMessage());
        }
        try {
            return spec.command().execute(session, arguments);
        } catch (CommandException e) {
            return new RespError(e.getMessage());
        } catch (RuntimeException e) {
            log.error("Command '{}' failed", spec.name(), e);
            return RespError.err("internal error while executing '" + spec.name() + "'");
        }
    }

    public CommandTable commands() {
        return commands;
    }

    /** Same wording as Redis 7: {@code ERR unknown command 'foo', with args beginning with: 'a' 'b' }. */
    static RespError unknownCommand(byte[][] arguments) {
        StringBuilder quoted = new StringBuilder();
        for (int i = 1; i < arguments.length && quoted.length() < MAX_QUOTED_BYTES; i++) {
            int room = MAX_QUOTED_BYTES - quoted.length();
            quoted.append('\'').append(Arguments.truncate(arguments[i], room)).append("' ");
        }
        return RespError.err("unknown command '" + Arguments.truncate(arguments[0], MAX_QUOTED_BYTES)
                + "', with args beginning with: " + quoted);
    }
}
