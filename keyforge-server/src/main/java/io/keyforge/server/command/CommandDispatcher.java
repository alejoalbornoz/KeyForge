package io.keyforge.server.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespValue;

/**
 * Turns a decoded request into a reply: looks the command up, checks its arity and runs it. Transport-agnostic and
 * thread-safe; every server implementation calls it directly on the thread that read the request.
 *
 * <p>In a cluster, a data command on a node that is not the leader raises a {@link RedirectException}; the dispatcher
 * then hands the request to its {@link CommandRouter}, which runs it on the leader and returns the leader's reply.
 */
public final class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    /** Redis quotes at most this many bytes of the command name and of its arguments in "unknown command" errors. */
    private static final int MAX_QUOTED_BYTES = 128;

    private final CommandTable commands;
    private final CommandRouter router;

    public CommandDispatcher(CommandTable commands) {
        this(commands, null);
    }

    /** @param router where redirected commands go; {@code null} on a standalone node */
    public CommandDispatcher(CommandTable commands, CommandRouter router) {
        this.commands = commands;
        this.router = router;
    }

    /**
     * Executes one request, forwarding it if it must run on another node.
     *
     * @param arguments a non-empty request; {@code arguments[0]} is the command name
     */
    public RespValue dispatch(ClientSession session, byte[][] arguments) {
        return execute(session, arguments, true);
    }

    /**
     * Executes one request here, never forwarding it: a {@link RedirectException} propagates. For requests another node
     * forwarded to this one, so that a request never travels in circles.
     */
    public RespValue dispatchLocally(ClientSession session, byte[][] arguments) {
        return execute(session, arguments, false);
    }

    private RespValue execute(ClientSession session, byte[][] arguments, boolean mayForward) {
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
        } catch (RedirectException e) {
            if (!mayForward) {
                throw e;
            }
            if (router == null) {
                return RespError.err("this node cannot execute '" + spec.name() + "' and has nowhere to send it");
            }
            return router.forward(arguments, e.target());
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
