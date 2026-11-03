package io.keyforge.server.command;

/**
 * Sees every command the dispatcher executes, for metrics. Called on the thread that ran the command, right after it
 * returned its reply, so an implementation must be cheap and thread-safe. Requests for unknown commands and with the
 * wrong number of arguments are not reported: they never reach a command.
 */
@FunctionalInterface
public interface CommandObserver {

    /** Observes nothing; the dispatcher then does not read the clock either. */
    CommandObserver NONE = (spec, nanos, error) -> { };

    /**
     * @param spec  the command
     * @param nanos time from the dispatch to the reply on this node, forwarding to the leader and waiting for a Raft
     *              commit included
     * @param error whether the reply was an error
     */
    void executed(CommandSpec spec, long nanos, boolean error);
}
