package io.keyforge.server.command;

/**
 * The command must run on another node: in a cluster, only the leader executes data commands. The dispatcher hands it
 * to its {@link CommandRouter}; the command had no effect here.
 */
public final class RedirectException extends RuntimeException {

    private final String target;

    /** @param target the node that should execute it (the leader this node knows of), or {@code null} if unknown */
    public RedirectException(String target) {
        super(target == null ? "no leader is known" : "redirect to " + target, null, false, false);
        this.target = target;
    }

    public String target() {
        return target;
    }
}
