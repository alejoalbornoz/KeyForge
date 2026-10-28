package io.keyforge.server.command;

import io.keyforge.protocol.RespValue;

/** Runs a command on another node, for commands that raised a {@link RedirectException} here. */
@FunctionalInterface
public interface CommandRouter {

    /**
     * Executes {@code arguments} elsewhere and returns that node's reply.
     *
     * @param target the node suggested by the redirect, or {@code null}
     */
    RespValue forward(byte[][] arguments, String target);
}
