package io.keyforge.protocol;

import java.util.Objects;

/**
 * A RESP error ({@code -ERR message\r\n}). The message starts with an error code such as {@code ERR},
 * {@code WRONGTYPE} or {@code NOPROTO}.
 *
 * <p>Error messages often echo client input (an unknown command name, for example). Like Redis, CR and LF are replaced
 * with spaces, so an error can never break the framing of the reply stream.
 */
public record RespError(String message) implements RespValue {

    public RespError {
        Objects.requireNonNull(message, "message");
        if (message.indexOf('\r') >= 0 || message.indexOf('\n') >= 0) {
            message = message.replace('\r', ' ').replace('\n', ' ');
        }
    }

    /** An error with the generic {@code ERR} code. */
    public static RespError err(String detail) {
        return new RespError("ERR " + detail);
    }
}
