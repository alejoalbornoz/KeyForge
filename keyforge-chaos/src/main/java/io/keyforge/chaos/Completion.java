package io.keyforge.chaos;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;

/**
 * How an operation ended, and the rules that decide it (ADR 0015). A wrong {@code fail} can make a correct system
 * look non-linearizable, so an operation is a failure only when it certainly had no effect:
 *
 * <ul>
 *   <li>the connection could not be established, so nothing was sent;</li>
 *   <li>the server answered {@code CLUSTERDOWN}, which KeyForge only sends when it found no leader to run the
 *       command (ADR 0014).</li>
 * </ul>
 *
 * Everything else that is not a proper reply is {@code info}: timeouts (never failures), broken connections, and every
 * other error reply, including KeyForge's "may or may not have been applied" errors.
 *
 * @param type  {@code ok}, {@code fail} or {@code info}
 * @param value the value a {@code GET} read or an {@code INCR} produced
 * @param error why it failed or is indeterminate
 */
public record Completion(EventType type, Long value, String error) {

    public static Completion ok(Long value) {
        return new Completion(EventType.OK, value, null);
    }

    public static Completion fail(String error) {
        return new Completion(EventType.FAIL, null, error);
    }

    public static Completion info(String error) {
        return new Completion(EventType.INFO, null, error);
    }

    /** The connection could not be opened: the operation was never sent. */
    public static Completion notSent(IOException e) {
        return fail("not sent: " + describe(e));
    }

    /** The request was (perhaps partly) sent but no reply came back. */
    public static Completion noReply(IOException e, Duration timeout) {
        return info(e instanceof SocketTimeoutException
                ? "timed out after " + timeout.toMillis() + " ms"
                : "connection lost: " + describe(e));
    }

    /** Classifies a server reply to {@code function}. */
    public static Completion of(OpFunction function, RespValue reply) {
        if (reply instanceof RespError error) {
            return error.message().startsWith("CLUSTERDOWN") ? fail(error.message()) : info(error.message());
        }
        return switch (function) {
            case GET -> switch (reply) {
                case RespNull _ -> ok(null);
                case RespBulkString bulk -> parse(new String(bulk.value(), StandardCharsets.UTF_8));
                default -> unexpected(reply);
            };
            case SET -> reply instanceof RespSimpleString(String status) && status.equals("OK") ? ok(null)
                    : unexpected(reply);
            case INCR -> reply instanceof RespInteger(long value) ? ok(value) : unexpected(reply);
        };
    }

    private static Completion parse(String text) {
        try {
            return ok(Long.parseLong(text));
        } catch (NumberFormatException e) {
            // The workload only ever writes integers.
            return info("unexpected value '" + text + "'");
        }
    }

    private static Completion unexpected(RespValue reply) {
        return info("unexpected reply " + reply);
    }

    private static String describe(IOException e) {
        return e.getMessage() == null ? e.getClass().getSimpleName()
                : e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
