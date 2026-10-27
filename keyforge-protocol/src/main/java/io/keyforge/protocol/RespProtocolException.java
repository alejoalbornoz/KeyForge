package io.keyforge.protocol;

/**
 * Thrown by the decoders when the byte stream is not valid RESP. The stream cannot be resynchronised afterwards, so
 * the connection should be closed. A server replies with {@code -ERR Protocol error: <message>} first, as Redis does.
 */
public final class RespProtocolException extends RuntimeException {

    public RespProtocolException(String message) {
        super(message);
    }
}
