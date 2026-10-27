package io.keyforge.protocol;

/**
 * A RESP2 value: what a server sends as a reply, and what the generic {@link RespDecoder} produces.
 *
 * <p>Requests travel through {@link RespRequestDecoder} instead, which yields plain argument arrays and never builds
 * these objects.
 */
public sealed interface RespValue
        permits RespSimpleString, RespError, RespInteger, RespBulkString, RespArray, RespNull {
}
