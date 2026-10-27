package io.keyforge.protocol;

import java.util.Objects;

/** A RESP simple string ({@code +OK\r\n}). It cannot contain CR or LF. */
public record RespSimpleString(String value) implements RespValue {

    public static final RespSimpleString OK = new RespSimpleString("OK");
    public static final RespSimpleString PONG = new RespSimpleString("PONG");

    public RespSimpleString {
        Objects.requireNonNull(value, "value");
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("A simple string cannot contain CR or LF: " + value);
        }
    }
}
