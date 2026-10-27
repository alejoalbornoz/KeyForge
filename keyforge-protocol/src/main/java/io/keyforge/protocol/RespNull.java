package io.keyforge.protocol;

/** The two RESP2 null values: the null bulk string ({@code $-1\r\n}) and the null array ({@code *-1\r\n}). */
public enum RespNull implements RespValue {
    BULK_STRING,
    ARRAY
}
