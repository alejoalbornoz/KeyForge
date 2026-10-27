package io.keyforge.protocol;

import java.util.List;

/** A RESP array of values. The null array is {@link RespNull#ARRAY}, not a {@code RespArray}. */
public record RespArray(List<RespValue> elements) implements RespValue {

    public static final RespArray EMPTY = new RespArray(List.of());

    public RespArray {
        elements = List.copyOf(elements);
    }

    public static RespArray of(RespValue... elements) {
        return new RespArray(List.of(elements));
    }
}
