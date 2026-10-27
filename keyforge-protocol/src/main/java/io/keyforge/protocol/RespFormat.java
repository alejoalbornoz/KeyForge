package io.keyforge.protocol;

import java.util.HexFormat;

/** Human-readable rendering of binary data, for logs, test failures and error messages. */
public final class RespFormat {

    private static final int MAX_PRINTED_BYTES = 256;

    private RespFormat() {
    }

    /**
     * Renders bytes the way {@code redis-cli} does: quoted, printable ASCII as-is, everything else escaped. Long
     * values are truncated.
     */
    public static String printable(byte[] bytes) {
        int shown = Math.min(bytes.length, MAX_PRINTED_BYTES);
        StringBuilder sb = new StringBuilder(shown + 2).append('"');
        for (int i = 0; i < shown; i++) {
            int b = bytes[i] & 0xFF;
            switch (b) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (b >= 0x20 && b < 0x7F) {
                        sb.append((char) b);
                    } else {
                        sb.append("\\x").append(HexFormat.of().toHexDigits((byte) b));
                    }
                }
            }
        }
        sb.append('"');
        if (shown < bytes.length) {
            sb.append("...(").append(bytes.length).append(" bytes)");
        }
        return sb.toString();
    }
}
