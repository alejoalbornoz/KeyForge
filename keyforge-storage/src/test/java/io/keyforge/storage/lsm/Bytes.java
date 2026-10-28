package io.keyforge.storage.lsm;

import java.nio.charset.StandardCharsets;

/** String/byte conversions for tests. */
final class Bytes {

    private Bytes() {
    }

    static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
