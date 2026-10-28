package io.keyforge.storage.lsm;

/**
 * One live key and its value, as returned by {@link LsmKeyValueStore#scan}. The arrays belong to the store and must not be
 * modified.
 */
public record KeyValue(byte[] key, byte[] value) {
}
