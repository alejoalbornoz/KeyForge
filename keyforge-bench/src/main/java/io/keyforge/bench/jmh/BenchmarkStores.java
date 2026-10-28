package io.keyforge.bench.jmh;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import io.keyforge.storage.lsm.LsmKeyValueStore;

/** Shared setup for the storage benchmarks: temporary directories, keys and preloaded stores. */
final class BenchmarkStores {

    private static final byte[] KEY_PREFIX = "key:".getBytes(StandardCharsets.US_ASCII);
    private static final int KEY_DIGITS = 12;

    private BenchmarkStores() {
    }

    static Path createDirectory() {
        try {
            return Files.createTempDirectory("keyforge-jmh");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void delete(Path directory) {
        try (Stream<Path> files = Files.walk(directory)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@code key:} followed by {@code index} as 12 zero-padded decimal digits, so keys sort in index order and a scan from
     * key {@code i} returns keys {@code i, i+1, ...}. Encoded by hand: {@code String.format} would cost as much as the
     * lookup being measured.
     */
    static byte[] key(long index) {
        byte[] key = new byte[KEY_PREFIX.length + KEY_DIGITS];
        System.arraycopy(KEY_PREFIX, 0, key, 0, KEY_PREFIX.length);
        long remaining = index;
        for (int i = key.length - 1; i >= KEY_PREFIX.length; i--) {
            key[i] = (byte) ('0' + remaining % 10);
            remaining /= 10;
        }
        return key;
    }

    /**
     * A key that is never written but sorts between {@code key(index)} and {@code key(index + 1)}, so a lookup gets past a
     * table's key-range check and has to be answered by the Bloom filter (or the memtable's skip list).
     */
    static byte[] absentKey(long index) {
        byte[] present = key(index);
        byte[] absent = Arrays.copyOf(present, present.length + 1);
        absent[present.length] = '!';
        return absent;
    }

    static byte[] value(int size, long seed) {
        byte[] value = new byte[size];
        for (int i = 0; i < size; i++) {
            value[i] = (byte) ('a' + (seed + i) % 26);
        }
        return value;
    }

    /** Writes keys {@code 0 .. count-1} in batches of 1,000. */
    static void preload(LsmKeyValueStore store, long count, int valueSize) {
        for (long start = 0; start < count; start += 1_000) {
            long end = Math.min(count, start + 1_000);
            List<byte[]> keys = new ArrayList<>((int) (end - start));
            List<byte[]> values = new ArrayList<>((int) (end - start));
            for (long i = start; i < end; i++) {
                keys.add(key(i));
                values.add(value(valueSize, i));
            }
            store.putAll(keys, values);
        }
    }
}
