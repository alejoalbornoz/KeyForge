package io.keyforge.bench;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.random.RandomGenerator;

import io.keyforge.protocol.RespEncoder;

/**
 * Every request a run can send, encoded once up front: a {@code GET} and a {@code SET} per key. Keys are
 * {@code key:0000000} to {@code key:<keyspace-1>}; values are {@code valueSize} bytes of {@code 'v'}.
 */
final class RequestTemplates {

    private final byte[][] gets;
    private final byte[][] sets;
    private final double getRatio;

    RequestTemplates(int keyspace, int valueSize, double getRatio) {
        this.getRatio = getRatio;
        this.gets = new byte[keyspace][];
        this.sets = new byte[keyspace][];
        byte[] value = new byte[valueSize];
        Arrays.fill(value, (byte) 'v');
        byte[] get = ascii("GET");
        byte[] set = ascii("SET");
        for (int i = 0; i < keyspace; i++) {
            byte[] key = key(i);
            gets[i] = RespEncoder.encodeCommand(List.of(get, key));
            sets[i] = RespEncoder.encodeCommand(List.of(set, key, value));
        }
    }

    /** A random request: a {@code GET} with probability {@code getRatio}, else a {@code SET}, of a random key. */
    byte[] next(RandomGenerator random) {
        int key = random.nextInt(gets.length);
        return random.nextDouble() < getRatio ? gets[key] : sets[key];
    }

    /** The {@code SET} of key number {@code index}, used to prepopulate the keyspace. */
    byte[] set(int index) {
        return sets[index];
    }

    int keyspace() {
        return gets.length;
    }

    static byte[] key(int index) {
        return ascii(String.format("key:%07d", index));
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }
}
