package io.keyforge.bench;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    private final byte[] value;

    RequestTemplates(int keyspace, int valueSize, double getRatio) {
        this.getRatio = getRatio;
        this.gets = new byte[keyspace][];
        this.sets = new byte[keyspace][];
        this.value = new byte[valueSize];
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

    /** The {@code SET} of key number {@code index}. */
    byte[] set(int index) {
        return sets[index];
    }

    /**
     * One {@code MSET} of keys {@code from} (inclusive) to {@code to} (exclusive), with the same values as their
     * {@code SET}s. Prepopulating with {@code MSET} costs one command, and in a Raft cluster one log entry, per batch.
     */
    byte[] mset(int from, int to) {
        List<byte[]> arguments = new ArrayList<>(1 + 2 * (to - from));
        arguments.add(ascii("MSET"));
        for (int i = from; i < to; i++) {
            arguments.add(key(i));
            arguments.add(value);
        }
        return RespEncoder.encodeCommand(arguments);
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
