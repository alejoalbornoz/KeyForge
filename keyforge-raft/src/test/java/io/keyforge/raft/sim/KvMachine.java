package io.keyforge.raft.sim;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import io.keyforge.raft.LogEntry;

/**
 * The simulated state machine: a small string map. Commands are {@code key=value}. It also keeps a hash of the whole
 * sequence of applied entries, so two machines at the same index can be compared cheaply.
 */
final class KvMachine {

    private final TreeMap<String, String> data;
    private long appliedIndex;
    private long appliedTerm;
    private long sequenceHash;

    KvMachine() {
        this(new TreeMap<>(), 0, 0, 0);
    }

    private KvMachine(TreeMap<String, String> data, long appliedIndex, long appliedTerm, long sequenceHash) {
        this.data = data;
        this.appliedIndex = appliedIndex;
        this.appliedTerm = appliedTerm;
        this.sequenceHash = sequenceHash;
    }

    static byte[] command(String key, String value) {
        return (key + "=" + value).getBytes(StandardCharsets.UTF_8);
    }

    void apply(LogEntry entry) {
        if (entry.index() != appliedIndex + 1) {
            throw new InvariantViolation("state machine at " + appliedIndex + " asked to apply " + entry);
        }
        if (entry.kind() == LogEntry.Kind.COMMAND) {
            String command = new String(entry.data(), StandardCharsets.UTF_8);
            int equals = command.indexOf('=');
            data.put(command.substring(0, equals), command.substring(equals + 1));
        }
        sequenceHash = mix(sequenceHash, entry);
        appliedIndex = entry.index();
        appliedTerm = entry.term();
    }

    static long mix(long hash, LogEntry entry) {
        long h = hash * 0x9E3779B97F4A7C15L + entry.index();
        h = h * 31 + entry.term();
        h = h * 31 + entry.kind().ordinal();
        h = h * 31 + Arrays.hashCode(entry.data());
        return h ^ (h >>> 29);
    }

    String get(String key) {
        return data.get(key);
    }

    long appliedIndex() {
        return appliedIndex;
    }

    long appliedTerm() {
        return appliedTerm;
    }

    long sequenceHash() {
        return sequenceHash;
    }

    long dataHash() {
        return data.hashCode();
    }

    KvMachine copy() {
        return new KvMachine(new TreeMap<>(data), appliedIndex, appliedTerm, sequenceHash);
    }

    byte[] serialize() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(appliedIndex);
            out.writeLong(appliedTerm);
            out.writeLong(sequenceHash);
            out.writeInt(data.size());
            for (Map.Entry<String, String> entry : data.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    static KvMachine deserialize(byte[] snapshot) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            long index = in.readLong();
            long term = in.readLong();
            long hash = in.readLong();
            int size = in.readInt();
            TreeMap<String, String> data = new TreeMap<>();
            for (int i = 0; i < size; i++) {
                data.put(in.readUTF(), in.readUTF());
            }
            return new KvMachine(data, index, term, hash);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
