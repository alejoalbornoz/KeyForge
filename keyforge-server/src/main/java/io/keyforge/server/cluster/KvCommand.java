package io.keyforge.server.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.keyforge.storage.WriteCondition;

/**
 * A write as it travels in a Raft log entry. The leader builds it from a client command: relative TTLs are already
 * absolute instants, and {@link #time()} is the leader's wall clock when it proposed the write. Every replica applies
 * the entry with those values, so none consults its own clock.
 *
 * <pre>
 * entry = [type:byte][time:long] fields
 * SET    [keyLength:int][key][valueLength:int][value][expireAt:long][condition:byte]
 * MSET   [count:int] ([keyLength:int][key][valueLength:int][value])*
 * INCRBY [keyLength:int][key][delta:long]
 * DEL    [count:int] ([keyLength:int][key])*
 * CLEAR
 * </pre>
 */
public sealed interface KvCommand {

    /** The leader's wall clock, in epoch milliseconds, when it proposed the command. */
    long time();

    record Set(long time, byte[] key, byte[] value, long expireAtMillis, WriteCondition condition)
            implements KvCommand {
        public Set {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(condition, "condition");
        }
    }

    record SetAll(long time, List<byte[]> keys, List<byte[]> values) implements KvCommand {
        public SetAll {
            if (keys.size() != values.size()) {
                throw new IllegalArgumentException(keys.size() + " keys but " + values.size() + " values");
            }
            keys = List.copyOf(keys);
            values = List.copyOf(values);
        }
    }

    record IncrementBy(long time, byte[] key, long delta) implements KvCommand {
        public IncrementBy {
            Objects.requireNonNull(key, "key");
        }
    }

    record Delete(long time, List<byte[]> keys) implements KvCommand {
        public Delete {
            keys = List.copyOf(keys);
        }
    }

    record Clear(long time) implements KvCommand {
    }

    /** A command the state machine rejected deterministically, such as INCR on a non-integer: the Redis error. */
    record Failure(String message) {
    }

    byte SET = 1;
    byte SET_ALL = 2;
    byte INCREMENT_BY = 3;
    byte DELETE = 4;
    byte CLEAR = 5;

    static byte[] encode(KvCommand command) {
        int size = 9 + switch (command) {
            case Set set -> 4 + set.key().length + 4 + set.value().length + 9;
            case SetAll setAll -> 4 + bytes(setAll.keys()) + bytes(setAll.values()) + 8 * setAll.keys().size();
            case IncrementBy increment -> 4 + increment.key().length + 8;
            case Delete delete -> 4 + bytes(delete.keys()) + 4 * delete.keys().size();
            case Clear _ -> 0;
        };
        ByteBuffer out = ByteBuffer.allocate(size);
        switch (command) {
            case Set set -> {
                out.put(SET).putLong(set.time());
                putBytes(out, set.key());
                putBytes(out, set.value());
                out.putLong(set.expireAtMillis()).put((byte) set.condition().ordinal());
            }
            case SetAll setAll -> {
                out.put(SET_ALL).putLong(setAll.time()).putInt(setAll.keys().size());
                for (int i = 0; i < setAll.keys().size(); i++) {
                    putBytes(out, setAll.keys().get(i));
                    putBytes(out, setAll.values().get(i));
                }
            }
            case IncrementBy increment -> {
                out.put(INCREMENT_BY).putLong(increment.time());
                putBytes(out, increment.key());
                out.putLong(increment.delta());
            }
            case Delete delete -> {
                out.put(DELETE).putLong(delete.time()).putInt(delete.keys().size());
                delete.keys().forEach(key -> putBytes(out, key));
            }
            case Clear clear -> out.put(CLEAR).putLong(clear.time());
        }
        return out.array();
    }

    static KvCommand decode(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        byte type = in.get();
        long time = in.getLong();
        KvCommand command = switch (type) {
            case SET -> {
                byte[] key = getBytes(in);
                byte[] value = getBytes(in);
                long expireAt = in.getLong();
                yield new Set(time, key, value, expireAt, WriteCondition.values()[in.get()]);
            }
            case SET_ALL -> {
                int count = in.getInt();
                List<byte[]> keys = new ArrayList<>(count);
                List<byte[]> values = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    keys.add(getBytes(in));
                    values.add(getBytes(in));
                }
                yield new SetAll(time, keys, values);
            }
            case INCREMENT_BY -> new IncrementBy(time, getBytes(in), in.getLong());
            case DELETE -> {
                int count = in.getInt();
                List<byte[]> keys = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    keys.add(getBytes(in));
                }
                yield new Delete(time, keys);
            }
            case CLEAR -> new Clear(time);
            default -> throw new IllegalArgumentException("unknown command type " + type);
        };
        if (in.hasRemaining()) {
            throw new IllegalArgumentException(in.remaining() + " trailing bytes after " + command.getClass()
                    .getSimpleName());
        }
        return command;
    }

    private static int bytes(List<byte[]> arrays) {
        int total = 0;
        for (byte[] array : arrays) {
            total += array.length;
        }
        return total;
    }

    private static void putBytes(ByteBuffer out, byte[] bytes) {
        out.putInt(bytes.length).put(bytes);
    }

    private static byte[] getBytes(ByteBuffer in) {
        byte[] bytes = new byte[in.getInt()];
        in.get(bytes);
        return bytes;
    }
}
