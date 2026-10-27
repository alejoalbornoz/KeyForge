package io.keyforge.server.command;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;

import io.keyforge.protocol.RespArray;
import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespNumbers;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.WriteCondition;

/** GET, SET, INCR, DECR, MGET and MSET. */
final class StringCommands {

    private static final String OVERFLOW = "ERR increment or decrement would overflow";

    private final KeyValueStore store;
    private final InstantSource clock;

    StringCommands(KeyValueStore store, InstantSource clock) {
        this.store = store;
        this.clock = clock;
    }

    List<CommandSpec> specs() {
        return List.of(
                new CommandSpec("get", 2, (session, args) -> get(args)),
                new CommandSpec("set", -3, (session, args) -> set(args)),
                new CommandSpec("incr", 2, (session, args) -> incrementBy(args[1], 1)),
                new CommandSpec("decr", 2, (session, args) -> incrementBy(args[1], -1)),
                new CommandSpec("mget", -2, (session, args) -> mget(args)),
                new CommandSpec("mset", -3, (session, args) -> mset(args)));
    }

    private RespValue get(byte[][] args) {
        return bulkOrNull(store.get(args[1]));
    }

    /** {@code SET key value [NX | XX] [EX seconds | PX milliseconds]}. */
    private RespValue set(byte[][] args) {
        WriteCondition condition = WriteCondition.ALWAYS;
        byte[] ttl = null;
        boolean ttlInSeconds = false;
        // Parse the flags first, like Redis: a syntax error wins over an invalid number.
        for (int i = 3; i < args.length; i++) {
            byte[] option = args[i];
            boolean hasNext = i + 1 < args.length;
            if (Arguments.is(option, "NX") && condition != WriteCondition.IF_PRESENT) {
                condition = WriteCondition.IF_ABSENT;
            } else if (Arguments.is(option, "XX") && condition != WriteCondition.IF_ABSENT) {
                condition = WriteCondition.IF_PRESENT;
            } else if (Arguments.is(option, "EX") && (ttl == null || ttlInSeconds) && hasNext) {
                ttl = args[++i];
                ttlInSeconds = true;
            } else if (Arguments.is(option, "PX") && (ttl == null || !ttlInSeconds) && hasNext) {
                ttl = args[++i];
                ttlInSeconds = false;
            } else {
                throw new CommandException(Arguments.SYNTAX_ERROR);
            }
        }
        long expireAt = ttl == null ? KeyValueStore.NO_EXPIRY : absoluteExpiry(ttl, ttlInSeconds);
        boolean written = store.put(args[1], args[2], expireAt, condition);
        return written ? RespSimpleString.OK : RespNull.BULK_STRING;
    }

    /**
     * Converts a relative TTL into an absolute epoch-millisecond instant, here at the command layer, so that the instant
     * (not the TTL) is what reaches the store and, later, the replicated log.
     */
    private long absoluteExpiry(byte[] ttlArgument, boolean seconds) {
        long ttl = Arguments.parseLong(ttlArgument);
        if (ttl <= 0 || (seconds && ttl > Long.MAX_VALUE / 1000)) {
            throw invalidExpireTime();
        }
        long ttlMillis = seconds ? ttl * 1000 : ttl;
        long now = clock.millis();
        if (ttlMillis > Long.MAX_VALUE - now) {
            throw invalidExpireTime();
        }
        return now + ttlMillis;
    }

    private RespValue incrementBy(byte[] key, long delta) {
        byte[] updated;
        try {
            updated = store.compute(key, current -> {
                long value = current == null ? 0 : RespNumbers.parseLong(current);
                return RespNumbers.toBytes(Math.addExact(value, delta));
            });
        } catch (NumberFormatException e) {
            throw new CommandException(Arguments.NOT_AN_INTEGER);
        } catch (ArithmeticException e) {
            throw new CommandException(OVERFLOW);
        }
        return RespInteger.of(RespNumbers.parseLong(updated));
    }

    private RespValue mget(byte[][] args) {
        List<byte[]> values = store.getAll(Arguments.tail(args, 1));
        List<RespValue> replies = new ArrayList<>(values.size());
        for (byte[] value : values) {
            replies.add(bulkOrNull(value));
        }
        return new RespArray(replies);
    }

    private RespValue mset(byte[][] args) {
        if (args.length % 2 == 0) {
            throw Arguments.wrongArity("mset");
        }
        int pairs = (args.length - 1) / 2;
        List<byte[]> keys = new ArrayList<>(pairs);
        List<byte[]> values = new ArrayList<>(pairs);
        for (int i = 1; i < args.length; i += 2) {
            keys.add(args[i]);
            values.add(args[i + 1]);
        }
        store.putAll(keys, values);
        return RespSimpleString.OK;
    }

    private static RespValue bulkOrNull(byte[] value) {
        return value == null ? RespNull.BULK_STRING : new RespBulkString(value);
    }

    private static CommandException invalidExpireTime() {
        return new CommandException("ERR invalid expire time in 'set' command");
    }
}
