package io.keyforge.server.command;

import java.util.List;

import io.keyforge.protocol.RespNumbers;
import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.WriteCondition;

/** The keyspace of a standalone node: the store itself. */
public final class LocalKeyspace implements Keyspace {

    private final KeyValueStore store;

    public LocalKeyspace(KeyValueStore store) {
        this.store = store;
    }

    @Override
    public byte[] get(byte[] key) {
        return store.get(key);
    }

    @Override
    public List<byte[]> getAll(List<byte[]> keys) {
        return store.getAll(keys);
    }

    @Override
    public int countExisting(List<byte[]> keys) {
        return store.countExisting(keys);
    }

    @Override
    public long size() {
        return store.size();
    }

    @Override
    public boolean set(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
        return store.put(key, value, expireAtMillis, condition);
    }

    @Override
    public void setAll(List<byte[]> keys, List<byte[]> values) {
        store.putAll(keys, values);
    }

    @Override
    public long incrementBy(byte[] key, long delta) {
        try {
            return RespNumbers.parseLong(store.compute(key, current -> Counters.add(current, delta)));
        } catch (NumberFormatException | ArithmeticException e) {
            throw new CommandException(Counters.errorFor(e));
        }
    }

    @Override
    public int delete(List<byte[]> keys) {
        return store.delete(keys);
    }

    @Override
    public void clear() {
        store.clear();
    }
}
