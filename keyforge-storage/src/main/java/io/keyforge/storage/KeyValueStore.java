package io.keyforge.storage;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * A byte-oriented key-value store with optional per-key expiry. The server's command layer is written against this
 * interface only, so engines can be swapped: the durable LSM tree ({@link io.keyforge.storage.lsm.LsmKeyValueStore}) or
 * the in-memory hash table ({@link InMemoryKeyValueStore}).
 *
 * <p><b>Ownership.</b> Keys and values are opaque byte arrays. Arrays passed in belong to the store afterwards, and
 * arrays handed out must not be modified: engines skip defensive copies on the hot path.
 *
 * <p><b>Expiry.</b> An expiry is an absolute wall-clock instant in epoch milliseconds, or {@link #NO_EXPIRY}. Callers turn
 * relative TTLs ({@code EX 10}) into instants before calling the store, so that a replicated write carries the same
 * instant to every replica. A key whose instant is at or before the store clock's current time is logically absent:
 * reads do not return it, conditional writes treat it as missing, and deletes do not count it.
 *
 * <p><b>Consistency.</b> Every method is atomic and linearizable, including the ones that touch several keys: a
 * concurrent {@link #getAll} never observes half of a {@link #putAll}.
 */
public interface KeyValueStore extends AutoCloseable {

    /** Expiry value meaning "never expires". */
    long NO_EXPIRY = 0L;

    /** Returns the value of a live key, or {@code null}. */
    byte[] get(byte[] key);

    /** Returns the values of the given keys in order, with {@code null} for keys that are not live. */
    List<byte[]> getAll(List<byte[]> keys);

    /**
     * Writes a value, replacing any previous value and expiry, if {@code condition} holds.
     *
     * @param expireAtMillis absolute expiry instant in epoch milliseconds, or {@link #NO_EXPIRY}
     * @return whether the write happened
     */
    boolean put(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition);

    /** Writes several keys at once, without expiry. Later duplicates of a key win. */
    void putAll(List<byte[]> keys, List<byte[]> values);

    /**
     * Atomically replaces the value of {@code key} with {@code remapping(current)}, where {@code current} is the live
     * value or {@code null}. An existing expiry is kept; a new key gets none. If the function returns {@code null} the key
     * is removed. If it throws, the store is left unchanged and the exception propagates.
     *
     * <p>The function runs while the key is locked, so it must be fast and must not call back into the store.
     *
     * @return the new value, or {@code null} if the key ended up absent
     */
    byte[] compute(byte[] key, UnaryOperator<byte[]> remapping);

    /** Removes the given keys and returns how many of them were live. */
    int delete(List<byte[]> keys);

    /** Returns how many of the given keys are live. A key listed twice is counted twice. */
    int countExisting(List<byte[]> keys);

    /**
     * Returns the number of stored keys. Like Redis' {@code DBSIZE}, this may include keys that have expired but have not
     * been reclaimed yet.
     */
    long size();

    /** Removes every key. */
    void clear();

    /** Stops background work. The store must not be used afterwards. */
    @Override
    void close();
}
