package io.keyforge.server.command;

import java.util.List;

import io.keyforge.storage.WriteCondition;

/**
 * The data operations the commands perform. On a standalone node it is the store itself ({@link LocalKeyspace}); in
 * a cluster, writes become Raft log entries and reads wait for a ReadIndex confirmation
 * ({@code io.keyforge.server.cluster.ReplicatedKeyspace}).
 *
 * <p>Every operation is atomic and linearizable, as the {@link io.keyforge.storage.KeyValueStore} contract requires.
 * A cluster keyspace on a node that is not the leader throws {@code io.keyforge.raft.runtime.NotLeaderException}, which
 * the dispatcher turns into forwarding.
 */
public interface Keyspace {

    byte[] get(byte[] key);

    List<byte[]> getAll(List<byte[]> keys);

    int countExisting(List<byte[]> keys);

    long size();

    /** {@code SET}: {@code expireAtMillis} is absolute, already computed by the node that received the command. */
    boolean set(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition);

    void setAll(List<byte[]> keys, List<byte[]> values);

    /**
     * {@code INCRBY}: adds {@code delta} to the integer stored at {@code key} (0 if absent) and returns the result.
     *
     * @throws CommandException if the value is not an integer or the result overflows
     */
    long incrementBy(byte[] key, long delta);

    int delete(List<byte[]> keys);

    void clear();
}
