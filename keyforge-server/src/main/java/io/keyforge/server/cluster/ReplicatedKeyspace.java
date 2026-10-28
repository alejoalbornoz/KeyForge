package io.keyforge.server.cluster;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.keyforge.raft.runtime.NotLeaderException;
import io.keyforge.raft.runtime.OutcomeUnknownException;
import io.keyforge.raft.runtime.RaftRuntime;
import io.keyforge.server.command.CommandException;
import io.keyforge.server.command.Keyspace;
import io.keyforge.server.command.RedirectException;
import io.keyforge.storage.WriteCondition;
import io.keyforge.storage.lsm.LsmKeyValueStore;

/**
 * The keyspace of a cluster node. Writes become log entries: they return once committed and applied here, with the
 * state machine's result. Reads first pass a ReadIndex barrier (the leader confirms it still leads and has applied
 * everything committed before the read), then read the local store: they are linearizable. On a node that is not the
 * leader both raise a {@link RedirectException}, and the dispatcher forwards the command to the leader.
 */
public final class ReplicatedKeyspace implements Keyspace {

    private final RaftRuntime raft;
    private final LsmKeyValueStore store;
    private final InstantSource clock;
    private final long timeoutMillis;

    /**
     * @param clock   the leader's wall clock: it stamps every write with its proposal time
     * @param timeout how long a command waits for its write to commit or its read to be confirmed
     */
    public ReplicatedKeyspace(RaftRuntime raft, LsmKeyValueStore store, InstantSource clock, Duration timeout) {
        this.raft = raft;
        this.store = store;
        this.clock = clock;
        this.timeoutMillis = timeout.toMillis();
    }

    @Override
    public byte[] get(byte[] key) {
        readBarrier();
        return store.get(key);
    }

    @Override
    public List<byte[]> getAll(List<byte[]> keys) {
        readBarrier();
        return store.getAll(keys);
    }

    @Override
    public int countExisting(List<byte[]> keys) {
        readBarrier();
        return store.countExisting(keys);
    }

    @Override
    public long size() {
        readBarrier();
        return store.size();
    }

    @Override
    public boolean set(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
        return (Boolean) propose(new KvCommand.Set(clock.millis(), key, value, expireAtMillis, condition));
    }

    @Override
    public void setAll(List<byte[]> keys, List<byte[]> values) {
        propose(new KvCommand.SetAll(clock.millis(), keys, values));
    }

    @Override
    public long incrementBy(byte[] key, long delta) {
        Object result = propose(new KvCommand.IncrementBy(clock.millis(), key, delta));
        if (result instanceof KvCommand.Failure failure) {
            throw new CommandException(failure.message());
        }
        return (Long) result;
    }

    @Override
    public int delete(List<byte[]> keys) {
        return (Integer) propose(new KvCommand.Delete(clock.millis(), keys));
    }

    @Override
    public void clear() {
        propose(new KvCommand.Clear(clock.millis()));
    }

    private void readBarrier() {
        await(raft.readBarrier(), "confirming leadership for a linearizable read");
    }

    private Object propose(KvCommand command) {
        if (!raft.isLeader()) {
            // Not executed anywhere yet: send the original command to the leader instead.
            throw new RedirectException(raft.leaderId());
        }
        return await(raft.propose(KvCommand.encode(command)), "committing the write (it may still be applied)");
    }

    private <T> T await(CompletableFuture<T> future, String what) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NotLeaderException notLeader) {
                throw new RedirectException(notLeader.leaderHint());
            }
            if (cause instanceof OutcomeUnknownException) {
                throw new CommandException("ERR " + cause.getMessage() + "; the write may or may not be applied");
            }
            throw new CommandException("ERR the Raft node failed while " + what + ": " + cause.getMessage());
        } catch (TimeoutException e) {
            future.cancel(false);
            throw new CommandException("ERR timed out after " + timeoutMillis + " ms " + what);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("ERR interrupted while " + what);
        }
    }
}
