package io.keyforge.server.cluster;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import io.keyforge.protocol.RespNumbers;
import io.keyforge.raft.LogEntry;
import io.keyforge.raft.runtime.StateMachine;
import io.keyforge.server.command.Counters;
import io.keyforge.storage.lsm.LsmKeyValueStore;
import io.keyforge.storage.lsm.LsmKeyValueStore.LogPosition;
import io.keyforge.storage.lsm.LsmKeyValueStore.StoredEntry;

/**
 * The replicated state machine: applies {@link KvCommand}s from committed log entries to the LSM store in replicated
 * mode.
 *
 * <p><b>Determinism.</b> Every replica applies the same entries in order and reaches the same state. Nothing here reads
 * a clock: an entry is applied at its <em>log time</em>, the maximum of the leader-assigned times of all entries up to
 * it. Log time never goes backwards, even across leaders whose clocks disagree, and the store makes every expiry
 * decision of a write (conditions, delete counts, {@code INCR} on an expired key) at that time.
 *
 * <p><b>Durability.</b> Each write carries its log position, which the store keeps atomically with the write and
 * persists when it flushes ({@link LsmKeyValueStore#durablePosition()}): after a crash the runtime replays the log from
 * there. There is no second write-ahead log.
 *
 * <pre>
 * snapshot = [magic:int "KFKV"][version:int][logTime:long] ([1:byte][key][value][expireAt:long])* [0:byte]
 *            key and value as [length:int][bytes]
 * </pre>
 */
public final class KeyValueStateMachine implements StateMachine {

    private static final int SNAPSHOT_MAGIC = 0x4B464B56;
    private static final int SNAPSHOT_VERSION = 1;

    private final LsmKeyValueStore store;
    /** Log time of the last applied entry; only the apply thread touches it. */
    private long logTime;

    public KeyValueStateMachine(LsmKeyValueStore store) {
        this.store = store;
        this.logTime = store.durablePosition().timeMillis();
    }

    @Override
    public long durableAppliedIndex() {
        return store.durablePosition().index();
    }

    @Override
    public Object apply(LogEntry entry) {
        if (entry.kind() == LogEntry.Kind.NOOP) {
            store.markApplied(new LogPosition(entry.index(), logTime));
            return null;
        }
        KvCommand command = KvCommand.decode(entry.data());
        logTime = Math.max(logTime, command.time());
        LogPosition position = new LogPosition(entry.index(), logTime);
        return switch (command) {
            case KvCommand.Set set -> store.put(position, set.key(), set.value(), set.expireAtMillis(),
                    set.condition());
            case KvCommand.SetAll setAll -> {
                store.putAll(position, setAll.keys(), setAll.values());
                yield null;
            }
            case KvCommand.IncrementBy increment -> {
                try {
                    yield RespNumbers.parseLong(store.compute(position, increment.key(),
                            current -> Counters.add(current, increment.delta())));
                } catch (NumberFormatException | ArithmeticException e) {
                    // Rejected the same way on every replica; the store recorded the position and changed nothing.
                    yield new KvCommand.Failure(Counters.errorFor(e));
                }
            }
            case KvCommand.Delete delete -> store.delete(position, delete.keys());
            case KvCommand.Clear _ -> {
                store.clear(position);
                yield null;
            }
        };
    }

    @Override
    public void writeSnapshot(OutputStream out) throws IOException {
        DataOutputStream data = new DataOutputStream(new BufferedOutputStream(out, 1 << 16));
        data.writeInt(SNAPSHOT_MAGIC);
        data.writeInt(SNAPSHOT_VERSION);
        data.writeLong(logTime);
        try {
            store.export(entry -> {
                try {
                    data.writeByte(1);
                    data.writeInt(entry.key().length);
                    data.write(entry.key());
                    data.writeInt(entry.value().length);
                    data.write(entry.value());
                    data.writeLong(entry.expireAtMillis());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        data.writeByte(0);
        data.flush();
    }

    @Override
    public void restoreSnapshot(long index, long term, InputStream in) throws IOException {
        DataInputStream data = new DataInputStream(new BufferedInputStream(in, 1 << 16));
        if (data.readInt() != SNAPSHOT_MAGIC || data.readInt() != SNAPSHOT_VERSION) {
            throw new IOException("not a KeyForge key-value snapshot");
        }
        long snapshotTime = data.readLong();
        Iterator<StoredEntry> entries = new Iterator<>() {
            private StoredEntry next = read();

            private StoredEntry read() {
                try {
                    if (data.readByte() == 0) {
                        return null;
                    }
                    byte[] key = data.readNBytes(data.readInt());
                    byte[] value = data.readNBytes(data.readInt());
                    return new StoredEntry(key, value, data.readLong());
                } catch (IOException e) {
                    throw new UncheckedIOException("damaged snapshot", e);
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public StoredEntry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                StoredEntry current = next;
                next = read();
                return current;
            }
        };
        try {
            store.restore(new LogPosition(index, snapshotTime), entries);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        logTime = snapshotTime;
    }

    /** Log time of the last applied entry. */
    long logTime() {
        return logTime;
    }
}
