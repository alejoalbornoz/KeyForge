package io.keyforge.storage.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.WriteCondition;

/**
 * A durable {@link KeyValueStore}: a log-structured merge tree with a write-ahead log, a concurrent memtable, immutable
 * SSTables and size-tiered compaction.
 *
 * <p><b>Writes</b> go through one dedicated platform thread, the writer. A caller enqueues its write and parks until the
 * writer completes it; on a virtual thread, parking releases the carrier. The writer takes queued writes in batches.
 * For each write it evaluates conditions ({@code NX}/{@code XX}), {@code compute} functions and delete counts against
 * the latest state, assigns one sequence number to the whole write, appends one WAL record, and inserts the entries into
 * the active memtable. Then it writes the batch's records with one system call, forces them to disk if the
 * {@link FsyncPolicy} is {@code ALWAYS}, publishes the batch's last sequence number, and completes the callers.
 * Serialising writes this way makes every write atomic and puts all of them in one total order, without locks; blocking
 * file I/O never happens on connection threads. Under {@code GROUP_COMMIT} a separate WAL sync thread forces the log
 * every interval or every so many bytes, so a slow fsync (they slow down badly while a flush saturates the disk) never
 * holds up acknowledgements, which do not depend on it.
 *
 * <p><b>Reads</b> take no lock. A read takes a reference to the current {@link Version} and then reads the published
 * sequence number; it ignores memtable entries with a higher sequence number, which belong to writes that are not
 * complete yet. A multi-key read therefore sees a single consistent cut, so a concurrent {@link #getAll} never observes
 * half of a {@link #putAll}.
 *
 * <p><b>Background work.</b> When the active memtable reaches {@link Options#memtableSizeBytes()} the writer starts a
 * new WAL and memtable and hands the old memtable to the flush thread, which writes it to an SSTable, records it in the
 * manifest and deletes the old WAL. If {@value #MAX_IMMUTABLE_MEMTABLES} memtables are already waiting, writes stall
 * until one is flushed. A compaction thread merges runs of similar-sized, age-adjacent tables
 * ({@link SizeTieredCompactionPicker}).
 *
 * <p><b>Recovery.</b> {@link #open} loads the manifest, maps the tables it lists, replays the WALs that are not yet
 * flushed (truncating a torn record at the end of the newest one; see {@link WalReader}), flushes what it replayed into
 * new tables, starts a fresh WAL and deletes files the manifest does not reference.
 *
 * <p><b>Expiry.</b> Values carry their absolute expiry instant. Reads treat an expired value as absent. Flushes and
 * compactions turn expired values into tombstones, and a compaction that includes the oldest table drops both
 * tombstones and expired values, since no older version of the key can exist below it.
 *
 * <p><b>Replicated mode</b> ({@link Options#replicated()}): the store is the state machine of a replicated log (Raft),
 * and the log is the source of durability, so there is no WAL. Each write carries the {@link LogPosition} of the log
 * entry it applies; the store records it atomically with the write, and the manifest persists the position of the
 * newest flushed memtable ({@link #durablePosition()}). After a crash the store holds exactly the entries up to that
 * position and the log replays the rest. Expiry is decided at log time, never with the local clock: conditional writes
 * and read-modify-writes evaluate at the entry's time, and flushes and compactions drop only values expired at the
 * latest applied log time, which every later entry sees as expired too. Reads hide values expired at the later of the
 * wall clock and that log time.
 */
public final class LsmKeyValueStore implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(LsmKeyValueStore.class);

    /** Memtables that may wait for a flush before writes stall. */
    static final int MAX_IMMUTABLE_MEMTABLES = 2;
    /** Most writes the writer takes from the queue into one batch. */
    static final int MAX_WRITES_PER_BATCH = 1024;
    /** The WAL encoding buffer is replaced after a batch that grew it beyond this. */
    private static final int MAX_RETAINED_WAL_BUFFER = 16 << 20;
    /** Entries per write when {@link #restore} loads a snapshot. */
    private static final int RESTORE_BATCH = 1024;

    /**
     * Construction options.
     *
     * @param directory                 where the WAL, SSTables and manifest live; created if missing
     * @param fsyncPolicy               when the WAL is forced to disk
     * @param groupCommitInterval       {@link FsyncPolicy#GROUP_COMMIT}: longest time between two fsyncs while data is
     *                                  waiting
     * @param groupCommitBytes          {@link FsyncPolicy#GROUP_COMMIT}: unsynced bytes that trigger an fsync
     * @param memtableSizeBytes         approximate heap size at which the active memtable is flushed
     * @param compactionMinThreshold    fewest similar-sized tables that get merged
     * @param compactionSmallTableBytes tables smaller than this all count as one size tier
     * @param clock                     wall clock that decides whether a value has expired
     * @param replicated                the store applies a replicated log: no WAL, log positions in the manifest, and
     *                                  expiry decided at log time (see the class comment)
     */
    public record Options(Path directory, FsyncPolicy fsyncPolicy, Duration groupCommitInterval, long groupCommitBytes,
                          long memtableSizeBytes, int compactionMinThreshold, long compactionSmallTableBytes,
                          InstantSource clock, boolean replicated) {

        public static final FsyncPolicy DEFAULT_FSYNC_POLICY = FsyncPolicy.GROUP_COMMIT;
        public static final Duration DEFAULT_GROUP_COMMIT_INTERVAL = Duration.ofMillis(10);
        public static final long DEFAULT_GROUP_COMMIT_BYTES = 1L << 20;
        public static final long DEFAULT_MEMTABLE_SIZE_BYTES = 64L << 20;
        public static final int DEFAULT_COMPACTION_MIN_THRESHOLD = 4;
        public static final long DEFAULT_COMPACTION_SMALL_TABLE_BYTES = 16L << 20;

        public Options {
            Objects.requireNonNull(directory, "directory");
            Objects.requireNonNull(fsyncPolicy, "fsyncPolicy");
            Objects.requireNonNull(groupCommitInterval, "groupCommitInterval");
            Objects.requireNonNull(clock, "clock");
            if (groupCommitInterval.isNegative()) {
                throw new IllegalArgumentException("groupCommitInterval must not be negative");
            }
            if (groupCommitBytes < 1) {
                throw new IllegalArgumentException("groupCommitBytes must be positive");
            }
            if (memtableSizeBytes < 1) {
                throw new IllegalArgumentException("memtableSizeBytes must be positive");
            }
            if (compactionMinThreshold < 2) {
                throw new IllegalArgumentException("compactionMinThreshold must be at least 2");
            }
            if (compactionSmallTableBytes < 0) {
                throw new IllegalArgumentException("compactionSmallTableBytes must not be negative");
            }
        }

        /** A standalone (not replicated) store. */
        public Options(Path directory, FsyncPolicy fsyncPolicy, Duration groupCommitInterval, long groupCommitBytes,
                       long memtableSizeBytes, int compactionMinThreshold, long compactionSmallTableBytes,
                       InstantSource clock) {
            this(directory, fsyncPolicy, groupCommitInterval, groupCommitBytes, memtableSizeBytes,
                    compactionMinThreshold, compactionSmallTableBytes, clock, false);
        }

        public static Options defaults(Path directory) {
            return new Options(directory, DEFAULT_FSYNC_POLICY, DEFAULT_GROUP_COMMIT_INTERVAL,
                    DEFAULT_GROUP_COMMIT_BYTES, DEFAULT_MEMTABLE_SIZE_BYTES, DEFAULT_COMPACTION_MIN_THRESHOLD,
                    DEFAULT_COMPACTION_SMALL_TABLE_BYTES, InstantSource.system());
        }

        public Options withFsyncPolicy(FsyncPolicy newPolicy) {
            return new Options(directory, newPolicy, groupCommitInterval, groupCommitBytes, memtableSizeBytes,
                    compactionMinThreshold, compactionSmallTableBytes, clock, replicated);
        }

        public Options withGroupCommit(Duration newInterval, long newBytes) {
            return new Options(directory, fsyncPolicy, newInterval, newBytes, memtableSizeBytes,
                    compactionMinThreshold, compactionSmallTableBytes, clock, replicated);
        }

        public Options withMemtableSizeBytes(long newSize) {
            return new Options(directory, fsyncPolicy, groupCommitInterval, groupCommitBytes, newSize,
                    compactionMinThreshold, compactionSmallTableBytes, clock, replicated);
        }

        public Options withCompaction(int newMinThreshold, long newSmallTableBytes) {
            return new Options(directory, fsyncPolicy, groupCommitInterval, groupCommitBytes, memtableSizeBytes,
                    newMinThreshold, newSmallTableBytes, clock, replicated);
        }

        public Options withClock(InstantSource newClock) {
            return new Options(directory, fsyncPolicy, groupCommitInterval, groupCommitBytes, memtableSizeBytes,
                    compactionMinThreshold, compactionSmallTableBytes, newClock, replicated);
        }

        public Options withReplicated(boolean newReplicated) {
            return new Options(directory, fsyncPolicy, groupCommitInterval, groupCommitBytes, memtableSizeBytes,
                    compactionMinThreshold, compactionSmallTableBytes, clock, newReplicated);
        }
    }

    /**
     * A position in a replicated log: the index of an entry and its log time (epoch milliseconds), at which the entry's
     * expiry decisions are made. {@link #NONE} is the position of a store that has applied nothing.
     */
    public record LogPosition(long index, long timeMillis) {

        public static final LogPosition NONE = new LogPosition(0, 0);

        public LogPosition {
            if (index < 0 || (index == 0 && timeMillis != 0)) {
                throw new IllegalArgumentException("invalid log position " + index + "@" + timeMillis);
            }
        }
    }

    /** A live key with its value and absolute expiry ({@link #NO_EXPIRY} for none), as snapshots carry it. */
    public record StoredEntry(byte[] key, byte[] value, long expireAtMillis) {
    }

    private final Options options;
    private final DataDirectory directory;
    private final InstantSource clock;
    private final long groupCommitIntervalNanos;
    private final SizeTieredCompactionPicker picker;
    private final AtomicLong nextFileNumber;

    /** Guards installing versions, the manifest and {@link #epoch}; readers never take it. */
    private final ReentrantLock versionLock = new ReentrantLock();
    private final Condition flushWork = versionLock.newCondition();
    private final Condition flushed = versionLock.newCondition();
    private final Condition compactionWork = versionLock.newCondition();
    /** Serialises compactions: the background thread's and {@link #compact()}'s. */
    private final ReentrantLock compactionLock = new ReentrantLock();

    private volatile Version current;
    /** Highest sequence number whose write is complete and visible to readers. */
    private volatile long publishedSequence;
    /** Replicated mode: log time of the newest published write; reads never treat an older time as now. */
    private volatile long publishedLogTime;
    /** Replicated mode: the position the manifest records, that of the newest flushed memtable. */
    private volatile LogPosition durablePosition;
    /** Bumped by {@link #clear()}, so that a flush or compaction that started before it discards its output. */
    private long epoch;
    private boolean compactionRequested = true;

    private final LinkedBlockingQueue<WriteRequest> writeQueue = new LinkedBlockingQueue<>();
    private final Thread writer;
    private final Thread flusher;
    private final Thread compactor;
    /**
     * Forces the WAL under {@link FsyncPolicy#GROUP_COMMIT}; {@code null} under {@link FsyncPolicy#ALWAYS} and in
     * replicated mode.
     */
    private final Thread walSyncer;

    // Writer-thread state; the WAL reference is also read by the WAL sync thread. No WAL in replicated mode.
    private volatile WalWriter wal;
    private long lastSequence;
    private ByteWriter walBuffer = new ByteWriter(64 * 1024);
    private final List<WriteRequest> pending = new ArrayList<>();

    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private volatile boolean closed;
    private volatile boolean stopBackground;
    private volatile boolean stopWalSyncer;
    private volatile boolean writerExited;
    private volatile boolean skipFinalFlush;
    private volatile Throwable failure;

    private LsmKeyValueStore(Options options, DataDirectory directory, Recovered recovered) {
        this.options = options;
        this.directory = directory;
        this.clock = options.clock();
        this.groupCommitIntervalNanos = options.groupCommitInterval().toNanos();
        this.picker = SizeTieredCompactionPicker.withDefaults(options.compactionMinThreshold(),
                options.compactionSmallTableBytes());
        this.nextFileNumber = new AtomicLong(recovered.nextFileNumber());
        this.wal = recovered.wal();
        this.lastSequence = recovered.lastSequence();
        this.publishedSequence = recovered.lastSequence();
        this.durablePosition = recovered.position();
        this.publishedLogTime = recovered.position().timeMillis();
        Memtable active = new Memtable(walNumber());
        active.setPosition(recovered.position());
        this.current = new Version(active, List.of(), recovered.tables());
        recovered.tables().forEach(SSTable::release);

        Map<String, String> mdc = MDC.getCopyOfContextMap();
        writer = newThread("keyforge-lsm-writer", mdc, this::runWriter);
        flusher = newThread("keyforge-lsm-flush", mdc, this::runFlusher);
        compactor = newThread("keyforge-lsm-compaction", mdc, this::runCompactor);
        walSyncer = options.fsyncPolicy() == FsyncPolicy.GROUP_COMMIT && !options.replicated()
                ? newThread("keyforge-lsm-wal-sync", mdc, this::runWalSyncer)
                : null;
        writer.start();
        flusher.start();
        compactor.start();
        if (walSyncer != null) {
            walSyncer.start();
        }
    }

    /**
     * Opens the store in {@link Options#directory()}, creating it if needed and recovering whatever a previous process
     * left there.
     *
     * @throws CorruptionException   if persistent data is damaged beyond a torn WAL tail
     * @throws IllegalStateException if another store has the directory open
     * @throws UncheckedIOException  if the files cannot be read or written
     */
    public static LsmKeyValueStore open(Options options) {
        Objects.requireNonNull(options, "options");
        DataDirectory directory = null;
        try {
            directory = DataDirectory.open(options.directory());
            return new LsmKeyValueStore(options, directory, recover(directory, options));
        } catch (IOException e) {
            closeQuietly(directory);
            throw new UncheckedIOException("Cannot open the store in " + options.directory(), e);
        } catch (RuntimeException | Error e) {
            closeQuietly(directory);
            throw e;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Reads

    @Override
    public byte[] get(byte[] key) {
        Objects.requireNonNull(key, "key");
        Version version = acquireVersion();
        try {
            InternalEntry entry = version.find(key, publishedSequence);
            return entry != null && entry.isLiveAt(readTime()) ? entry.value() : null;
        } finally {
            version.release();
        }
    }

    @Override
    public List<byte[]> getAll(List<byte[]> keys) {
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        Version version = acquireVersion();
        try {
            long snapshot = publishedSequence;
            long now = readTime();
            List<byte[]> values = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                InternalEntry entry = version.find(key, snapshot);
                values.add(entry != null && entry.isLiveAt(now) ? entry.value() : null);
            }
            return values;
        } finally {
            version.release();
        }
    }

    @Override
    public int countExisting(List<byte[]> keys) {
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        Version version = acquireVersion();
        try {
            long snapshot = publishedSequence;
            long now = readTime();
            int existing = 0;
            for (byte[] key : keys) {
                InternalEntry entry = version.find(key, snapshot);
                if (entry != null && entry.isLiveAt(now)) {
                    existing++;
                }
            }
            return existing;
        } finally {
            version.release();
        }
    }

    /**
     * Counts live keys by merging every memtable and table: O(number of stored entries). Expired keys are not counted,
     * which the {@link KeyValueStore#size()} contract allows.
     */
    @Override
    public long size() {
        Version version = acquireVersion();
        try {
            long now = readTime();
            long count = 0;
            Iterator<InternalEntry> entries = version.iterator(null, null, publishedSequence);
            while (entries.hasNext()) {
                if (entries.next().isLiveAt(now)) {
                    count++;
                }
            }
            return count;
        } finally {
            version.release();
        }
    }

    /**
     * Returns up to {@code limit} live keys in {@code [fromInclusive, toExclusive)}, in unsigned lexicographic key order,
     * from one consistent snapshot. A {@code null} bound is open.
     */
    public List<KeyValue> scan(byte[] fromInclusive, byte[] toExclusive, int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        Version version = acquireVersion();
        try {
            long now = readTime();
            List<KeyValue> result = new ArrayList<>(Math.min(limit, 1024));
            Iterator<InternalEntry> entries = version.iterator(fromInclusive, toExclusive, publishedSequence);
            while (result.size() < limit && entries.hasNext()) {
                InternalEntry entry = entries.next();
                if (entry.isLiveAt(now)) {
                    result.add(new KeyValue(entry.key(), entry.value()));
                }
            }
            return result;
        } finally {
            version.release();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Writes

    @Override
    public boolean put(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(condition, "condition");
        return (Boolean) submit(new PutOp(key, value, expireAtMillis, condition));
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values) {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(keys.size() + " keys but " + values.size() + " values");
        }
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        values.forEach(value -> Objects.requireNonNull(value, "value"));
        submit(new PutAllOp(keys, values));
    }

    /** {@inheritDoc} The function runs on the store's writer thread; while it runs, no other write proceeds. */
    @Override
    public byte[] compute(byte[] key, UnaryOperator<byte[]> remapping) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remapping, "remapping");
        return (byte[]) submit(new ComputeOp(key, remapping));
    }

    @Override
    public int delete(List<byte[]> keys) {
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        return (Integer) submit(new DeleteOp(keys));
    }

    /** Removes every key: switches to an empty memtable and WAL and records an empty table list in the manifest. */
    @Override
    public void clear() {
        submit(new ClearOp(null));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Replicated writes: each applies one log entry, decides expiry at the entry's log time, and records its position
    // atomically with its effect.

    /** {@link #put(byte[], byte[], long, WriteCondition)} as the effect of the log entry at {@code position}. */
    public boolean put(LogPosition position, byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(condition, "condition");
        return (Boolean) submit(new PutOp(key, value, expireAtMillis, condition), position);
    }

    /** {@link #putAll(List, List)} as the effect of the log entry at {@code position}. */
    public void putAll(LogPosition position, List<byte[]> keys, List<byte[]> values) {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(keys.size() + " keys but " + values.size() + " values");
        }
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        values.forEach(value -> Objects.requireNonNull(value, "value"));
        submit(new PutAllOp(keys, values), position);
    }

    /** {@link #compute(byte[], UnaryOperator)} as the effect of the log entry at {@code position}. */
    public byte[] compute(LogPosition position, byte[] key, UnaryOperator<byte[]> remapping) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remapping, "remapping");
        return (byte[]) submit(new ComputeOp(key, remapping), position);
    }

    /** {@link #delete(List)} as the effect of the log entry at {@code position}. */
    public int delete(LogPosition position, List<byte[]> keys) {
        keys.forEach(key -> Objects.requireNonNull(key, "key"));
        return (Integer) submit(new DeleteOp(keys), position);
    }

    /** {@link #clear()} as the effect of the log entry at {@code position}; the manifest records it at once. */
    public void clear(LogPosition position) {
        Objects.requireNonNull(position, "position");
        submit(new ClearOp(position), position);
    }

    /** Records the log entry at {@code position} as applied, for entries that change nothing (a leader's no-op). */
    public void markApplied(LogPosition position) {
        submit(new MarkAppliedOp(), position);
    }

    /** Replicated mode: the position of the newest write that survives a crash. */
    public LogPosition durablePosition() {
        return durablePosition;
    }

    /**
     * Passes every live key to {@code sink}, from one consistent snapshot, in key order, and returns the log position
     * that snapshot reflects. Values expired at that position's log time are left out. Meant for the replicated state
     * machine's snapshots, which run while no write is in flight.
     */
    public LogPosition export(Consumer<StoredEntry> sink) {
        Version version = acquireVersion();
        try {
            long snapshot = publishedSequence;
            LogPosition position = version.active.position();
            long now = options.replicated() ? position.timeMillis() : clock.millis();
            Iterator<InternalEntry> entries = version.iterator(null, null, snapshot);
            while (entries.hasNext()) {
                InternalEntry entry = entries.next();
                if (entry.isLiveAt(now)) {
                    sink.accept(new StoredEntry(entry.key(), entry.value(), entry.expireAt()));
                }
            }
            return position;
        } finally {
            version.release();
        }
    }

    /**
     * Replaces the whole contents with {@code entries} (distinct keys), which reflect the log up to {@code position}:
     * clears the store, durably forgetting its position, loads the entries, then records {@code position} and flushes.
     * A crash part-way leaves a store at position zero, which the caller restores again.
     */
    public void restore(LogPosition position, Iterator<StoredEntry> entries) {
        Objects.requireNonNull(position, "position");
        clear(LogPosition.NONE);
        List<StoredEntry> batch = new ArrayList<>(RESTORE_BATCH);
        while (entries.hasNext()) {
            batch.add(entries.next());
            if (batch.size() == RESTORE_BATCH) {
                submit(new LoadOp(List.copyOf(batch)), null);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            submit(new LoadOp(List.copyOf(batch)), null);
        }
        markApplied(position);
        flush();
    }

    /** Flushes the active memtable to an SSTable and waits until no memtable is waiting for a flush. */
    public void flush() {
        submit(new FlushOp());
        versionLock.lock();
        try {
            while (!current.immutables.isEmpty()) {
                ensureUsable();
                flushed.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a flush", e);
        } finally {
            versionLock.unlock();
        }
    }

    /**
     * Flushes, then merges every table into one (a major compaction), dropping tombstones and expired values. Runs on
     * the calling thread.
     */
    public void compact() {
        flush();
        try {
            compactOnce(true);
        } catch (IOException e) {
            fail(e);
            throw new UncheckedIOException("Compaction failed", e);
        } catch (CancellationException e) {
            throw new IllegalStateException("The store closed during the compaction", e);
        }
    }

    /** Stops the background threads after the writes already queued complete. The store must not be used afterwards. */
    @Override
    public void close() {
        if (!closeStarted.compareAndSet(false, true)) {
            return;
        }
        if (options.replicated() && failure == null && !skipFinalFlush) {
            // Without a WAL the memtable would be rebuilt from the log; flushing makes the restart quick.
            try {
                flush();
            } catch (RuntimeException e) {
                log.warn("Could not flush before closing; the log will be replayed instead", e);
            }
        }
        closed = true;
        writeQueue.add(new WriteRequest(new StopOp()));
        join(writer);
        if (walSyncer != null) {
            stopWalSyncer = true;
            LockSupport.unpark(walSyncer);
            join(walSyncer);
        }
        versionLock.lock();
        try {
            stopBackground = true;
            flushWork.signalAll();
            compactionWork.signalAll();
            flushed.signalAll();
        } finally {
            versionLock.unlock();
        }
        join(flusher);
        join(compactor);
        compactionLock.lock();
        compactionLock.unlock();
        versionLock.lock();
        try {
            current.release();
        } finally {
            versionLock.unlock();
        }
        closeQuietly(directory);
        log.info("Closed the LSM store in {}", directory.path());
    }

    // Package-private views for tests.

    /** Closes without the final flush of replicated mode, which loses the memtable as a crash would. */
    void closeWithoutFlush() {
        skipFinalFlush = true;
        close();
    }

    List<TableStats> tableStats() {
        return current.tables.stream().map(SSTable::stats).toList();
    }

    int immutableMemtableCount() {
        return current.immutables.size();
    }

    long publishedSequence() {
        return publishedSequence;
    }

    long unsyncedWalBytes() {
        return wal == null ? 0 : wal.unsyncedBytes();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Write path (writer thread)

    private sealed interface WriteOp {
    }

    private record PutOp(byte[] key, byte[] value, long expireAt, WriteCondition condition) implements WriteOp {
    }

    private record PutAllOp(List<byte[]> keys, List<byte[]> values) implements WriteOp {
    }

    private record ComputeOp(byte[] key, UnaryOperator<byte[]> remapping) implements WriteOp {
    }

    private record DeleteOp(List<byte[]> keys) implements WriteOp {
    }

    private record ClearOp(LogPosition position) implements WriteOp {
    }

    private record MarkAppliedOp() implements WriteOp {
    }

    private record LoadOp(List<StoredEntry> entries) implements WriteOp {
    }

    private record FlushOp() implements WriteOp {
    }

    private record StopOp() implements WriteOp {
    }

    /** A queued write. The writer sets {@code result} or {@code error}, then completes it once the batch is durable. */
    private static final class WriteRequest {
        final WriteOp op;
        /** The log entry this write applies, or {@code null} for a write outside any log. */
        final LogPosition position;
        final CompletableFuture<Object> future = new CompletableFuture<>();
        Object result;
        Throwable error;

        WriteRequest(WriteOp op) {
            this(op, null);
        }

        WriteRequest(WriteOp op, LogPosition position) {
            this.op = op;
            this.position = position;
        }

        void complete() {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        }
    }

    private Object submit(WriteOp op) {
        return submit(op, null);
    }

    private Object submit(WriteOp op, LogPosition position) {
        ensureUsable();
        WriteRequest request = new WriteRequest(op, position);
        writeQueue.add(request);
        // The writer may have exited between the check and the add; then nobody would ever complete the request.
        if (writerExited && writeQueue.remove(request)) {
            ensureUsable();
            throw new IllegalStateException("The store is closed");
        }
        try {
            return request.future.join();
        } catch (CompletionException e) {
            throw rethrow(e.getCause());
        }
    }

    private void runWriter() {
        List<WriteRequest> batch = new ArrayList<>(MAX_WRITES_PER_BATCH);
        try {
            boolean stopping = false;
            while (!stopping) {
                WriteRequest first = nextRequest();
                if (first == null) {
                    continue;
                }
                batch.add(first);
                writeQueue.drainTo(batch, MAX_WRITES_PER_BATCH - 1);
                throwIfFailed();
                for (WriteRequest request : batch) {
                    if (stopping) {
                        request.error = new IllegalStateException("The store is closed");
                        pending.add(request);
                    } else if (request.op instanceof StopOp) {
                        stopping = true;
                        pending.add(request);
                    } else {
                        apply(request);
                    }
                }
                commit();
                batch.clear();
                if (!stopping && current.active.approximateBytes() >= options.memtableSizeBytes()) {
                    rotate();
                }
            }
            if (wal != null) {
                wal.sync();
                wal.close();
            }
        } catch (Throwable t) {
            fail(t);
            Throwable cause = failure;
            for (WriteRequest request : batch) {
                request.future.completeExceptionally(new IllegalStateException("The store failed", cause));
            }
            pending.clear();
            closeQuietly(wal);
        } finally {
            writerExited = true;
            for (WriteRequest request = writeQueue.poll(); request != null; request = writeQueue.poll()) {
                request.future.completeExceptionally(failure != null
                        ? new IllegalStateException("The store failed", failure)
                        : new IllegalStateException("The store is closed"));
            }
        }
    }

    private WriteRequest nextRequest() throws InterruptedException {
        return writeQueue.take();
    }

    private void apply(WriteRequest request) throws IOException, InterruptedException {
        switch (request.op) {
            case ClearOp clear -> {
                commit();
                clearAll(clear.position());
            }
            case FlushOp _ -> {
                commit();
                // An empty memtable is still flushed when it carries a newer log position, to make that durable.
                if (!current.active.isEmpty() || !current.active.position().equals(durablePosition)) {
                    rotate();
                }
            }
            default -> {
                // A logged write decides expiry at its log time; others at the wall clock.
                long now = request.position != null ? request.position.timeMillis() : clock.millis();
                // A failure here (a throwing compute function, a corrupt block) fails this write only.
                try {
                    request.result = switch (request.op) {
                        case PutOp op -> applyPut(op, now);
                        case PutAllOp op -> applyPutAll(op);
                        case ComputeOp op -> applyCompute(op, now);
                        case DeleteOp op -> applyDelete(op, now);
                        case LoadOp op -> applyLoad(op);
                        case MarkAppliedOp _ -> null;
                        default -> throw new IllegalStateException("Unexpected write " + request.op);
                    };
                } catch (RuntimeException e) {
                    request.error = e;
                }
                if (request.position != null) {
                    // In the same memtable as the write, so a flush makes both durable or neither.
                    current.active.setPosition(request.position);
                }
            }
        }
        // Replies wait for the batch's commit, even failures: they may depend on writes earlier in the batch.
        pending.add(request);
    }

    private Object applyPut(PutOp op, long now) {
        if (op.condition() != WriteCondition.ALWAYS) {
            boolean live = latestLive(op.key(), now) != null;
            if (op.condition() == WriteCondition.IF_ABSENT ? live : !live) {
                return Boolean.FALSE;
            }
        }
        long seq = lastSequence + 1;
        append(seq, List.of(InternalEntry.put(op.key(), seq, op.value(), op.expireAt())));
        return Boolean.TRUE;
    }

    private Object applyPutAll(PutAllOp op) {
        TreeMap<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);
        for (int i = 0; i < op.keys().size(); i++) {
            latest.put(op.keys().get(i), op.values().get(i));
        }
        if (latest.isEmpty()) {
            return null;
        }
        long seq = lastSequence + 1;
        List<InternalEntry> entries = new ArrayList<>(latest.size());
        latest.forEach((key, value) -> entries.add(InternalEntry.put(key, seq, value, NO_EXPIRY)));
        append(seq, entries);
        return null;
    }

    private Object applyCompute(ComputeOp op, long now) {
        InternalEntry existing = latestLive(op.key(), now);
        byte[] updated = op.remapping().apply(existing == null ? null : existing.value());
        long seq = lastSequence + 1;
        if (updated == null) {
            if (existing != null) {
                append(seq, List.of(InternalEntry.tombstone(op.key(), seq)));
            }
            return null;
        }
        long expireAt = existing == null ? NO_EXPIRY : existing.expireAt();
        append(seq, List.of(InternalEntry.put(op.key(), seq, updated, expireAt)));
        return updated;
    }

    private Object applyDelete(DeleteOp op, long now) {
        long seq = lastSequence + 1;
        TreeSet<byte[]> seen = new TreeSet<>(Arrays::compareUnsigned);
        List<InternalEntry> tombstones = new ArrayList<>();
        for (byte[] key : op.keys()) {
            if (seen.add(key) && latestLive(key, now) != null) {
                tombstones.add(InternalEntry.tombstone(key, seq));
            }
        }
        if (!tombstones.isEmpty()) {
            append(seq, tombstones);
        }
        return tombstones.size();
    }

    private Object applyLoad(LoadOp op) {
        if (op.entries().isEmpty()) {
            return null;
        }
        long seq = lastSequence + 1;
        List<InternalEntry> entries = new ArrayList<>(op.entries().size());
        for (StoredEntry entry : op.entries()) {
            entries.add(InternalEntry.put(entry.key(), seq, entry.value(), entry.expireAtMillis()));
        }
        append(seq, entries);
        return null;
    }

    /** The newest live version, including writes of the current batch that are not yet published. */
    private InternalEntry latestLive(byte[] key, long now) {
        Version version = acquireVersion();
        try {
            InternalEntry entry = version.find(key, Long.MAX_VALUE);
            return entry != null && entry.isLiveAt(now) ? entry : null;
        } finally {
            version.release();
        }
    }

    /** Encodes one WAL record for a write and inserts its entries into the active memtable, unpublished. */
    private void append(long seq, List<InternalEntry> entries) {
        if (wal != null) {
            WalRecord.encode(walBuffer, seq, entries);
        }
        Memtable active = current.active;
        for (InternalEntry entry : entries) {
            active.add(entry);
        }
        lastSequence = seq;
    }

    /** Writes the batch's WAL records, syncs per policy, publishes the batch and completes its callers. */
    private void commit() throws IOException {
        if (walBuffer.size() > 0) {
            boolean wasClean = wal.unsyncedBytes() == 0;
            wal.write(walBuffer.array(), walBuffer.size());
            walBuffer.reset();
            if (walBuffer.array().length > MAX_RETAINED_WAL_BUFFER) {
                walBuffer = new ByteWriter(64 * 1024);
            }
            if (walSyncer == null) {
                wal.sync();
            } else if (wasClean || walSyncDue(wal)) {
                // The sync thread parks without a deadline while the WAL is clean, and until the deadline otherwise.
                LockSupport.unpark(walSyncer);
            }
        }
        publishedLogTime = current.active.position().timeMillis();
        publishedSequence = lastSequence;
        for (WriteRequest request : pending) {
            request.complete();
        }
        pending.clear();
    }

    private boolean walSyncDue(WalWriter log) {
        long unsynced = log.unsyncedBytes();
        return unsynced > 0 && (unsynced >= options.groupCommitBytes()
                || System.nanoTime() - log.lastSyncNanos() >= groupCommitIntervalNanos);
    }

    /**
     * Group commit: forces the current WAL once {@link Options#groupCommitBytes()} are unsynced or
     * {@link Options#groupCommitInterval()} has passed since the last sync, whichever comes first. Acknowledged writes
     * never wait for this thread.
     */
    private void runWalSyncer() {
        try {
            while (!stopWalSyncer) {
                WalWriter log = wal;
                if (walSyncDue(log)) {
                    log.sync();
                    continue;
                }
                if (log.unsyncedBytes() == 0) {
                    LockSupport.park(this);
                } else {
                    LockSupport.parkNanos(this, groupCommitIntervalNanos - (System.nanoTime() - log.lastSyncNanos()));
                }
            }
        } catch (Throwable t) {
            fail(t);
        }
    }

    /** Makes the active memtable immutable and starts a new memtable and WAL. Stalls while too many await a flush. */
    private void rotate() throws IOException, InterruptedException {
        // A stall can last; under group commit, do not leave acknowledged writes unsynced for that long.
        if (wal != null) {
            wal.sync();
        }
        versionLock.lock();
        try {
            boolean stalled = false;
            while (current.immutables.size() >= MAX_IMMUTABLE_MEMTABLES) {
                throwIfFailed();
                if (!stalled) {
                    log.warn("Writes stalled: {} memtables are waiting to be flushed", current.immutables.size());
                    stalled = true;
                }
                flushed.await();
            }
        } finally {
            versionLock.unlock();
        }
        startNewWal();
        versionLock.lock();
        try {
            Version version = current;
            List<Memtable> immutables = new ArrayList<>(version.immutables.size() + 1);
            immutables.add(version.active);
            immutables.addAll(version.immutables);
            Memtable next = new Memtable(walNumber());
            next.setPosition(version.active.position());
            install(new Version(next, immutables, version.tables));
            flushWork.signal();
        } finally {
            versionLock.unlock();
        }
    }

    /** Empties the store; {@code position} (replicated mode) is the log entry that did it, or none. */
    private void clearAll(LogPosition position) throws IOException {
        startNewWal();
        LogPosition cleared = position != null ? position : LogPosition.NONE;
        versionLock.lock();
        try {
            epoch++;
            directory.writeManifest(new Manifest(nextFileNumber.get(), lastSequence, minWalNumber(), List.of(),
                    cleared.index(), cleared.timeMillis()));
            durablePosition = cleared;
            current.tables.forEach(SSTable::markObsolete);
            Memtable empty = new Memtable(walNumber());
            empty.setPosition(cleared);
            install(new Version(empty, List.of(), List.of()));
            flushed.signalAll();
        } finally {
            versionLock.unlock();
        }
        deleteWalsBelow(minWalNumber());
        log.info("Cleared the store");
    }

    /** The current WAL's number, or 0 when there is no WAL (replicated mode). */
    private long walNumber() {
        return wal == null ? 0 : wal.number();
    }

    /** WALs numbered below this are not needed; without a WAL, none is. */
    private long minWalNumber() {
        return wal == null ? nextFileNumber.get() : wal.number();
    }

    /** Syncs and closes the current WAL and opens the next one; does nothing without a WAL. */
    private void startNewWal() throws IOException {
        if (wal == null) {
            return;
        }
        wal.sync();
        wal.close();
        long number = nextFileNumber.getAndIncrement();
        wal = WalWriter.create(directory.walPath(number), number);
        directory.sync();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Flush and compaction (background threads)

    private void runFlusher() {
        try {
            while (true) {
                Memtable memtable;
                long startEpoch;
                boolean bottom;
                versionLock.lock();
                try {
                    while (!stopBackground && failure == null && current.immutables.isEmpty()) {
                        flushWork.await();
                    }
                    if (stopBackground || failure != null) {
                        return;
                    }
                    memtable = current.immutables.getLast();
                    startEpoch = epoch;
                    // Older versions of these keys can only live in tables; with none, tombstones are not needed.
                    bottom = current.tables.isEmpty();
                } finally {
                    versionLock.unlock();
                }
                flush(memtable, startEpoch, bottom);
            }
        } catch (CancellationException e) {
            // Closing: the memtable's WAL is still there and will be replayed.
        } catch (Throwable t) {
            fail(t);
        }
    }

    private void flush(Memtable memtable, long startEpoch, boolean bottom) throws IOException {
        long started = System.nanoTime();
        long number = nextFileNumber.getAndIncrement();
        // Replicated mode drops only what is expired at the memtable's log time, which every later entry agrees on.
        long now = options.replicated() ? memtable.position().timeMillis() : clock.millis();
        SSTable table = writeTable(directory.tablePath(number), number,
                new MergingIterator(List.of(memtable.iterator(null, null, Long.MAX_VALUE))), memtable.entryCount(),
                bottom, now, () -> stopBackground);
        boolean installed = false;
        long minWal;
        try {
            versionLock.lock();
            try {
                Version version = current;
                if (epoch != startEpoch || !version.immutables.contains(memtable)) {
                    return;
                }
                List<Memtable> immutables = new ArrayList<>(version.immutables);
                immutables.remove(memtable);
                List<SSTable> tables = new ArrayList<>(version.tables.size() + 1);
                if (table != null) {
                    tables.add(table);
                }
                tables.addAll(version.tables);
                Version next = new Version(version.active, immutables, tables);
                LogPosition previous = durablePosition;
                durablePosition = memtable.position();
                try {
                    directory.writeManifest(manifestFor(next));
                } catch (IOException | RuntimeException e) {
                    durablePosition = previous;
                    throw e;
                }
                install(next);
                installed = true;
                minWal = wal == null ? nextFileNumber.get() : next.minWalNumber();
                flushed.signalAll();
                compactionRequested = true;
                compactionWork.signal();
            } finally {
                versionLock.unlock();
            }
        } finally {
            if (table != null) {
                if (!installed) {
                    table.markObsolete();
                }
                table.release();
            }
        }
        deleteWalsBelow(minWal);
        log.debug("Flushed {} memtable entries into {} in {} ms", memtable.entryCount(), table,
                (System.nanoTime() - started) / 1_000_000);
    }

    private void runCompactor() {
        try {
            while (true) {
                versionLock.lock();
                try {
                    while (!stopBackground && failure == null && !compactionRequested) {
                        compactionWork.await();
                    }
                    if (stopBackground || failure != null) {
                        return;
                    }
                    compactionRequested = false;
                } finally {
                    versionLock.unlock();
                }
                while (!stopBackground && compactOnce(false)) {
                    // Keep going while some run of tables qualifies.
                }
            }
        } catch (CancellationException e) {
            // Closing: the inputs are still listed in the manifest; the partial output was deleted.
        } catch (Throwable t) {
            fail(t);
        }
    }

    /** Runs one compaction if one is due ({@code major}: of every table); returns whether it did. */
    private boolean compactOnce(boolean major) throws IOException {
        compactionLock.lock();
        try {
            Version version;
            List<SSTable> inputs;
            boolean bottom;
            long startEpoch;
            versionLock.lock();
            try {
                version = current;
                List<SSTable> tables = version.tables;
                SizeTieredCompactionPicker.Window window = major ? majorWindow(tables) : picker.pick(sizes(tables));
                if (window == null) {
                    return false;
                }
                version.tryRetain();
                inputs = List.copyOf(tables.subList(window.from(), window.to()));
                bottom = window.to() == tables.size();
                startEpoch = epoch;
            } finally {
                versionLock.unlock();
            }
            try {
                return compact(inputs, bottom, startEpoch);
            } finally {
                version.release();
            }
        } finally {
            compactionLock.unlock();
        }
    }

    private boolean compact(List<SSTable> inputs, boolean bottom, long startEpoch) throws IOException {
        long started = System.nanoTime();
        long expectedKeys = 0;
        long inputBytes = 0;
        List<Iterator<InternalEntry>> sources = new ArrayList<>(inputs.size());
        for (SSTable input : inputs) {
            expectedKeys += input.stats().entryCount();
            inputBytes += input.fileSize();
            sources.add(input.iterator(null, null));
        }
        long number = nextFileNumber.getAndIncrement();
        long now = options.replicated() ? current.active.position().timeMillis() : clock.millis();
        SSTable output = writeTable(directory.tablePath(number), number, new MergingIterator(sources), expectedKeys,
                bottom, now, () -> stopBackground);
        boolean installed = false;
        try {
            versionLock.lock();
            try {
                Version version = current;
                int at = indexOfRun(version.tables, inputs);
                if (epoch != startEpoch || at < 0) {
                    return true;
                }
                List<SSTable> tables = new ArrayList<>(version.tables.subList(0, at));
                if (output != null) {
                    tables.add(output);
                }
                tables.addAll(version.tables.subList(at + inputs.size(), version.tables.size()));
                Version next = new Version(version.active, version.immutables, tables);
                directory.writeManifest(manifestFor(next));
                inputs.forEach(SSTable::markObsolete);
                install(next);
                installed = true;
            } finally {
                versionLock.unlock();
            }
        } finally {
            if (output != null) {
                if (!installed) {
                    output.markObsolete();
                }
                output.release();
            }
        }
        log.info("Compacted {} tables ({} bytes{}) into {} in {} ms", inputs.size(), inputBytes,
                bottom ? ", including the oldest" : "", output == null ? "nothing" : output,
                (System.nanoTime() - started) / 1_000_000);
        return true;
    }

    private static SizeTieredCompactionPicker.Window majorWindow(List<SSTable> tables) {
        if (tables.isEmpty()) {
            return null;
        }
        TableStats only = tables.getFirst().stats();
        if (tables.size() == 1 && only.tombstoneCount() == 0 && only.expiringCount() == 0) {
            return null;
        }
        return new SizeTieredCompactionPicker.Window(0, tables.size());
    }

    private static long[] sizes(List<SSTable> tables) {
        long[] sizes = new long[tables.size()];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = tables.get(i).fileSize();
        }
        return sizes;
    }

    /** Where {@code run} sits, by identity and contiguously, in {@code tables}; -1 if it is no longer there. */
    private static int indexOfRun(List<SSTable> tables, List<SSTable> run) {
        int start = tables.indexOf(run.getFirst());
        if (start < 0 || start + run.size() > tables.size()) {
            return -1;
        }
        for (int i = 0; i < run.size(); i++) {
            if (tables.get(start + i) != run.get(i)) {
                return -1;
            }
        }
        return start;
    }

    /**
     * Writes the newest version of each key to a new table and opens it, or returns {@code null} if nothing survived.
     * With {@code bottom} (nothing older exists below these entries) tombstones and expired values are dropped;
     * otherwise expired values become tombstones, which must stay to shadow older versions.
     *
     * @throws CancellationException if {@code stop} turns true; the partial file is deleted
     */
    static SSTable writeTable(Path path, long number, Iterator<InternalEntry> newestPerKey, long expectedKeys,
                              boolean bottom, long now, BooleanSupplier stop) throws IOException {
        try (SSTableWriter writer = new SSTableWriter(path, expectedKeys)) {
            while (newestPerKey.hasNext()) {
                if (stop.getAsBoolean()) {
                    throw new CancellationException("The store is closing");
                }
                InternalEntry entry = newestPerKey.next();
                if (entry.isTombstone() || entry.isExpiredAt(now)) {
                    if (bottom) {
                        continue;
                    }
                    if (!entry.isTombstone()) {
                        entry = InternalEntry.tombstone(entry.key(), entry.seq());
                    }
                }
                writer.add(entry);
            }
            if (writer.entryCount() == 0) {
                return null;
            }
            writer.finish();
        }
        return SSTable.open(path, number);
    }

    private Manifest manifestFor(Version version) {
        LogPosition position = durablePosition;
        return new Manifest(nextFileNumber.get(), publishedSequence,
                wal == null ? nextFileNumber.get() : version.minWalNumber(),
                version.tables.stream().map(Manifest.TableEntry::of).toList(), position.index(),
                position.timeMillis());
    }

    private void deleteWalsBelow(long minWalNumber) {
        try {
            for (long number : directory.walNumbers().headSet(minWalNumber)) {
                directory.deleteIfExists(directory.walPath(number));
            }
        } catch (IOException e) {
            log.warn("Could not delete obsolete WAL files; the next start will skip and delete them", e);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Recovery

    private record Recovered(List<SSTable> tables, WalWriter wal, long lastSequence, long nextFileNumber,
                             LogPosition position) {
    }

    /** Collects replayed WAL records into memtables, starting a new one whenever one fills up. */
    private static final class Replay {
        private final long memtableSizeBytes;
        private final List<Memtable> memtables = new ArrayList<>();
        private Memtable memtable = new Memtable(0);
        private long lastSequence;
        private long records;

        Replay(long memtableSizeBytes) {
            this.memtableSizeBytes = memtableSizeBytes;
        }

        void apply(WalRecord record) {
            for (InternalEntry entry : record.entries()) {
                memtable.add(entry);
            }
            lastSequence = Math.max(lastSequence, record.seq());
            records++;
            if (memtable.approximateBytes() >= memtableSizeBytes) {
                memtables.add(memtable);
                memtable = new Memtable(0);
            }
        }

        /** The filled memtables, oldest first. */
        List<Memtable> finish() {
            if (!memtable.isEmpty()) {
                memtables.add(memtable);
                memtable = new Memtable(0);
            }
            return memtables;
        }
    }

    private static Recovered recover(DataDirectory directory, Options options) throws IOException {
        long started = System.nanoTime();
        List<SSTable> tables = new ArrayList<>();
        WalWriter wal = null;
        try {
            Manifest manifest = directory.readManifest();
            TreeSet<Long> walNumbers = directory.walNumbers();
            TreeSet<Long> tableNumbers = directory.tableNumbers();
            if (manifest == null) {
                if (!walNumbers.isEmpty() || !tableNumbers.isEmpty()) {
                    throw new CorruptionException(directory.path() + " holds WAL or SSTable files but no MANIFEST");
                }
                manifest = new Manifest(1, 0, 1, List.of(), 0, 0);
            }
            for (Manifest.TableEntry entry : manifest.tables()) {
                Path path = directory.tablePath(entry.fileNumber());
                if (!Files.exists(path)) {
                    throw new CorruptionException("The MANIFEST lists " + path + ", which does not exist");
                }
                SSTable table = SSTable.open(path, entry.fileNumber());
                tables.add(table);
                if (table.fileSize() != entry.fileSize() || table.stats().minSeq() != entry.minSeq()
                        || table.stats().maxSeq() != entry.maxSeq()) {
                    throw new CorruptionException(path + " does not match its MANIFEST entry " + entry);
                }
            }

            long nextFileNumber = Math.max(manifest.nextFileNumber(),
                    Math.max(lastOrZero(walNumbers), lastOrZero(tableNumbers)) + 1);
            long lastSequence = manifest.lastSequence();

            List<Long> toReplay = new ArrayList<>(walNumbers.tailSet(manifest.minWalNumber(), true));
            Replay replay = new Replay(options.memtableSizeBytes());
            long truncatedBytes = 0;
            for (int i = 0; i < toReplay.size(); i++) {
                WalReader.Result result = WalReader.replay(directory.walPath(toReplay.get(i)),
                        i == toReplay.size() - 1, replay::apply);
                truncatedBytes += result.truncatedBytes();
            }
            lastSequence = Math.max(lastSequence, replay.lastSequence);
            for (Memtable memtable : replay.finish()) {
                long number = nextFileNumber++;
                SSTable table = writeTable(directory.tablePath(number), number,
                        new MergingIterator(List.of(memtable.iterator(null, null, Long.MAX_VALUE))),
                        memtable.entryCount(), tables.isEmpty(), options.clock().millis(), () -> false);
                if (table != null) {
                    tables.addFirst(table);
                }
            }

            // Replicated mode keeps no WAL: whatever the memtable held is replayed from the replicated log.
            long walNumber = options.replicated() ? 0 : nextFileNumber++;
            if (!options.replicated()) {
                wal = WalWriter.create(directory.walPath(walNumber), walNumber);
                directory.sync();
            }
            LogPosition position = new LogPosition(manifest.appliedIndex(), manifest.appliedTime());
            directory.writeManifest(new Manifest(nextFileNumber, lastSequence,
                    options.replicated() ? nextFileNumber : walNumber,
                    tables.stream().map(Manifest.TableEntry::of).toList(), position.index(), position.timeMillis()));

            for (long number : walNumbers) {
                directory.deleteIfExists(directory.walPath(number));
            }
            Set<Long> live = new HashSet<>();
            tables.forEach(table -> live.add(table.fileNumber()));
            for (long number : directory.tableNumbers()) {
                if (!live.contains(number)) {
                    directory.deleteIfExists(directory.tablePath(number));
                }
            }
            directory.deleteIfExists(directory.path().resolve(DataDirectory.MANIFEST_TEMP));

            log.info("Opened the LSM store in {}: {} tables, {} WAL records replayed{}, last sequence {}{}, {} ms",
                    directory.path(), tables.size(), replay.records,
                    truncatedBytes > 0 ? " (" + truncatedBytes + " torn bytes truncated)" : "", lastSequence,
                    options.replicated() ? ", replicated, applied log index " + position.index() : "",
                    (System.nanoTime() - started) / 1_000_000);
            return new Recovered(List.copyOf(tables), wal, lastSequence, nextFileNumber, position);
        } catch (IOException | RuntimeException | Error e) {
            tables.forEach(SSTable::release);
            closeQuietly(wal);
            throw e;
        }
    }

    private static long lastOrZero(TreeSet<Long> numbers) {
        return numbers.isEmpty() ? 0 : numbers.last();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Plumbing

    /** The time reads decide expiry at: the wall clock, never earlier than the newest applied log time. */
    private long readTime() {
        long now = clock.millis();
        return options.replicated() ? Math.max(now, publishedLogTime) : now;
    }

    private Version acquireVersion() {
        while (true) {
            Version version = current;
            if (version.tryRetain()) {
                return version;
            }
            if (closed) {
                throw new IllegalStateException("The store is closed");
            }
            Thread.onSpinWait();
        }
    }

    /** Replaces the current version; the caller holds {@link #versionLock}. */
    private void install(Version next) {
        Version previous = current;
        current = next;
        previous.release();
    }

    private void fail(Throwable cause) {
        versionLock.lock();
        try {
            if (failure == null) {
                failure = cause;
                log.error("The LSM store failed and accepts no more writes", cause);
            }
            flushed.signalAll();
            flushWork.signalAll();
            compactionWork.signalAll();
        } finally {
            versionLock.unlock();
        }
    }

    private void ensureUsable() {
        if (closed) {
            throw new IllegalStateException("The store is closed");
        }
        throwIfFailed();
    }

    private void throwIfFailed() {
        Throwable cause = failure;
        if (cause != null) {
            throw new IllegalStateException("The store failed and accepts no more writes", cause);
        }
    }

    private static RuntimeException rethrow(Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException(cause);
    }

    private static Thread newThread(String name, Map<String, String> mdc, Runnable body) {
        return Thread.ofPlatform().name(name).daemon(true).unstarted(() -> {
            if (mdc != null) {
                MDC.setContextMap(mdc);
            }
            body.run();
        });
    }

    private static void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for {} to stop", thread.getName());
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Error while closing {}", closeable, e);
        }
    }
}
