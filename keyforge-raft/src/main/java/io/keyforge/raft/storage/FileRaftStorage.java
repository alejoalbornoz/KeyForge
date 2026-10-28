package io.keyforge.raft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.raft.LogEntry;
import io.keyforge.raft.PersistentState;
import io.keyforge.raft.RaftAction;

/**
 * Raft's durable state in one directory: the term and vote, the log ({@link SegmentedLog}) and snapshots
 * ({@link SnapshotStore}).
 *
 * <pre>
 * LOCK                     held with an OS file lock while a node uses the directory
 * state                    [magic:int "KFRS"][term:long][votedForLength:int][votedFor utf-8][crc32c:int]
 * log-*.seg                the log
 * snapshot-*.snap          the newest snapshot
 * </pre>
 *
 * <p>The driver mirrors the core's {@link RaftAction.Persist} actions with {@link #apply} and calls {@link #sync} once
 * per batch, before any message that depends on them leaves. The term and vote are written durably right away (a
 * temporary file, forced and renamed); they change only around elections.
 *
 * <p>The log and snapshot actions are called from one thread; {@link #snapshots()} is thread-safe.
 */
public final class FileRaftStorage implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FileRaftStorage.class);

    public static final long DEFAULT_SEGMENT_BYTES = 64L << 20;
    private static final int STATE_MAGIC = 0x4B465253;

    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final SegmentedLog segmentedLog;
    private final SnapshotStore snapshots;
    private final PersistentState recovered;

    private FileRaftStorage(Path directory, FileChannel lockChannel, FileLock lock, SegmentedLog segmentedLog,
                            SnapshotStore snapshots, PersistentState recovered) {
        this.directory = directory;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.segmentedLog = segmentedLog;
        this.snapshots = snapshots;
        this.recovered = recovered;
    }

    /** Opens (creating if needed) and recovers the storage in {@code directory}. */
    public static FileRaftStorage open(Path directory) throws IOException {
        return open(directory, DEFAULT_SEGMENT_BYTES);
    }

    public static FileRaftStorage open(Path directory, long segmentBytes) throws IOException {
        Files.createDirectories(directory);
        FileChannel lockChannel = FileChannel.open(directory.resolve("LOCK"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = lockChannel.tryLock();
        } catch (OverlappingFileLockException e) {
            lock = null;
        }
        if (lock == null) {
            lockChannel.close();
            throw new IllegalStateException("Raft directory " + directory + " is already in use");
        }
        try {
            Files.deleteIfExists(directory.resolve("state.tmp"));
            long[] term = {0};
            String votedFor = readState(directory.resolve("state"), term);
            SnapshotStore snapshots = SnapshotStore.open(directory);
            Optional<SnapshotStore.SnapshotFile> snapshot = snapshots.latest();
            long snapshotIndex = snapshot.map(SnapshotStore.SnapshotFile::index).orElse(0L);
            long snapshotTerm = snapshot.map(SnapshotStore.SnapshotFile::term).orElse(0L);

            SegmentedLog.Loaded loaded = SegmentedLog.load(directory, segmentBytes, snapshotIndex + 1);
            List<LogEntry> all = loaded.entries();
            List<LogEntry> entries = all.stream().filter(entry -> entry.index() > snapshotIndex).toList();
            boolean consistent;
            if (all.isEmpty()) {
                consistent = loaded.log().lastIndex() == snapshotIndex;
            } else if (all.getFirst().index() > snapshotIndex + 1) {
                throw new StorageCorruptionException("the log starts at " + all.getFirst().index()
                        + " but the snapshot ends at " + snapshotIndex);
            } else {
                // A crash between installing a snapshot and resetting the log leaves a log that ends before the
                // snapshot or disagrees with it: finish the reset.
                long lastIndex = all.getLast().index();
                consistent = lastIndex >= snapshotIndex && (snapshotIndex < all.getFirst().index()
                        || all.get((int) (snapshotIndex - all.getFirst().index())).term() == snapshotTerm);
            }
            if (!consistent) {
                log.warn("The log in {} does not continue snapshot {}@{}; discarding it", directory, snapshotIndex,
                        snapshotTerm);
                loaded.log().reset(snapshotIndex);
                loaded.log().sync();
                entries = List.of();
            }
            PersistentState state = new PersistentState(term[0], votedFor, snapshotIndex, snapshotTerm, entries);
            log.info("Opened Raft storage in {}: term {}, voted for {}, snapshot {}@{}, {} log entries", directory,
                    state.term(), state.votedFor(), snapshotIndex, snapshotTerm, entries.size());
            return new FileRaftStorage(directory, lockChannel, lock, loaded.log(), snapshots, state);
        } catch (IOException | RuntimeException e) {
            lock.release();
            lockChannel.close();
            throw e;
        }
    }

    /** The state recovered when the storage was opened. */
    public PersistentState recovered() {
        return recovered;
    }

    public SnapshotStore snapshots() {
        return snapshots;
    }

    public Path directory() {
        return directory;
    }

    /** Applies a persistence action; log changes are durable after the next {@link #sync}. */
    public void apply(RaftAction.Persist action) throws IOException {
        switch (action) {
            case RaftAction.PersistState state -> writeState(state.term(), state.votedFor());
            case RaftAction.AppendLog append -> segmentedLog.append(append.entries());
            case RaftAction.TruncateLog truncate -> segmentedLog.truncateFrom(truncate.fromIndex());
            case RaftAction.CompactLog compact -> segmentedLog.compactThrough(compact.snapshotIndex());
            case RaftAction.RestoreSnapshot restore -> {
                // The snapshot first: should the reset be lost in a crash, recovery finishes it (see open).
                snapshots.install(restore.index(), restore.term(), restore.payload());
                segmentedLog.reset(restore.index());
            }
        }
    }

    /** Makes every log change since the last sync durable. */
    public void sync() throws IOException {
        segmentedLog.sync();
    }

    @Override
    public void close() throws IOException {
        try {
            segmentedLog.close();
        } finally {
            lock.release();
            lockChannel.close();
        }
    }

    private void writeState(long term, String votedFor) throws IOException {
        byte[] vote = votedFor == null ? new byte[0] : votedFor.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(20 + vote.length);
        buffer.putInt(STATE_MAGIC).putLong(term).putInt(votedFor == null ? -1 : vote.length).put(vote);
        buffer.putInt(DurableFiles.crc32c(buffer.array(), 0, buffer.position()));
        Path temporary = directory.resolve("state.tmp");
        Files.write(temporary, buffer.array());
        DurableFiles.commit(temporary, directory.resolve("state"));
    }

    private static String readState(Path file, long[] term) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < 20) {
            throw new StorageCorruptionException(file + " is too short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getInt() != STATE_MAGIC
                || DurableFiles.crc32c(bytes, 0, bytes.length - 4) != buffer.getInt(bytes.length - 4)) {
            throw new StorageCorruptionException(file + " is damaged");
        }
        term[0] = buffer.getLong();
        int length = buffer.getInt();
        if (length < 0) {
            return null;
        }
        if (length != bytes.length - 20) {
            throw new StorageCorruptionException(file + " has a bad vote length");
        }
        return new String(bytes, 16, length, StandardCharsets.UTF_8);
    }
}
