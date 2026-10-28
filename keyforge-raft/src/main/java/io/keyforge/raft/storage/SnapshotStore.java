package io.keyforge.raft.storage;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.raft.SnapshotPayload;

/**
 * Snapshot files of the state machine. Only the newest is kept.
 *
 * <pre>
 * snapshot-INDEX-TERM.snap   [magic:long "KFSNAP01"][index:long][term:long] body [crc32c(body):int]
 * </pre>
 *
 * <p>A snapshot is written to a temporary file, forced, and renamed into place, so a crash leaves either the old or the
 * new one. The body is whatever the state machine wrote; it travels to followers as an InstallSnapshot payload.
 *
 * <p>Thread-safe: the apply thread writes snapshots, the Raft thread installs them, and the transport reads them.
 */
public final class SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    private static final long MAGIC = 0x4B46534E41503031L; // "KFSNAP01"
    private static final int HEADER_BYTES = 24;
    private static final int TRAILER_BYTES = 4;
    private static final Pattern SNAPSHOT = Pattern.compile("snapshot-(\\d+)-(\\d+)\\.snap");
    private static final AtomicLong TEMP_COUNTER = new AtomicLong();

    /** A snapshot on disk. */
    public record SnapshotFile(long index, long term, Path path, long bodySize) {

        /** The body, as a payload to send to a follower. */
        public SnapshotPayload payload() {
            SnapshotFile file = this;
            return new SnapshotPayload() {
                @Override
                public long size() {
                    return bodySize;
                }

                @Override
                public InputStream open() throws IOException {
                    return openBody(file);
                }

                @Override
                public String toString() {
                    return file.path().getFileName() + " (" + bodySize + " bytes)";
                }
            };
        }
    }

    /** Writes a snapshot body. */
    @FunctionalInterface
    public interface BodyWriter {
        void writeTo(OutputStream out) throws IOException;
    }

    private final Path directory;
    private SnapshotFile latest;

    private SnapshotStore(Path directory, SnapshotFile latest) {
        this.directory = directory;
        this.latest = latest;
    }

    /** Finds the newest valid snapshot in {@code directory} and deletes older ones and leftover temporary files. */
    static SnapshotStore open(Path directory) throws IOException {
        List<SnapshotFile> found = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "snapshot-*")) {
            for (Path file : files) {
                Matcher matcher = SNAPSHOT.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    long size = Files.size(file) - HEADER_BYTES - TRAILER_BYTES;
                    found.add(new SnapshotFile(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)),
                            file, size));
                } else {
                    Files.deleteIfExists(file); // an interrupted write
                }
            }
        }
        found.sort((a, b) -> Long.compare(b.index(), a.index()));
        SnapshotFile latest = found.isEmpty() ? null : found.getFirst();
        if (latest != null) {
            verify(latest);
        }
        for (int i = 1; i < found.size(); i++) {
            Files.deleteIfExists(found.get(i).path());
        }
        return new SnapshotStore(directory, latest);
    }

    public synchronized Optional<SnapshotFile> latest() {
        return Optional.ofNullable(latest);
    }

    /** Writes a snapshot of the entries up to {@code index}, whose term is {@code term}, and makes it the newest. */
    public SnapshotFile write(long index, long term, BodyWriter body) throws IOException {
        Path temporary = directory.resolve("snapshot-" + index + "-" + term + ".tmp" + TEMP_COUNTER.incrementAndGet());
        long size;
        try (OutputStream file = new BufferedOutputStream(Files.newOutputStream(temporary), 1 << 16)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).putLong(MAGIC).putLong(index).putLong(term);
            file.write(header.array());
            CountingCrcStream counting = new CountingCrcStream(file);
            body.writeTo(counting);
            size = counting.count;
            file.write(ByteBuffer.allocate(TRAILER_BYTES).putInt((int) counting.crc.getValue()).array());
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
        Path target = directory.resolve("snapshot-" + index + "-" + term + ".snap");
        DurableFiles.commit(temporary, target);
        return publish(new SnapshotFile(index, term, target, size));
    }

    /** Stores a snapshot received from the leader. */
    public SnapshotFile install(long index, long term, SnapshotPayload payload) throws IOException {
        return write(index, term, out -> {
            try (InputStream in = payload.open()) {
                in.transferTo(out);
            }
        });
    }

    /** Opens the body of a snapshot; the caller closes the stream. */
    public static InputStream openBody(SnapshotFile file) throws IOException {
        InputStream in = Files.newInputStream(file.path());
        try {
            in.skipNBytes(HEADER_BYTES);
        } catch (IOException e) {
            in.close();
            throw e;
        }
        return new BoundedInputStream(in, file.bodySize());
    }

    private synchronized SnapshotFile publish(SnapshotFile written) {
        if (latest == null || written.index() > latest.index()) {
            SnapshotFile previous = latest;
            latest = written;
            if (previous != null && !previous.path().equals(written.path())) {
                deleteQuietly(previous.path());
            }
        } else if (!written.path().equals(latest.path())) {
            deleteQuietly(written.path()); // superseded while it was being written
        }
        return written;
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // On Windows a file being sent to a follower cannot be deleted; the next start removes it.
            log.debug("Could not delete old snapshot {}: {}", path, e.toString());
        }
    }

    private static void verify(SnapshotFile file) throws IOException {
        if (file.bodySize() < 0) {
            throw new StorageCorruptionException(file.path() + " is too short to be a snapshot");
        }
        try (DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(
                Files.newInputStream(file.path()), 1 << 16))) {
            long magic = in.readLong();
            long index = in.readLong();
            long term = in.readLong();
            if (magic != MAGIC || index != file.index() || term != file.term()) {
                throw new StorageCorruptionException(file.path() + " has a bad header");
            }
            CRC32C crc = new CRC32C();
            byte[] chunk = new byte[1 << 16];
            long remaining = file.bodySize();
            while (remaining > 0) {
                int read = in.read(chunk, 0, (int) Math.min(chunk.length, remaining));
                if (read < 0) {
                    throw new StorageCorruptionException(file.path() + " is truncated");
                }
                crc.update(chunk, 0, read);
                remaining -= read;
            }
            if (in.readInt() != (int) crc.getValue()) {
                throw new StorageCorruptionException(file.path() + " fails its checksum");
            }
        }
    }

    /** Counts and checksums the body while passing it through; closing it does not close the file. */
    private static final class CountingCrcStream extends FilterOutputStream {
        final CRC32C crc = new CRC32C();
        long count;

        CountingCrcStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            crc.update(b);
            count++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            out.write(bytes, offset, length);
            crc.update(bytes, offset, length);
            count += length;
        }

        @Override
        public void close() {
            // The snapshot file is closed by the store.
        }
    }

    /** Reads at most {@code limit} bytes of the underlying stream. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = in.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = in.read(bytes, offset, (int) Math.min(length, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
