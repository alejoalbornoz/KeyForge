package io.keyforge.raft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.raft.LogEntry;

/**
 * The durable Raft log: a sequence of segment files, each named after the index of its first entry.
 *
 * <pre>
 * log-NNNNNNNNNNNNNNNNNNNN.seg   record*
 * record  = [length:int][crc32c(payload):int][payload]          big-endian
 * payload = [index:long][term:long][kind:byte][data]
 * </pre>
 *
 * <p>Appends go to the last segment; once it passes the size limit, it is forced to disk and a new one starts. Writes
 * become durable at {@link #sync()}. Truncating a suffix deletes later segments first and then cuts the file, so a crash
 * in between leaves the old suffix, which the leader overwrites again. Compaction deletes whole segments the snapshot
 * covers. On load, a record torn by a crash at the very end of the last segment is truncated; damage anywhere else is a
 * {@link StorageCorruptionException}.
 *
 * <p>Used by one thread at a time.
 */
final class SegmentedLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SegmentedLog.class);

    private static final Pattern SEGMENT = Pattern.compile("log-(\\d{20})\\.seg");
    private static final int HEADER_BYTES = 8;
    private static final int PAYLOAD_FIXED_BYTES = 17;

    private final Path directory;
    private final long segmentBytes;
    private final List<Segment> segments = new ArrayList<>();
    private ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
    /** A truncation or a new file awaits a metadata sync. */
    private boolean metadataDirty;
    private boolean dataDirty;

    /** One segment file and the byte offset of each of its records. */
    private static final class Segment {
        final long firstIndex;
        final Path path;
        long[] offsets = new long[64];
        int count;
        long size;
        FileChannel channel;

        Segment(long firstIndex, Path path) {
            this.firstIndex = firstIndex;
            this.path = path;
        }

        long lastIndex() {
            return firstIndex + count - 1;
        }

        void addOffset(long offset) {
            if (count == offsets.length) {
                offsets = Arrays.copyOf(offsets, count * 2);
            }
            offsets[count++] = offset;
        }
    }

    private SegmentedLog(Path directory, long segmentBytes) {
        this.directory = directory;
        this.segmentBytes = segmentBytes;
    }

    /** What {@link #load} found. */
    record Loaded(SegmentedLog log, List<LogEntry> entries) {
    }

    /**
     * Opens the log in {@code directory}, recovering every entry in it; an empty directory yields an empty log whose
     * first entry will have index {@code firstIndexIfEmpty}.
     */
    static Loaded load(Path directory, long segmentBytes, long firstIndexIfEmpty) throws IOException {
        SegmentedLog segmentedLog = new SegmentedLog(directory, segmentBytes);
        List<Long> starts = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "log-*.seg")) {
            for (Path file : files) {
                Matcher matcher = SEGMENT.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    starts.add(Long.parseLong(matcher.group(1)));
                }
            }
        }
        starts.sort(null);
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < starts.size(); i++) {
            long first = starts.get(i);
            if (!entries.isEmpty() && first != entries.getLast().index() + 1) {
                throw new StorageCorruptionException("log segment " + segmentName(first) + " does not follow index "
                        + entries.getLast().index());
            }
            Segment segment = new Segment(first, directory.resolve(segmentName(first)));
            segmentedLog.readSegment(segment, entries, i == starts.size() - 1);
            segmentedLog.segments.add(segment);
        }
        if (segmentedLog.segments.isEmpty()) {
            segmentedLog.startSegment(firstIndexIfEmpty);
        } else {
            Segment last = segmentedLog.segments.getLast();
            last.channel = FileChannel.open(last.path, StandardOpenOption.WRITE);
            last.channel.position(last.size);
        }
        return new Loaded(segmentedLog, entries);
    }

    private void readSegment(Segment segment, List<LogEntry> entries, boolean last) throws IOException {
        byte[] bytes = Files.readAllBytes(segment.path);
        int position = 0;
        while (position < bytes.length) {
            // Why the record at position is damaged, and whether that can be a write torn by a crash.
            String damage;
            boolean tornTail;
            if (bytes.length - position < HEADER_BYTES) {
                damage = "record header cut short";
                tornTail = true;
            } else {
                ByteBuffer header = ByteBuffer.wrap(bytes, position, HEADER_BYTES);
                int length = header.getInt();
                int crc = header.getInt();
                if (length < PAYLOAD_FIXED_BYTES) {
                    // File systems may extend a file with zeros before its data lands.
                    damage = "invalid record length " + length;
                    tornTail = onlyZeros(bytes, position);
                } else if (length > bytes.length - position - HEADER_BYTES) {
                    // Indistinguishable from an append cut short by the crash.
                    damage = "record cut short";
                    tornTail = true;
                } else if (DurableFiles.crc32c(bytes, position + HEADER_BYTES, length) != crc) {
                    damage = "record checksum mismatch";
                    tornTail = onlyZeros(bytes, position + HEADER_BYTES + length);
                } else {
                    ByteBuffer payload = ByteBuffer.wrap(bytes, position + HEADER_BYTES, length);
                    long index = payload.getLong();
                    long term = payload.getLong();
                    byte kind = payload.get();
                    byte[] data = new byte[payload.remaining()];
                    payload.get(data);
                    long expected = segment.firstIndex + segment.count;
                    if (index != expected || term < 1 || kind < 0 || kind >= LogEntry.Kind.values().length) {
                        throw new StorageCorruptionException(segment.path + ": record at offset " + position
                                + " holds index " + index + " (term " + term + ") where " + expected
                                + " was expected");
                    }
                    entries.add(new LogEntry(index, term, LogEntry.Kind.values()[kind], data));
                    segment.addOffset(position);
                    position += HEADER_BYTES + length;
                    continue;
                }
            }
            // Only the newest segment can end in a torn write: older ones were forced before the next one began.
            if (!last || !tornTail) {
                throw new StorageCorruptionException(segment.path + ": " + damage + " at offset " + position
                        + (last ? ", followed by more data" : " in a segment that is not the newest"));
            }
            log.warn("Truncating {} at offset {}: {} ({} bytes discarded)", segment.path, position, damage,
                    bytes.length - position);
            try (FileChannel channel = FileChannel.open(segment.path, StandardOpenOption.WRITE)) {
                channel.truncate(position);
                channel.force(true);
            }
            break;
        }
        segment.size = Math.min(position, bytes.length);
    }

    private static boolean onlyZeros(byte[] bytes, int from) {
        for (int i = from; i < bytes.length; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return true;
    }

    long lastIndex() {
        Segment last = segments.getLast();
        return last.firstIndex + last.count - 1;
    }

    void append(List<LogEntry> entries) throws IOException {
        for (LogEntry entry : entries) {
            Segment active = segments.getLast();
            if (entry.index() != active.firstIndex + active.count) {
                throw new IllegalStateException("appending " + entry + " after index " + lastIndex());
            }
            if (active.size >= segmentBytes && active.count > 0) {
                active.channel.force(false);
                active.channel.close();
                active.channel = null;
                active = startSegment(entry.index());
            }
            int length = PAYLOAD_FIXED_BYTES + entry.data().length;
            ensureCapacity(HEADER_BYTES + length);
            buffer.clear();
            buffer.putInt(length).putInt(0).putLong(entry.index()).putLong(entry.term())
                    .put((byte) entry.kind().ordinal()).put(entry.data());
            buffer.putInt(4, DurableFiles.crc32c(buffer.array(), HEADER_BYTES, length));
            buffer.flip();
            while (buffer.hasRemaining()) {
                active.channel.write(buffer);
            }
            active.addOffset(active.size);
            active.size += HEADER_BYTES + length;
            dataDirty = true;
        }
    }

    /** Removes every entry at or after {@code index}. */
    void truncateFrom(long index) throws IOException {
        if (index > lastIndex()) {
            return;
        }
        while (segments.size() > 1 && segments.getLast().firstIndex >= index) {
            Segment dropped = segments.removeLast();
            closeQuietly(dropped);
            Files.deleteIfExists(dropped.path);
        }
        Segment segment = segments.getLast();
        if (segment.channel == null) {
            segment.channel = FileChannel.open(segment.path, StandardOpenOption.WRITE);
        }
        int keep = (int) Math.max(0, index - segment.firstIndex);
        long offset = keep < segment.count ? segment.offsets[keep] : segment.size;
        segment.channel.truncate(offset);
        segment.channel.position(offset);
        segment.count = keep;
        segment.size = offset;
        metadataDirty = true;
    }

    /** Deletes the segments whose entries a snapshot through {@code index} covers. */
    void compactThrough(long index) throws IOException {
        while (segments.size() > 1 && segments.get(1).firstIndex <= index + 1) {
            Segment dropped = segments.removeFirst();
            Files.deleteIfExists(dropped.path);
            metadataDirty = true;
        }
        Segment only = segments.getFirst();
        if (segments.size() == 1 && only.count > 0 && only.lastIndex() <= index) {
            reset(index);
        }
    }

    /** Discards everything: the next entry appended will be {@code index + 1}. */
    void reset(long index) throws IOException {
        for (Segment segment : segments) {
            closeQuietly(segment);
            Files.deleteIfExists(segment.path);
        }
        segments.clear();
        startSegment(index + 1);
    }

    /** Forces appended entries, truncations and new files to disk. */
    void sync() throws IOException {
        Segment active = segments.getLast();
        if (dataDirty || metadataDirty) {
            active.channel.force(metadataDirty);
        }
        if (metadataDirty) {
            DurableFiles.syncDirectory(directory);
        }
        dataDirty = false;
        metadataDirty = false;
    }

    @Override
    public void close() throws IOException {
        for (Segment segment : segments) {
            if (segment.channel != null) {
                segment.channel.close();
                segment.channel = null;
            }
        }
    }

    private Segment startSegment(long firstIndex) throws IOException {
        Segment segment = new Segment(firstIndex, directory.resolve(segmentName(firstIndex)));
        segment.channel = FileChannel.open(segment.path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        segments.add(segment);
        metadataDirty = true;
        return segment;
    }

    private void ensureCapacity(int bytes) {
        if (buffer.capacity() < bytes) {
            buffer = ByteBuffer.allocate(Math.max(bytes, buffer.capacity() * 2));
        } else if (buffer.capacity() > (16 << 20) && bytes < 64 * 1024) {
            buffer = ByteBuffer.allocate(64 * 1024);
        }
    }

    private static void closeQuietly(Segment segment) {
        if (segment.channel != null) {
            try {
                segment.channel.close();
            } catch (IOException e) {
                log.warn("Could not close {}", segment.path, e);
            }
            segment.channel = null;
        }
    }

    static String segmentName(long firstIndex) {
        return String.format("log-%020d.seg", firstIndex);
    }
}
