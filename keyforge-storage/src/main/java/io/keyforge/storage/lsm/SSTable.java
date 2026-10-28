package io.keyforge.storage.lsm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read-only view of one SSTable file (format in {@link SSTableWriter}).
 *
 * <p>The file is memory-mapped, so reading a block is a copy out of the page cache rather than a system call. That
 * matters on virtual threads: a blocking file read holds the carrier thread and makes the JDK add carriers, a mapped
 * read does not. The sparse index, the Bloom filter and the statistics are decoded onto the heap when the table opens.
 * Every block is checked against its CRC when read.
 *
 * <p><b>Lifetime.</b> A table is reference-counted: whoever opens it holds one reference, and every {@link Version} that
 * lists it holds one. When the count reaches zero the mapping is released, and the file is deleted if the table was
 * {@linkplain #markObsolete() made obsolete} by a compaction or a clear. Releasing the mapping before deleting is
 * required on Windows, which refuses to delete a mapped file.
 */
final class SSTable {

    private static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private final long fileNumber;
    private final Path path;
    private final long fileSize;
    private final Arena arena;
    private final MemorySegment data;
    private final byte[][] blockLastKeys;
    private final long[] blockOffsets;
    private final int[] blockLengths;
    private final BloomFilter bloom;
    private final TableStats stats;
    private final AtomicInteger refs = new AtomicInteger(1);
    private volatile boolean obsolete;

    private SSTable(long fileNumber, Path path, long fileSize, Arena arena, MemorySegment data) {
        this.fileNumber = fileNumber;
        this.path = path;
        this.fileSize = fileSize;
        this.arena = arena;
        this.data = data;

        byte[] footer = copy(fileSize - SSTableWriter.FOOTER_BYTES, SSTableWriter.FOOTER_BYTES);
        ByteReader in = new ByteReader(footer, path + " footer");
        long indexOffset = in.readLong();
        int indexLength = in.readInt();
        long filterOffset = in.readLong();
        int filterLength = in.readInt();
        long metaOffset = in.readLong();
        int metaLength = in.readInt();
        int footerCrc = in.readInt();
        int formatVersion = in.readInt();
        long magic = in.readLong();
        if (magic != SSTableWriter.MAGIC) {
            throw corrupt("bad magic number 0x" + Long.toHexString(magic) + "; not an SSTable");
        }
        if (formatVersion != SSTableWriter.FORMAT_VERSION) {
            throw corrupt("unsupported format version " + formatVersion + " (this build reads version "
                    + SSTableWriter.FORMAT_VERSION + ")");
        }
        if (WalRecord.crc32c(footer, 0, SSTableWriter.FOOTER_CHECKSUMMED_BYTES) != footerCrc) {
            throw corrupt("footer CRC mismatch");
        }

        ByteReader index = new ByteReader(readBlock(indexOffset, indexLength, "index block"), path + " index");
        int blockCount = index.readVarInt();
        if (blockCount <= 0 || blockCount > index.remaining()) {
            throw corrupt("invalid block count " + blockCount);
        }
        blockLastKeys = new byte[blockCount][];
        blockOffsets = new long[blockCount];
        blockLengths = new int[blockCount];
        for (int i = 0; i < blockCount; i++) {
            blockLastKeys[i] = index.readBytes(index.readLength());
            blockOffsets[i] = index.readVarLong();
            blockLengths[i] = index.readVarInt();
            checkBounds(blockOffsets[i], blockLengths[i], "data block " + i);
        }
        bloom = BloomFilter.readFrom(new ByteReader(readBlock(filterOffset, filterLength, "filter block"),
                path + " filter"));
        stats = TableStats.readFrom(new ByteReader(readBlock(metaOffset, metaLength, "meta block"), path + " meta"));
    }

    /** Maps and validates the table at {@code path}. The caller holds the one reference it returns with. */
    static SSTable open(Path path, long fileNumber) throws IOException {
        Arena arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < SSTableWriter.FOOTER_BYTES) {
                throw new CorruptionException(path + ": " + size + " bytes is too short for an SSTable");
            }
            MemorySegment data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            return new SSTable(fileNumber, path, size, arena, data);
        } catch (IOException | RuntimeException e) {
            arena.close();
            throw e;
        }
    }

    long fileNumber() {
        return fileNumber;
    }

    long fileSize() {
        return fileSize;
    }

    TableStats stats() {
        return stats;
    }

    /** The entry for {@code key}, which may be a tombstone or expired, or {@code null} if the table has none. */
    InternalEntry get(byte[] key) {
        if (Arrays.compareUnsigned(key, stats.minKey()) < 0 || Arrays.compareUnsigned(key, stats.maxKey()) > 0
                || !bloom.mightContain(key)) {
            return null;
        }
        int block = findBlock(key);
        if (block < 0) {
            return null;
        }
        byte[] content = readDataBlock(block);
        ByteReader in = new ByteReader(content, path + " block " + block);
        while (in.hasRemaining()) {
            int type = in.readByte();
            int keyLength = in.readLength();
            int keyStart = in.position();
            int order = Arrays.compareUnsigned(content, keyStart, keyStart + keyLength, key, 0, key.length);
            if (order > 0) {
                return null;
            }
            in.skip(keyLength);
            if (order == 0) {
                return decodeRest(in, type, Arrays.copyOfRange(content, keyStart, keyStart + keyLength));
            }
            skipRest(in, type);
        }
        return null;
    }

    /** Entries with keys in {@code [from, to)} in key order; {@code null} bounds are open. */
    Iterator<InternalEntry> iterator(byte[] from, byte[] to) {
        int firstBlock = from == null ? 0 : findBlock(from);
        return new Iterator<>() {
            private int block = firstBlock < 0 ? blockLastKeys.length : firstBlock;
            private ByteReader in = block < blockLastKeys.length ? openBlock(block) : null;
            private InternalEntry next = advance();

            private InternalEntry advance() {
                while (in != null) {
                    if (!in.hasRemaining()) {
                        block++;
                        in = block < blockLastKeys.length ? openBlock(block) : null;
                        continue;
                    }
                    int type = in.readByte();
                    byte[] key = in.readBytes(in.readLength());
                    InternalEntry entry = decodeRest(in, type, key);
                    if (from != null && Arrays.compareUnsigned(key, from) < 0) {
                        continue;
                    }
                    if (to != null && Arrays.compareUnsigned(key, to) >= 0) {
                        in = null;
                        return null;
                    }
                    return entry;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public InternalEntry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                InternalEntry current = next;
                next = advance();
                return current;
            }
        };
    }

    /** Takes another reference. Only legal while the caller already holds one, directly or through a version. */
    void retain() {
        if (refs.getAndIncrement() <= 0) {
            throw new IllegalStateException("SSTable " + fileNumber + " was already released");
        }
    }

    /** Drops a reference; the last one unmaps the file and, if the table is obsolete, deletes it. */
    void release() {
        int remaining = refs.decrementAndGet();
        if (remaining > 0) {
            return;
        }
        if (remaining < 0) {
            throw new IllegalStateException("SSTable " + fileNumber + " released too often");
        }
        arena.close();
        if (obsolete) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.warn("Could not delete obsolete SSTable {}; it will be removed at the next start", path, e);
            }
        }
    }

    /** No version will list this table again; delete its file once the last reader is done. */
    void markObsolete() {
        obsolete = true;
    }

    @Override
    public String toString() {
        return "SSTable[" + fileNumber + ", " + fileSize + " bytes, " + stats.entryCount() + " entries]";
    }

    /** Index of the first block whose last key is at or after {@code key}, or -1 if every key in the table is smaller. */
    private int findBlock(byte[] key) {
        int low = 0;
        int high = blockLastKeys.length - 1;
        int found = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (Arrays.compareUnsigned(blockLastKeys[mid], key) >= 0) {
                found = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return found;
    }

    private ByteReader openBlock(int block) {
        return new ByteReader(readDataBlock(block), path + " block " + block);
    }

    private byte[] readDataBlock(int block) {
        return readBlock(blockOffsets[block], blockLengths[block], "data block " + block);
    }

    private byte[] readBlock(long offset, int length, String what) {
        checkBounds(offset, length, what);
        byte[] content = copy(offset, length);
        if (WalRecord.crc32c(content, 0, length) != data.get(INT, offset + length)) {
            throw corrupt(what + " CRC mismatch");
        }
        return content;
    }

    private void checkBounds(long offset, int length, String what) {
        if (offset < 0 || length < 0 || offset + length + 4 > fileSize - SSTableWriter.FOOTER_BYTES) {
            throw corrupt(what + " at offset " + offset + " with length " + length + " lies outside the file");
        }
    }

    private byte[] copy(long offset, int length) {
        byte[] bytes = new byte[length];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, offset, bytes, 0, length);
        return bytes;
    }

    private static InternalEntry decodeRest(ByteReader in, int type, byte[] key) {
        long seq = in.readLong();
        return switch (type) {
            case InternalEntry.TYPE_PUT -> {
                long expireAt = in.readLong();
                yield InternalEntry.put(key, seq, in.readBytes(in.readLength()), expireAt);
            }
            case InternalEntry.TYPE_DELETE -> InternalEntry.tombstone(key, seq);
            default -> throw in.corrupt("unknown entry type " + type);
        };
    }

    private static void skipRest(ByteReader in, int type) {
        in.skip(8);
        if (type == InternalEntry.TYPE_PUT) {
            in.skip(8);
            in.skip(in.readLength());
        } else if (type != InternalEntry.TYPE_DELETE) {
            throw in.corrupt("unknown entry type " + type);
        }
    }

    private CorruptionException corrupt(String problem) {
        return new CorruptionException(path + ": " + problem);
    }
}
