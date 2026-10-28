package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Writes one immutable SSTable file from entries in strictly increasing key order.
 *
 * <pre>
 * file   = data-block* index-block filter-block meta-block footer
 * block  = [content][crc32c(content):int]
 * data   = entry*, cut after the entry that brings the block to {@value #BLOCK_SIZE} bytes or more
 * entry  = [type:byte][keyLength:varint][key][seq:long]                                       type 2: tombstone
 *        | [type:byte][keyLength:varint][key][seq:long][expireAt:long][valueLength:varint][value]  type 1: put
 * index  = [blockCount:varint] ([lastKeyLength:varint][lastKey][offset:varlong][length:varint])*
 * filter = Bloom filter over every key (see {@link BloomFilter})
 * meta   = {@link TableStats}
 * footer = [indexOffset:long][indexLength:int][filterOffset:long][filterLength:int][metaOffset:long][metaLength:int]
 *          [crc32c(previous 36 bytes):int][formatVersion:int][magic:long]
 * </pre>
 *
 * The index is sparse: one entry, the block's last key, per data block. Block lengths exclude the CRC trailer.
 * Integers are big-endian.
 */
final class SSTableWriter implements AutoCloseable {

    /** Target data block size. Blocks end at the first entry boundary at or after it. */
    static final int BLOCK_SIZE = 4096;
    /** "KEYFORGE" in ASCII. */
    static final long MAGIC = 0x4B455946_4F524745L;
    static final int FORMAT_VERSION = 1;
    static final int FOOTER_BYTES = 52;
    /** Bytes of the footer covered by its CRC: the three offset/length pairs. */
    static final int FOOTER_CHECKSUMMED_BYTES = 36;

    private static final int WRITE_BUFFER_BYTES = 64 * 1024;

    private final Path path;
    private final FileChannel channel;
    private final BloomFilter bloom;
    /** Encoded bytes not yet written to the file. */
    private final ByteWriter pending = new ByteWriter(WRITE_BUFFER_BYTES + 2 * BLOCK_SIZE);
    private final ByteWriter block = new ByteWriter(2 * BLOCK_SIZE);
    private final ByteWriter indexEntries = new ByteWriter(1024);

    private long written;
    private int blockCount;
    private byte[] firstKey;
    private byte[] lastKey;
    private long entries;
    private long tombstones;
    private long expiring;
    private long minSeq = Long.MAX_VALUE;
    private long maxSeq = Long.MIN_VALUE;
    private boolean finished;

    /** @param expectedKeys sizes the Bloom filter; an overestimate only lowers the false-positive rate */
    SSTableWriter(Path path, long expectedKeys) throws IOException {
        this.path = path;
        this.bloom = BloomFilter.create(expectedKeys);
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    long entryCount() {
        return entries;
    }

    void add(InternalEntry entry) throws IOException {
        byte[] key = entry.key();
        if (lastKey != null && Arrays.compareUnsigned(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be added in strictly increasing order");
        }
        if (firstKey == null) {
            firstKey = key;
        }
        lastKey = key;

        block.writeByte(entry.isTombstone() ? InternalEntry.TYPE_DELETE : InternalEntry.TYPE_PUT);
        block.writeVarInt(key.length);
        block.writeBytes(key);
        block.writeLong(entry.seq());
        if (entry.isTombstone()) {
            tombstones++;
        } else {
            block.writeLong(entry.expireAt());
            block.writeVarInt(entry.value().length);
            block.writeBytes(entry.value());
            if (entry.expireAt() != NO_EXPIRY) {
                expiring++;
            }
        }
        bloom.add(key);
        entries++;
        minSeq = Math.min(minSeq, entry.seq());
        maxSeq = Math.max(maxSeq, entry.seq());

        if (block.size() >= BLOCK_SIZE) {
            finishDataBlock();
        }
    }

    /** Writes the index, filter, meta block and footer, then forces the file to disk and closes it. */
    TableStats finish() throws IOException {
        if (entries == 0) {
            throw new IllegalStateException("An SSTable needs at least one entry");
        }
        if (block.size() > 0) {
            finishDataBlock();
        }
        ByteWriter index = new ByteWriter(indexEntries.size() + 5);
        index.writeVarInt(blockCount);
        index.writeBytes(indexEntries.array(), 0, indexEntries.size());
        long indexOffset = appendBlock(index);

        ByteWriter filter = new ByteWriter((int) Math.min(Integer.MAX_VALUE - 16, bloom.bitCount() / 8 + 8));
        bloom.writeTo(filter);
        long filterOffset = appendBlock(filter);

        TableStats stats = new TableStats(entries, tombstones, expiring, minSeq, maxSeq, firstKey, lastKey);
        ByteWriter meta = new ByteWriter(64 + firstKey.length + lastKey.length);
        stats.writeTo(meta);
        long metaOffset = appendBlock(meta);

        ByteWriter footer = new ByteWriter(FOOTER_BYTES);
        footer.writeLong(indexOffset);
        footer.writeInt(index.size());
        footer.writeLong(filterOffset);
        footer.writeInt(filter.size());
        footer.writeLong(metaOffset);
        footer.writeInt(meta.size());
        footer.writeInt(WalRecord.crc32c(footer.array(), 0, FOOTER_CHECKSUMMED_BYTES));
        footer.writeInt(FORMAT_VERSION);
        footer.writeLong(MAGIC);
        pending.writeBytes(footer.array(), 0, footer.size());

        drain();
        channel.force(true);
        channel.close();
        finished = true;
        return stats;
    }

    /** Closes and deletes an unfinished file. */
    void abort() {
        try {
            channel.close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // Best effort: recovery deletes table files the manifest does not list.
        }
    }

    @Override
    public void close() {
        if (!finished) {
            abort();
        }
    }

    private void finishDataBlock() throws IOException {
        long offset = appendBlock(block);
        indexEntries.writeVarInt(lastKey.length);
        indexEntries.writeBytes(lastKey);
        indexEntries.writeVarLong(offset);
        indexEntries.writeVarInt(block.size());
        blockCount++;
        block.reset();
    }

    /** Queues {@code content} plus its CRC trailer and returns the file offset it starts at. */
    private long appendBlock(ByteWriter content) throws IOException {
        long offset = written + pending.size();
        pending.writeBytes(content.array(), 0, content.size());
        pending.writeInt(WalRecord.crc32c(content.array(), 0, content.size()));
        if (pending.size() >= WRITE_BUFFER_BYTES) {
            drain();
        }
        return offset;
    }

    private void drain() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(pending.array(), 0, pending.size());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        written += pending.size();
        pending.reset();
    }
}
