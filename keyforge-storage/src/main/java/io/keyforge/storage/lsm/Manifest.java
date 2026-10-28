package io.keyforge.storage.lsm;

import java.util.ArrayList;
import java.util.List;

/**
 * The durable list of live SSTables and the bookkeeping recovery needs. The store rewrites it as a whole whenever the
 * set of tables changes (write a temporary file, fsync it, rename it over the old one; see {@link DataDirectory}), so a
 * reader always finds either the old or the new version, never a mix.
 *
 * <pre>
 * [magic:int "KFMF"][version:int][nextFileNumber:long][lastSequence:long][minWalNumber:long][tableCount:int]
 * ([fileNumber:long][fileSize:long][minSeq:long][maxSeq:long])*  newest table first
 * [crc32c(everything before):int]
 * </pre>
 *
 * @param nextFileNumber lower bound for the next WAL or SSTable file number
 * @param lastSequence   lower bound for the next sequence number (every sequence number in a table is at most this)
 * @param minWalNumber   WAL files numbered below this are fully flushed and must not be replayed
 * @param tables         the live tables, newest first; their sequence ranges do not overlap
 */
record Manifest(long nextFileNumber, long lastSequence, long minWalNumber, List<TableEntry> tables) {

    static final int MAGIC = 0x4B464D46;
    static final int FORMAT_VERSION = 1;

    /** One live table: its file number and size, and the range of sequence numbers it holds. */
    record TableEntry(long fileNumber, long fileSize, long minSeq, long maxSeq) {

        static TableEntry of(SSTable table) {
            return new TableEntry(table.fileNumber(), table.fileSize(), table.stats().minSeq(),
                    table.stats().maxSeq());
        }
    }

    Manifest {
        tables = List.copyOf(tables);
    }

    byte[] encode() {
        ByteWriter out = new ByteWriter(48 + tables.size() * 32);
        out.writeInt(MAGIC);
        out.writeInt(FORMAT_VERSION);
        out.writeLong(nextFileNumber);
        out.writeLong(lastSequence);
        out.writeLong(minWalNumber);
        out.writeInt(tables.size());
        for (TableEntry table : tables) {
            out.writeLong(table.fileNumber());
            out.writeLong(table.fileSize());
            out.writeLong(table.minSeq());
            out.writeLong(table.maxSeq());
        }
        out.writeInt(WalRecord.crc32c(out.array(), 0, out.size()));
        return out.toByteArray();
    }

    static Manifest decode(byte[] bytes, String source) {
        if (bytes.length < 4) {
            throw new CorruptionException(source + ": too short for a manifest");
        }
        ByteReader in = new ByteReader(bytes, source);
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw in.corrupt("bad magic number 0x" + Integer.toHexString(magic) + "; not a manifest");
        }
        int version = in.readInt();
        if (version != FORMAT_VERSION) {
            throw in.corrupt("unsupported manifest version " + version);
        }
        int crcOffset = bytes.length - 4;
        int storedCrc = new ByteReader(bytes, crcOffset, 4, source).readInt();
        if (WalRecord.crc32c(bytes, 0, crcOffset) != storedCrc) {
            throw in.corrupt("manifest CRC mismatch");
        }
        long nextFileNumber = in.readLong();
        long lastSequence = in.readLong();
        long minWalNumber = in.readLong();
        int count = in.readInt();
        if (count < 0 || (long) count * 32 != in.remaining() - 4) {
            throw in.corrupt("table count " + count + " does not match the manifest size");
        }
        List<TableEntry> tables = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            tables.add(new TableEntry(in.readLong(), in.readLong(), in.readLong(), in.readLong()));
        }
        return new Manifest(nextFileNumber, lastSequence, minWalNumber, tables);
    }
}
