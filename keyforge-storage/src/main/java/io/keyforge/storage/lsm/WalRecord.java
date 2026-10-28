package io.keyforge.storage.lsm;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * One WAL record: every change made by one write, under one sequence number, so that replay applies a multi-key write
 * entirely or not at all.
 *
 * <pre>
 * record  = [length:int][crc32c:int][payload]       length and CRC cover the payload only
 * payload = [seq:long][count:varint] entry*
 * entry   = [type:byte][keyLength:varint][key]                                     type 2: delete
 *         | [type:byte][keyLength:varint][key][expireAt:long][valueLength:varint][value]   type 1: put
 * </pre>
 *
 * Integers are big-endian.
 */
record WalRecord(long seq, List<InternalEntry> entries) {

    static final int HEADER_BYTES = 8;

    /** Appends one framed record for {@code entries}, all written under {@code seq}, to {@code out}. */
    static void encode(ByteWriter out, long seq, List<InternalEntry> entries) {
        int start = out.size();
        try {
            int header = out.skip(HEADER_BYTES);
            int payloadStart = out.size();
            out.writeLong(seq);
            out.writeVarInt(entries.size());
            for (InternalEntry entry : entries) {
                out.writeByte(entry.isTombstone() ? InternalEntry.TYPE_DELETE : InternalEntry.TYPE_PUT);
                out.writeVarInt(entry.key().length);
                out.writeBytes(entry.key());
                if (!entry.isTombstone()) {
                    out.writeLong(entry.expireAt());
                    out.writeVarInt(entry.value().length);
                    out.writeBytes(entry.value());
                }
            }
            int length = out.size() - payloadStart;
            out.putInt(header, length);
            out.putInt(header + 4, crc32c(out.array(), payloadStart, length));
        } catch (RuntimeException e) {
            out.truncate(start);
            throw e;
        }
    }

    /** Decodes a payload whose CRC has already been verified. */
    static WalRecord decode(byte[] payload, String source) {
        ByteReader in = new ByteReader(payload, source);
        long seq = in.readLong();
        int count = in.readVarInt();
        if (count < 0 || count > payload.length) {
            throw in.corrupt("invalid entry count " + count);
        }
        List<InternalEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int type = in.readByte();
            byte[] key = in.readBytes(in.readLength());
            switch (type) {
                case InternalEntry.TYPE_PUT -> {
                    long expireAt = in.readLong();
                    byte[] value = in.readBytes(in.readLength());
                    entries.add(InternalEntry.put(key, seq, value, expireAt));
                }
                case InternalEntry.TYPE_DELETE -> entries.add(InternalEntry.tombstone(key, seq));
                default -> throw in.corrupt("unknown entry type " + type);
            }
        }
        if (in.hasRemaining()) {
            throw in.corrupt(in.remaining() + " unexpected bytes after the last entry");
        }
        return new WalRecord(seq, entries);
    }

    static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
