package io.keyforge.storage.lsm;

/**
 * Summary of one SSTable, stored in its meta block.
 *
 * @param entryCount     entries, tombstones included
 * @param tombstoneCount entries that delete their key
 * @param expiringCount  entries that carry an expiry instant
 * @param minSeq         smallest sequence number in the table
 * @param maxSeq         largest sequence number in the table
 * @param minKey         first key
 * @param maxKey         last key
 */
record TableStats(long entryCount, long tombstoneCount, long expiringCount, long minSeq, long maxSeq, byte[] minKey,
                  byte[] maxKey) {

    void writeTo(ByteWriter out) {
        out.writeLong(entryCount);
        out.writeLong(tombstoneCount);
        out.writeLong(expiringCount);
        out.writeLong(minSeq);
        out.writeLong(maxSeq);
        out.writeVarInt(minKey.length);
        out.writeBytes(minKey);
        out.writeVarInt(maxKey.length);
        out.writeBytes(maxKey);
    }

    static TableStats readFrom(ByteReader in) {
        long entries = in.readLong();
        long tombstones = in.readLong();
        long expiring = in.readLong();
        long minSeq = in.readLong();
        long maxSeq = in.readLong();
        byte[] minKey = in.readBytes(in.readLength());
        byte[] maxKey = in.readBytes(in.readLength());
        if (entries <= 0 || tombstones < 0 || tombstones > entries || expiring < 0 || expiring > entries
                || minSeq > maxSeq) {
            throw in.corrupt("inconsistent table statistics");
        }
        return new TableStats(entries, tombstones, expiring, minSeq, maxSeq, minKey, maxKey);
    }
}
