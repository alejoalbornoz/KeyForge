package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;

import java.util.Arrays;

/**
 * One version of a key inside the engine: the value written by the write with sequence number {@code seq}, or a
 * tombstone ({@code value == null}) if that write deleted the key. {@code expireAt} is an absolute epoch-millisecond
 * instant, or {@link io.keyforge.storage.KeyValueStore#NO_EXPIRY}.
 */
record InternalEntry(byte[] key, long seq, byte[] value, long expireAt) {

    static final int TYPE_PUT = 1;
    static final int TYPE_DELETE = 2;

    static InternalEntry put(byte[] key, long seq, byte[] value, long expireAt) {
        return new InternalEntry(key, seq, value, expireAt);
    }

    static InternalEntry tombstone(byte[] key, long seq) {
        return new InternalEntry(key, seq, null, NO_EXPIRY);
    }

    boolean isTombstone() {
        return value == null;
    }

    boolean isExpiredAt(long now) {
        return expireAt != NO_EXPIRY && expireAt <= now;
    }

    /** A value that a read at {@code now} returns: not a tombstone and not expired. */
    boolean isLiveAt(long now) {
        return value != null && !isExpiredAt(now);
    }

    InternalEntry withSeq(long newSeq) {
        return new InternalEntry(key, newSeq, value, expireAt);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof InternalEntry that
                && seq == that.seq
                && expireAt == that.expireAt
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Arrays.hashCode(key) + Long.hashCode(seq)) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "InternalEntry[key=" + Arrays.toString(key) + ", seq=" + seq
                + (value == null ? ", tombstone" : ", value=" + Arrays.toString(value) + ", expireAt=" + expireAt) + "]";
    }
}
