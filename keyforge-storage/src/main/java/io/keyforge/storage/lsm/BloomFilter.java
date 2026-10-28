package io.keyforge.storage.lsm;

/**
 * A Bloom filter over the keys of one SSTable, so that a lookup for a key the table does not hold usually costs a few
 * memory probes instead of a block read.
 *
 * <p>Ten bits per key and seven probes give a false-positive rate of about 0.8% (the optimum for ten bits per key is
 * {@code 10 * ln 2 ≈ 6.9} probes). Probe positions use Kirsch-Mitzenmacher double hashing, {@code h1 + i * h2}, over the
 * two halves of a 128-bit MurmurHash3, so one hash computation serves every probe.
 */
final class BloomFilter {

    static final int BITS_PER_KEY = 10;
    static final int HASH_FUNCTIONS = 7;

    private static final long SEED = 0x4B4559464F524745L;

    private final long[] words;
    private final long bitCount;
    private final int hashFunctions;

    private BloomFilter(long[] words, int hashFunctions) {
        this.words = words;
        this.bitCount = (long) words.length * Long.SIZE;
        this.hashFunctions = hashFunctions;
    }

    /** An empty filter sized for {@code expectedKeys}; more keys still work, at a higher false-positive rate. */
    static BloomFilter create(long expectedKeys) {
        long bits = Math.max(Long.SIZE, Math.multiplyExact(Math.max(expectedKeys, 1), BITS_PER_KEY));
        long wordCount = (bits + Long.SIZE - 1) / Long.SIZE;
        if (wordCount > Integer.MAX_VALUE - 16) {
            throw new IllegalArgumentException("Bloom filter for " + expectedKeys + " keys is too large");
        }
        return new BloomFilter(new long[(int) wordCount], HASH_FUNCTIONS);
    }

    void add(byte[] key) {
        long[] hash = Murmur3.hash128(key, SEED);
        long combined = hash[0];
        long step = hash[1] | 1;
        for (int i = 0; i < hashFunctions; i++) {
            long bit = Long.remainderUnsigned(combined, bitCount);
            words[(int) (bit >>> 6)] |= 1L << bit;
            combined += step;
        }
    }

    /** {@code false} means the key was certainly never added; {@code true} means it probably was. */
    boolean mightContain(byte[] key) {
        long[] hash = Murmur3.hash128(key, SEED);
        long combined = hash[0];
        long step = hash[1] | 1;
        for (int i = 0; i < hashFunctions; i++) {
            long bit = Long.remainderUnsigned(combined, bitCount);
            if ((words[(int) (bit >>> 6)] & (1L << bit)) == 0) {
                return false;
            }
            combined += step;
        }
        return true;
    }

    long bitCount() {
        return bitCount;
    }

    /** {@code [hashFunctions:byte][wordCount:int][words:long...]}. */
    void writeTo(ByteWriter out) {
        out.writeByte(hashFunctions);
        out.writeInt(words.length);
        for (long word : words) {
            out.writeLong(word);
        }
    }

    static BloomFilter readFrom(ByteReader in) {
        int hashFunctions = in.readByte();
        int wordCount = in.readInt();
        if (hashFunctions < 1 || hashFunctions > 30 || wordCount < 1 || (long) wordCount * 8 > in.remaining()) {
            throw in.corrupt("invalid bloom filter header (" + hashFunctions + " hash functions, " + wordCount
                    + " words)");
        }
        long[] words = new long[wordCount];
        for (int i = 0; i < wordCount; i++) {
            words[i] = in.readLong();
        }
        return new BloomFilter(words, hashFunctions);
    }
}
