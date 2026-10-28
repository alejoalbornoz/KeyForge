package io.keyforge.storage.lsm;

/**
 * MurmurHash3, x64 128-bit variant (Austin Appleby's public-domain algorithm). Bloom filters need a hash with good
 * avalanche; {@link java.util.Arrays#hashCode(byte[])} is a polynomial hash and clusters badly.
 */
final class Murmur3 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private Murmur3() {
    }

    /** The first 64 bits of the 128-bit hash. */
    static long hash64(byte[] data, long seed) {
        return hash128(data, seed)[0];
    }

    /** The 128-bit hash as {@code {h1, h2}}, the two halves the reference implementation writes out. */
    @SuppressWarnings("fallthrough")
    static long[] hash128(byte[] data, long seed) {
        int length = data.length;
        int blocks = length / 16;
        long h1 = seed;
        long h2 = seed;

        for (int i = 0; i < blocks; i++) {
            long k1 = littleEndianLong(data, i * 16);
            long k2 = littleEndianLong(data, i * 16 + 8);

            h1 ^= mixK1(k1);
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= mixK2(k2);
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        int tail = blocks * 16;
        long k1 = 0;
        long k2 = 0;
        switch (length & 15) {
            case 15: k2 ^= (long) (data[tail + 14] & 0xff) << 48;
            case 14: k2 ^= (long) (data[tail + 13] & 0xff) << 40;
            case 13: k2 ^= (long) (data[tail + 12] & 0xff) << 32;
            case 12: k2 ^= (long) (data[tail + 11] & 0xff) << 24;
            case 11: k2 ^= (long) (data[tail + 10] & 0xff) << 16;
            case 10: k2 ^= (long) (data[tail + 9] & 0xff) << 8;
            case 9:
                k2 ^= data[tail + 8] & 0xff;
                h2 ^= mixK2(k2);
            case 8: k1 ^= (long) (data[tail + 7] & 0xff) << 56;
            case 7: k1 ^= (long) (data[tail + 6] & 0xff) << 48;
            case 6: k1 ^= (long) (data[tail + 5] & 0xff) << 40;
            case 5: k1 ^= (long) (data[tail + 4] & 0xff) << 32;
            case 4: k1 ^= (long) (data[tail + 3] & 0xff) << 24;
            case 3: k1 ^= (long) (data[tail + 2] & 0xff) << 16;
            case 2: k1 ^= (long) (data[tail + 1] & 0xff) << 8;
            case 1:
                k1 ^= data[tail] & 0xff;
                h1 ^= mixK1(k1);
            default:
                break;
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        h2 += h1;
        return new long[] {h1, h2};
    }

    /** MurmurHash3's finalizer: a bijective mix with full avalanche. */
    static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long mixK1(long k1) {
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= C2;
        return k1;
    }

    private static long mixK2(long k2) {
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= C1;
        return k2;
    }

    private static long littleEndianLong(byte[] data, int offset) {
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (data[offset + i] & 0xffL);
        }
        return value;
    }
}
