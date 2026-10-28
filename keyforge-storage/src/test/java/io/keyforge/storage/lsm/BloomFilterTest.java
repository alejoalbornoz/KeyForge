package io.keyforge.storage.lsm;

import static io.keyforge.storage.lsm.Bytes.b;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BloomFilterTest {

    @Test
    void murmur3MatchesTheReferenceImplementation() {
        // MurmurHash3_x64_128("The quick brown fox jumps over the lazy dog", seed 0) = 6c1b07bc7bbc4be3 47939ac4a93c437a
        // as little-endian bytes, that is h1 = 0xe34bbc7bbc071b6c and h2 = 0x7a433ca9c49a9347.
        long[] hash = Murmur3.hash128(b("The quick brown fox jumps over the lazy dog"), 0);

        assertThat(hash[0]).isEqualTo(0xe34bbc7bbc071b6cL);
        assertThat(hash[1]).isEqualTo(0x7a433ca9c49a9347L);
        assertThat(Murmur3.hash128(new byte[0], 0)).containsExactly(0L, 0L);
    }

    @Test
    void neverReportsAnAddedKeyAsAbsent() {
        BloomFilter filter = BloomFilter.create(10_000);
        for (int i = 0; i < 10_000; i++) {
            filter.add(b("key-" + i));
        }

        for (int i = 0; i < 10_000; i++) {
            assertThat(filter.mightContain(b("key-" + i))).isTrue();
        }
    }

    @Test
    void falsePositiveRateIsAboutOnePercent() {
        BloomFilter filter = BloomFilter.create(20_000);
        for (int i = 0; i < 20_000; i++) {
            filter.add(b("present-" + i));
        }

        int falsePositives = 0;
        int probes = 200_000;
        for (int i = 0; i < probes; i++) {
            if (filter.mightContain(b("absent-" + i))) {
                falsePositives++;
            }
        }

        double rate = (double) falsePositives / probes;
        assertThat(rate).as("false-positive rate").isBetween(0.002, 0.015);
    }

    @Test
    void survivesSerialization() {
        BloomFilter filter = BloomFilter.create(100);
        filter.add(b("a"));
        filter.add(b("b"));
        ByteWriter out = new ByteWriter(16);
        filter.writeTo(out);

        BloomFilter read = BloomFilter.readFrom(new ByteReader(out.toByteArray(), "test"));

        assertThat(read.mightContain(b("a"))).isTrue();
        assertThat(read.mightContain(b("b"))).isTrue();
        assertThat(read.bitCount()).isEqualTo(filter.bitCount());
    }

    @Test
    void rejectsAnImplausibleHeader() {
        ByteWriter out = new ByteWriter(16);
        out.writeByte(0);
        out.writeInt(1);
        out.writeLong(0);

        assertThatThrownBy(() -> BloomFilter.readFrom(new ByteReader(out.toByteArray(), "test")))
                .isInstanceOf(CorruptionException.class);
    }
}
