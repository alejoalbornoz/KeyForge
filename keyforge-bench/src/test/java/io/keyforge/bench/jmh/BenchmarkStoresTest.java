package io.keyforge.bench.jmh;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class BenchmarkStoresTest {

    @Test
    void keysAreZeroPaddedSoTheySortInIndexOrder() {
        assertThat(new String(BenchmarkStores.key(42), StandardCharsets.US_ASCII)).isEqualTo("key:000000000042");
        assertThat(new String(BenchmarkStores.key(999_999_999_999L), StandardCharsets.US_ASCII))
                .isEqualTo("key:999999999999");
        assertThat(Arrays.compareUnsigned(BenchmarkStores.key(9), BenchmarkStores.key(10))).isNegative();
    }

    @Test
    void absentKeysSortBetweenNeighbouringPresentKeys() {
        byte[] absent = BenchmarkStores.absentKey(7);

        assertThat(Arrays.compareUnsigned(BenchmarkStores.key(7), absent)).isNegative();
        assertThat(Arrays.compareUnsigned(absent, BenchmarkStores.key(8))).isNegative();
    }

    @Test
    void valuesHaveTheRequestedSize() {
        assertThat(BenchmarkStores.value(100, 3)).hasSize(100);
    }
}
