package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ServerHeapTest {

    @Test
    void parsesG1HeapInfo() {
        String heapInfo = """
                1:
                garbage-first heap   total reserved 2097152K, committed 14336K, used 3015K [0x0000000080000000, \
                0x0000000100000000)
                 region size 1024K, 1 young (1024K), 0 survivors (0K)
                """;

        assertThat(ServerHeap.parseUsedBytes(heapInfo)).isEqualTo(3015L * 1024);
    }

    @Test
    void parsesZgcHeapInfo() {
        assertThat(ServerHeap.parseUsedBytes(" ZHeap           used 58M, capacity 206M, max capacity 2048M"))
                .isEqualTo(58L * 1024 * 1024);
    }

    @Test
    void rejectsOutputWithoutUsage() {
        assertThatThrownBy(() -> ServerHeap.parseUsedBytes("1:\nCommand executed successfully"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
