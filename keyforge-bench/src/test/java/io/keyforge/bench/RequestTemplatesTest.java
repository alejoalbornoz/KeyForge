package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespRequestDecoder;

class RequestTemplatesTest {

    @Test
    void requestsAreValidGetsAndSetsOfKeysInTheKeyspace() {
        RequestTemplates templates = new RequestTemplates(100, 16, 0.5);
        SplittableRandom random = new SplittableRandom(1);
        int gets = 0;
        for (int i = 0; i < 2_000; i++) {
            String[] request = decode(templates.next(random));
            assertThat(request[1]).matches("key:00000[0-9][0-9]");
            if (request[0].equals("GET")) {
                assertThat(request).hasSize(2);
                gets++;
            } else {
                assertThat(request).hasSize(3);
                assertThat(request[0]).isEqualTo("SET");
                assertThat(request[2]).isEqualTo("v".repeat(16));
            }
        }
        assertThat(gets).as("about half are GETs").isBetween(850, 1150);
    }

    @Test
    void prepopulationSetsAreIndexedByKey() {
        RequestTemplates templates = new RequestTemplates(10, 3, 1.0);

        assertThat(decode(templates.set(7))).containsExactly("SET", "key:0000007", "vvv");
        assertThat(templates.keyspace()).isEqualTo(10);
    }

    @Test
    void aGetRatioOfOneOnlyReads() {
        RequestTemplates templates = new RequestTemplates(10, 3, 1.0);
        SplittableRandom random = new SplittableRandom(2);

        for (int i = 0; i < 100; i++) {
            assertThat(decode(templates.next(random))[0]).isEqualTo("GET");
        }
    }

    private static String[] decode(byte[] request) {
        byte[][] arguments = new RespRequestDecoder().decode(ByteBuffer.wrap(request));
        return Arrays.stream(arguments).map(a -> new String(a, StandardCharsets.US_ASCII)).toArray(String[]::new);
    }
}
