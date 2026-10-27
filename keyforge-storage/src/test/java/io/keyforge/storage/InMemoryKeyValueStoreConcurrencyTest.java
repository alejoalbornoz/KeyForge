package io.keyforge.storage;

import static io.keyforge.storage.InMemoryKeyValueStoreTest.b;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class InMemoryKeyValueStoreConcurrencyTest {

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            InMemoryKeyValueStore.Options.defaults().withActiveExpireInterval(Duration.ZERO));

    @Test
    void concurrentReadModifyWritesLoseNoUpdates() throws Exception {
        int threads = 8;
        int incrementsPerThread = 20_000;
        runConcurrently(threads, () -> {
            for (int i = 0; i < incrementsPerThread; i++) {
                store.compute(b("counter"), current -> {
                    long value = current == null ? 0 : Long.parseLong(new String(current, StandardCharsets.US_ASCII));
                    return Long.toString(value + 1).getBytes(StandardCharsets.US_ASCII);
                });
            }
        });

        assertThat(new String(store.get(b("counter")), StandardCharsets.US_ASCII))
                .isEqualTo(Long.toString((long) threads * incrementsPerThread));
    }

    /**
     * A writer keeps setting many keys to the same generation with putAll; readers must never see keys from two
     * different generations in one getAll. The keys span many segments, so this exercises multi-segment locking.
     */
    @Test
    void multiKeyWritesAreAtomicForMultiKeyReads() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            keys.add(b("key-" + i));
        }
        store.putAll(keys, repeat(b("0"), keys.size()));
        AtomicBoolean done = new AtomicBoolean();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            Future<?> writer = executor.submit(() -> {
                for (int generation = 1; generation <= 5_000; generation++) {
                    store.putAll(keys, repeat(b(Integer.toString(generation)), keys.size()));
                }
                done.set(true);
            });
            List<Future<Integer>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(executor.submit(() -> {
                    int torn = 0;
                    while (!done.get()) {
                        List<byte[]> values = store.getAll(keys);
                        String first = new String(values.getFirst(), StandardCharsets.US_ASCII);
                        for (byte[] value : values) {
                            if (!first.equals(new String(value, StandardCharsets.US_ASCII))) {
                                torn++;
                                break;
                            }
                        }
                    }
                    return torn;
                }));
            }
            writer.get();
            for (Future<Integer> reader : readers) {
                assertThat(reader.get()).as("torn multi-key reads").isZero();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void overlappingMultiKeyOperationsDoNotDeadlock() throws Exception {
        List<byte[]> forward = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            forward.add(b("k" + i));
        }
        List<byte[]> backward = new ArrayList<>(forward.reversed());

        runConcurrently(4, () -> {
            for (int i = 0; i < 2_000; i++) {
                store.putAll(forward, repeat(b("x"), forward.size()));
                store.delete(backward);
                store.getAll(backward);
                store.size();
            }
        });

        assertThat(store.size()).isBetween(0L, 32L);
    }

    private static void runConcurrently(int threads, Runnable task) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    task.run();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static List<byte[]> repeat(byte[] value, int times) {
        List<byte[]> values = new ArrayList<>(times);
        for (int i = 0; i < times; i++) {
            values.add(value);
        }
        return values;
    }
}
