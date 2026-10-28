package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.WriteCondition.ALWAYS;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrency checks with memtables small enough that rotations, flushes and compactions run underneath the readers
 * and writers, so the version swaps and table reference counts are exercised too.
 */
@Timeout(120)
class LsmKeyValueStoreConcurrencyTest {

    @TempDir
    Path dir;

    private LsmKeyValueStore.Options options() {
        return LsmKeyValueStore.Options.defaults(dir)
                .withMemtableSizeBytes(32 * 1024)
                .withCompaction(3, 1 << 20)
                .withGroupCommit(Duration.ofMillis(2), 64 * 1024);
    }

    @Test
    void concurrentReadModifyWritesLoseNoUpdates() throws Exception {
        int threads = 8;
        int incrementsPerThread = 2_000;
        try (LsmKeyValueStore store = LsmKeyValueStore.open(options())) {
            runConcurrently(threads, index -> () -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    store.compute(b("counter"), current -> increment(current));
                    store.put(b("noise-" + index + "-" + i), new byte[64], NO_EXPIRY, ALWAYS);
                }
                return null;
            });
            assertThat(s(store.get(b("counter")))).isEqualTo(Integer.toString(threads * incrementsPerThread));
        }
        try (LsmKeyValueStore reopened = LsmKeyValueStore.open(options())) {
            assertThat(s(reopened.get(b("counter")))).isEqualTo(Integer.toString(threads * incrementsPerThread));
        }
    }

    /**
     * A writer keeps setting 64 keys to the same generation with putAll; readers using getAll and scan must never see two
     * generations at once, while memtables rotate and tables are flushed and merged underneath.
     */
    @Test
    void multiKeyWritesAreAtomicForMultiKeyReadsAndScans() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            keys.add(b(String.format("key-%02d", i)));
        }
        try (LsmKeyValueStore store = LsmKeyValueStore.open(options())) {
            store.putAll(keys, Collections.nCopies(keys.size(), b("0")));
            AtomicBoolean done = new AtomicBoolean();
            ExecutorService executor = Executors.newFixedThreadPool(5);
            try {
                Future<?> writer = executor.submit(() -> {
                    for (int generation = 1; generation <= 3_000; generation++) {
                        store.putAll(keys, Collections.nCopies(keys.size(), b(Integer.toString(generation))));
                    }
                    done.set(true);
                });
                List<Future<Integer>> readers = new ArrayList<>();
                for (int r = 0; r < 4; r++) {
                    boolean scanner = r % 2 == 0;
                    readers.add(executor.submit(() -> {
                        int torn = 0;
                        int reads = 0;
                        while (!done.get() || reads == 0) {
                            List<String> values = scanner
                                    ? store.scan(null, null, 1_000).stream().map(kv -> s(kv.value())).toList()
                                    : store.getAll(keys).stream().map(Bytes::s).toList();
                            if (values.size() != keys.size() || values.stream().distinct().count() != 1) {
                                torn++;
                            }
                            reads++;
                        }
                        return torn;
                    }));
                }
                writer.get();
                for (Future<Integer> reader : readers) {
                    assertThat(reader.get()).as("torn reads").isZero();
                }
            } finally {
                executor.shutdownNow();
            }
            assertThat(store.tableStats()).as("tables were flushed during the test").isNotEmpty();
        }
    }

    /** A read that starts after a write returned must see it (or something newer), whatever thread it runs on. */
    @Test
    void readsSeeEveryCompletedWrite() throws Exception {
        try (LsmKeyValueStore store = LsmKeyValueStore.open(options())) {
            List<Integer> stale = Collections.synchronizedList(new ArrayList<>());
            runConcurrently(8, index -> () -> {
                for (int i = 0; i < 2_000; i++) {
                    byte[] key = b("thread-" + index + "-" + (i % 50));
                    String value = "value-" + i;
                    store.put(key, b(value), NO_EXPIRY, ALWAYS);
                    if (!value.equals(s(store.get(key)))) {
                        stale.add(i);
                    }
                }
                return null;
            });
            assertThat(stale).isEmpty();
        }
    }

    private static byte[] increment(byte[] current) {
        int value = current == null ? 0 : Integer.parseInt(s(current));
        return b(Integer.toString(value + 1));
    }

    private interface TaskFactory {
        Callable<Void> create(int index);
    }

    private static void runConcurrently(int threads, TaskFactory tasks) throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(tasks.create(t)));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
