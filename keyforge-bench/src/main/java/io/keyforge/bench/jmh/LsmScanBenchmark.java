package io.keyforge.bench.jmh;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.keyforge.storage.lsm.KeyValue;
import io.keyforge.storage.lsm.LsmKeyValueStore;

/**
 * Ordered range scans: {@code scanLength} consecutive keys from a random start, merged from the memtable and the
 * tables.
 *
 * <ul>
 *   <li>{@code layout=compacted}: one table holds everything; the scan is a sequential read of a few blocks.</li>
 *   <li>{@code layout=layered}: the data is spread over several tables written at different times (older versions of
 *       some keys included) plus a non-empty memtable, so the scan merges several sources and skips shadowed
 *       versions.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class LsmScanBenchmark {

    @Param({"compacted", "layered"})
    public String layout;

    @Param({"1000000"})
    public long keyCount;

    @Param({"100"})
    public int scanLength;

    @Param({"100"})
    public int valueSize;

    private Path directory;
    private LsmKeyValueStore store;

    @State(Scope.Thread)
    public static class Starts {
        private final SplittableRandom random = new SplittableRandom(7);
    }

    @Setup(Level.Trial)
    public void load() {
        directory = BenchmarkStores.createDirectory();
        LsmKeyValueStore.Options options = LsmKeyValueStore.Options.defaults(directory);
        if (layout.equals("layered")) {
            // No background compaction, so the tables written below stay separate.
            options = options.withCompaction(1_000, 0);
        }
        store = LsmKeyValueStore.open(options);
        BenchmarkStores.preload(store, keyCount, valueSize);
        if (layout.equals("compacted")) {
            store.compact();
            return;
        }
        store.flush();
        // Rewrite every fourth key in a second table and every tenth in the memtable: scans see three sources.
        rewriteEvery(4);
        store.flush();
        rewriteEvery(10);
    }

    private void rewriteEvery(int step) {
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        for (long i = 0; i < keyCount; i += step) {
            keys.add(BenchmarkStores.key(i));
            values.add(BenchmarkStores.value(valueSize, i + step));
            if (keys.size() == 1_000) {
                store.putAll(keys, values);
                keys.clear();
                values.clear();
            }
        }
        store.putAll(keys, values);
    }

    @TearDown(Level.Trial)
    public void close() {
        store.close();
        BenchmarkStores.delete(directory);
    }

    @Benchmark
    public List<KeyValue> scan(Starts starts) {
        long start = starts.random.nextLong(Math.max(1, keyCount - scanLength));
        return store.scan(BenchmarkStores.key(start), null, scanLength);
    }
}
