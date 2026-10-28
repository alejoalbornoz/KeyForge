package io.keyforge.bench.jmh;

import java.nio.file.Path;
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

import io.keyforge.storage.lsm.LsmKeyValueStore;

/**
 * Point-lookup latency of the LSM engine, for keys that exist and keys that do not.
 *
 * <ul>
 *   <li>{@code location=memtable}: the data set fits the memtable; lookups are skip-list searches.</li>
 *   <li>{@code location=sstable}: the data set is flushed and compacted into one table; a hit costs a Bloom-filter check,
 *       a binary search of the sparse index and a 4 KiB block copied out of the memory-mapped file; a miss usually stops
 *       at the Bloom filter.</li>
 * </ul>
 *
 * Missing keys lie inside the stored key range ({@link BenchmarkStores#absentKey}), so they are not rejected by a
 * table's min/max key check alone.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class LsmGetBenchmark {

    @Param({"memtable", "sstable"})
    public String location;

    @Param({"1000000"})
    public long keyCount;

    @Param({"100"})
    public int valueSize;

    private Path directory;
    private LsmKeyValueStore store;

    @State(Scope.Thread)
    public static class Lookups {
        private final SplittableRandom random = new SplittableRandom(42);
    }

    @Setup(Level.Trial)
    public void load() {
        directory = BenchmarkStores.createDirectory();
        store = LsmKeyValueStore.open(LsmKeyValueStore.Options.defaults(directory)
                .withMemtableSizeBytes(location.equals("memtable") ? 1L << 30 : 64L << 20));
        BenchmarkStores.preload(store, keyCount, valueSize);
        if (location.equals("sstable")) {
            store.compact();
        }
    }

    @TearDown(Level.Trial)
    public void close() {
        store.close();
        BenchmarkStores.delete(directory);
    }

    @Benchmark
    public byte[] getHit(Lookups lookups) {
        return store.get(BenchmarkStores.key(lookups.random.nextLong(keyCount)));
    }

    @Benchmark
    public byte[] getMiss(Lookups lookups) {
        return store.get(BenchmarkStores.absentKey(lookups.random.nextLong(keyCount)));
    }
}
