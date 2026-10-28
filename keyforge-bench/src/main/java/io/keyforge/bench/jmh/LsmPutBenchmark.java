package io.keyforge.bench.jmh;

import java.nio.file.Path;
import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.WriteCondition;
import io.keyforge.storage.lsm.FsyncPolicy;
import io.keyforge.storage.lsm.LsmKeyValueStore;

/**
 * Write throughput of the LSM engine: unconditional puts of random keys, which exercise the writer thread, the WAL, the
 * memtable and, as the data grows, background flushes and compactions.
 *
 * <p>With one thread every put waits for its own batch; with 16 the writer batches concurrent puts, so one WAL write
 * (and, with {@code always}, one fsync) serves many of them. Results depend heavily on the disk's fsync latency.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class LsmPutBenchmark {

    @Param({"group-commit", "always"})
    public String fsyncPolicy;

    @Param({"100"})
    public int valueSize;

    private Path directory;
    private LsmKeyValueStore store;
    private byte[] value;

    /** Each benchmark thread writes its own random keys. */
    @State(Scope.Thread)
    public static class Keys {
        private static final AtomicInteger THREADS = new AtomicInteger();
        private final SplittableRandom random = new SplittableRandom(THREADS.incrementAndGet());

        byte[] next() {
            return BenchmarkStores.key(random.nextLong(Long.MAX_VALUE / 2));
        }
    }

    @Setup(Level.Trial)
    public void open() {
        directory = BenchmarkStores.createDirectory();
        store = LsmKeyValueStore.open(LsmKeyValueStore.Options.defaults(directory)
                .withFsyncPolicy(FsyncPolicy.fromConfigName(fsyncPolicy))
                .withGroupCommit(Duration.ofMillis(10), 1 << 20));
        value = BenchmarkStores.value(valueSize, 0);
    }

    @TearDown(Level.Trial)
    public void close() {
        store.close();
        BenchmarkStores.delete(directory);
    }

    @Benchmark
    @Threads(1)
    public boolean put(Keys keys) {
        return store.put(keys.next(), value, KeyValueStore.NO_EXPIRY, WriteCondition.ALWAYS);
    }

    @Benchmark
    @Threads(16)
    public boolean put16Threads(Keys keys) {
        return store.put(keys.next(), value, KeyValueStore.NO_EXPIRY, WriteCondition.ALWAYS);
    }
}
