package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TreeMap;

import io.keyforge.storage.WriteCondition;

/**
 * The deterministic write workload of the crash test. Operation {@code i} of thread {@code t} in round {@code r} is a
 * pure function of {@code (r, t, i)}, so the test can replay any prefix of what a thread did into a model.
 *
 * <p>Run as a program, it opens a store and has every thread apply its operations in order, one at a time, printing
 * {@code ack <thread> <i>} after each one returns, until the process is killed.
 */
final class CrashWorkload {

    static final int KEYS_PER_THREAD = 40;

    sealed interface Op {
    }

    record Put(String key, String value, WriteCondition condition) implements Op {
    }

    record PutAll(List<String> keys, String value) implements Op {
    }

    record Delete(List<String> keys) implements Op {
    }

    record Append(String key) implements Op {
    }

    private CrashWorkload() {
    }

    static String prefix(int round, int thread) {
        return "r" + round + "-t" + thread + "-";
    }

    static Op op(int round, int thread, long index) {
        SplittableRandom random = new SplittableRandom(Murmur3.fmix64(round * 1_000_003L + thread * 7_919L) ^ index);
        String prefix = prefix(round, thread);
        int choice = random.nextInt(100);
        String key = prefix + random.nextInt(KEYS_PER_THREAD);
        if (choice < 50) {
            WriteCondition condition = choice < 40 ? WriteCondition.ALWAYS
                    : choice < 45 ? WriteCondition.IF_ABSENT : WriteCondition.IF_PRESENT;
            return new Put(key, value(random, index), condition);
        }
        if (choice < 70) {
            List<String> keys = new ArrayList<>();
            int count = 2 + random.nextInt(5);
            for (int i = 0; i < count; i++) {
                keys.add(prefix + random.nextInt(KEYS_PER_THREAD));
            }
            return new PutAll(keys, value(random, index));
        }
        if (choice < 85) {
            List<String> keys = new ArrayList<>();
            int count = 1 + random.nextInt(3);
            for (int i = 0; i < count; i++) {
                keys.add(prefix + random.nextInt(KEYS_PER_THREAD));
            }
            return new Delete(keys);
        }
        return new Append(key);
    }

    /** Values vary in size, up to a few KiB, so memtables fill and flushes and compactions run during the test. */
    private static String value(SplittableRandom random, long index) {
        return index + ":" + "x".repeat(random.nextInt(random.nextInt(10) == 0 ? 4_000 : 200));
    }

    static void apply(Op op, LsmKeyValueStore store) {
        switch (op) {
            case Put(String key, String value, WriteCondition condition) -> store.put(b(key), b(value), NO_EXPIRY,
                    condition);
            case PutAll(List<String> keys, String value) -> store.putAll(keys.stream().map(Bytes::b).toList(),
                    keys.stream().map(k -> b(value)).toList());
            case Delete(List<String> keys) -> store.delete(keys.stream().map(Bytes::b).toList());
            case Append(String key) -> store.compute(b(key), current -> b((current == null ? "" : s(current)) + "+"));
        }
    }

    static void apply(Op op, TreeMap<String, String> model) {
        switch (op) {
            case Put(String key, String value, WriteCondition condition) -> {
                boolean present = model.containsKey(key);
                if (condition == WriteCondition.ALWAYS || (condition == WriteCondition.IF_ABSENT) != present) {
                    model.put(key, value);
                }
            }
            case PutAll(List<String> keys, String value) -> keys.forEach(key -> model.put(key, value));
            case Delete(List<String> keys) -> keys.forEach(model::remove);
            case Append(String key) -> model.put(key, model.getOrDefault(key, "") + "+");
        }
    }

    /** The state of one thread's keys after its operations {@code 0..lastIndex} (none if {@code lastIndex < 0}). */
    static TreeMap<String, String> stateAfter(int round, int thread, long lastIndex) {
        TreeMap<String, String> model = new TreeMap<>();
        for (long i = 0; i <= lastIndex; i++) {
            apply(op(round, thread, i), model);
        }
        return model;
    }

    static LsmKeyValueStore.Options options(Path dir, FsyncPolicy policy) {
        return LsmKeyValueStore.Options.defaults(dir)
                .withFsyncPolicy(policy)
                .withGroupCommit(Duration.ofMillis(5), 256 * 1024)
                .withMemtableSizeBytes(64 * 1024)
                .withCompaction(3, 1 << 20);
    }

    /** {@code CrashWorkload <directory> <always|group-commit> <round> <threads>} */
    public static void main(String[] args) {
        Path dir = Path.of(args[0]);
        FsyncPolicy policy = FsyncPolicy.fromConfigName(args[1]);
        int round = Integer.parseInt(args[2]);
        int threads = Integer.parseInt(args[3]);
        PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8);

        LsmKeyValueStore store = LsmKeyValueStore.open(options(dir, policy));
        out.println("ready");
        for (int t = 0; t < threads; t++) {
            int thread = t;
            Thread.ofPlatform().name("crash-writer-" + t).start(() -> {
                for (long i = 0; ; i++) {
                    apply(op(round, thread, i), store);
                    out.println("ack " + thread + " " + i);
                }
            });
        }
        // The writer threads are non-daemon: the process runs until the test kills it.
    }
}
