package io.keyforge.storage.lsm;

import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Kills a process that is writing to the store and checks that every acknowledged write survives.
 *
 * <p>Each round starts {@link CrashWorkload} in a child JVM on the same directory. Several threads write concurrently
 * (puts, conditional puts, multi-key puts, deletes and read-modify-writes) while memtables rotate and tables are flushed
 * and compacted. After a random delay the child is killed without warning ({@link Process#destroyForcibly()}: SIGKILL on
 * Linux, TerminateProcess on Windows). The test then opens the directory itself and checks every thread's keys: each
 * thread has at most one operation in flight, so its keys must be exactly the state after its last acknowledged
 * operation, or after the one in flight. Anything else is a lost acknowledged write, a torn multi-key write or a
 * resurrected key. Earlier rounds' keys must be untouched.
 */
@Timeout(300)
class CrashRecoveryIT {

    private static final int ROUNDS = 3;
    private static final int THREADS = 4;
    /** The kill comes at a random moment after the child has acknowledged at least this many writes. */
    private static final int MIN_ACKS_BEFORE_KILL = 40;

    @TempDir
    Path dir;

    @TempDir
    Path logs;

    @ParameterizedTest
    @EnumSource(FsyncPolicy.class)
    void everyAcknowledgedWriteSurvivesAKill(FsyncPolicy policy) throws Exception {
        Map<String, TreeMap<String, String>> survivors = new HashMap<>();
        for (int round = 0; round < ROUNDS; round++) {
            long[] lastAcked = runAndKill(policy, round);

            try (LsmKeyValueStore store = LsmKeyValueStore.open(CrashWorkload.options(dir, policy))) {
                for (int thread = 0; thread < THREADS; thread++) {
                    String prefix = CrashWorkload.prefix(round, thread);
                    TreeMap<String, String> actual = keysWithPrefix(store, prefix);
                    TreeMap<String, String> acknowledged = CrashWorkload.stateAfter(round, thread, lastAcked[thread]);
                    TreeMap<String, String> inFlight = CrashWorkload.stateAfter(round, thread, lastAcked[thread] + 1);
                    assertThat(actual.equals(acknowledged) || actual.equals(inFlight))
                            .as("round %d thread %d after %d acknowledged operations: %s", round, thread,
                                    lastAcked[thread] + 1, describe(actual, acknowledged, inFlight))
                            .isTrue();
                    survivors.put(prefix, actual);
                }
                for (Map.Entry<String, TreeMap<String, String>> earlier : survivors.entrySet()) {
                    assertThat(keysWithPrefix(store, earlier.getKey())).as("keys of %s", earlier.getKey())
                            .isEqualTo(earlier.getValue());
                }
            }
        }
    }

    /** Runs one round in a child JVM, kills it, and returns the last acknowledged operation index per thread. */
    private long[] runAndKill(FsyncPolicy policy, int round) throws Exception {
        Path stderr = logs.resolve(policy.configName() + "-round-" + round + ".err");
        Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-cp", System.getProperty("java.class.path"),
                CrashWorkload.class.getName(),
                dir.toString(), policy.configName(), Integer.toString(round), Integer.toString(THREADS))
                .redirectError(stderr.toFile())
                .start();
        long[] lastAcked = new long[THREADS];
        Arrays.fill(lastAcked, -1);
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch underLoad = new CountDownLatch(MIN_ACKS_BEFORE_KILL);
        Thread reader = Thread.ofPlatform().start(() -> {
            try (BufferedReader lines = new BufferedReader(
                    new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line = lines.readLine(); line != null; line = lines.readLine()) {
                    if (line.equals("ready")) {
                        ready.countDown();
                    } else if (line.startsWith("ack ")) {
                        String[] parts = line.split(" ");
                        int thread = Integer.parseInt(parts[1]);
                        synchronized (lastAcked) {
                            lastAcked[thread] = Math.max(lastAcked[thread], Long.parseLong(parts[2]));
                        }
                        underLoad.countDown();
                    }
                }
            } catch (IOException e) {
                // The pipe breaks when the child dies.
            }
        });
        try {
            if (!ready.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("The child never opened the store: " + Files.readString(stderr));
            }
            if (!underLoad.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("The child acknowledged too few writes: " + Files.readString(stderr));
            }
            Thread.sleep(ThreadLocalRandom.current().nextLong(100, 1_000));
            assertThat(child.isAlive()).as("child still writing; stderr: %s", Files.readString(stderr)).isTrue();
        } finally {
            child.destroyForcibly();
            child.waitFor(30, TimeUnit.SECONDS);
            reader.join(TimeUnit.SECONDS.toMillis(30));
        }
        synchronized (lastAcked) {
            return lastAcked.clone();
        }
    }

    private static TreeMap<String, String> keysWithPrefix(LsmKeyValueStore store, String prefix) {
        // The prefix ends with '-'; '.' is the next character, so [prefix, prefix with '.') covers exactly the prefix.
        String end = prefix.substring(0, prefix.length() - 1) + ".";
        TreeMap<String, String> keys = new TreeMap<>();
        for (KeyValue entry : store.scan(b(prefix), b(end), Integer.MAX_VALUE)) {
            keys.put(s(entry.key()), s(entry.value()));
        }
        return keys;
    }

    private static String describe(TreeMap<String, String> actual, TreeMap<String, String> acknowledged,
                                   TreeMap<String, String> inFlight) {
        List<String> differences = new ArrayList<>();
        for (String key : union(actual, acknowledged, inFlight)) {
            String a = actual.get(key);
            String ack = acknowledged.get(key);
            String flight = inFlight.get(key);
            if (!Objects.equals(a, ack) || !Objects.equals(a, flight)) {
                differences.add(key + ": actual=" + abbreviate(a) + " acknowledged=" + abbreviate(ack)
                        + " inFlight=" + abbreviate(flight));
            }
        }
        return String.join("; ", differences);
    }

    @SafeVarargs
    private static List<String> union(TreeMap<String, String>... maps) {
        TreeMap<String, Boolean> keys = new TreeMap<>();
        for (TreeMap<String, String> map : maps) {
            map.keySet().forEach(key -> keys.put(key, true));
        }
        return new ArrayList<>(keys.keySet());
    }

    private static String abbreviate(String value) {
        return value == null || value.length() <= 24 ? value : value.substring(0, 24) + "...(" + value.length() + ")";
    }
}
