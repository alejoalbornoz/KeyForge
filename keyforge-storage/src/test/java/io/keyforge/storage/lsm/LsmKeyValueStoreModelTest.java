package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.lifecycle.AfterTry;
import net.jqwik.api.lifecycle.BeforeTry;

import io.keyforge.storage.ManualClock;
import io.keyforge.storage.WriteCondition;

/**
 * Model-based test: random operation sequences run against the LSM store and against a {@link TreeMap} of values with
 * expiry instants. Restarts (close and reopen on the same directory), flushes, compactions and clock movements are
 * interleaved with the writes. Memtables are tiny and compaction is eager, so the background threads rotate, flush and
 * merge while the sequence runs. After every step every key, the key count and a full ordered scan must agree with the
 * model.
 */
class LsmKeyValueStoreModelTest {

    private static final long START = 1_000_000L;
    private static final List<String> KEYS = List.of("a", "b", "c", "d", "e", "f", "g", "h");

    sealed interface Op {
    }

    record Put(String key, String value, long ttl, WriteCondition condition) implements Op {
    }

    record PutAll(List<String> keys, String value) implements Op {
    }

    record Delete(List<String> keys) implements Op {
    }

    record Append(String key) implements Op {
    }

    record AdvanceClock(long millis) implements Op {
    }

    record Flush() implements Op {
    }

    record Compact() implements Op {
    }

    record Restart() implements Op {
    }

    record Clear() implements Op {
    }

    record Scan(String from, String to, int limit) implements Op {
    }

    private Path dir;

    @BeforeTry
    void createDirectory() throws IOException {
        dir = Files.createTempDirectory("lsm-model");
    }

    @AfterTry
    void deleteDirectory() throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Property(tries = 150)
    void storeBehavesLikeASortedMap(@ForAll("operations") List<Op> operations) {
        ManualClock clock = new ManualClock(START);
        LsmKeyValueStore.Options options = LsmKeyValueStore.Options.defaults(dir)
                .withClock(clock)
                .withMemtableSizeBytes(600)
                .withCompaction(2, 1 << 20)
                .withGroupCommit(Duration.ofMillis(1), 4096);
        TreeMap<String, Model> model = new TreeMap<>();
        LsmKeyValueStore store = LsmKeyValueStore.open(options);
        try {
            for (Op op : operations) {
                if (op instanceof Restart) {
                    store.close();
                    store = LsmKeyValueStore.open(options);
                } else {
                    apply(op, store, model, clock);
                }
                check(store, model, clock.millis(), op);
            }
        } finally {
            store.close();
        }
    }

    private static void apply(Op op, LsmKeyValueStore store, TreeMap<String, Model> model, ManualClock clock) {
        long now = clock.millis();
        switch (op) {
            case Put(String key, String value, long ttl, WriteCondition condition) -> {
                boolean live = isLive(model.get(key), now);
                boolean shouldWrite = switch (condition) {
                    case ALWAYS -> true;
                    case IF_ABSENT -> !live;
                    case IF_PRESENT -> live;
                };
                long expireAt = ttl == 0 ? NO_EXPIRY : now + ttl;
                assertThat(store.put(b(key), b(value), expireAt, condition)).as("%s", op).isEqualTo(shouldWrite);
                if (shouldWrite) {
                    model.put(key, new Model(value, expireAt));
                }
            }
            case PutAll(List<String> keys, String value) -> {
                store.putAll(keys.stream().map(Bytes::b).toList(), keys.stream().map(k -> b(value)).toList());
                keys.forEach(key -> model.put(key, new Model(value, NO_EXPIRY)));
            }
            case Delete(List<String> keys) -> {
                int expected = 0;
                for (String key : keys) {
                    Model removed = model.remove(key);
                    if (isLive(removed, now)) {
                        expected++;
                    }
                }
                assertThat(store.delete(keys.stream().map(Bytes::b).toList())).as("%s", op).isEqualTo(expected);
            }
            case Append(String key) -> {
                Model current = model.get(key);
                boolean live = isLive(current, now);
                String updated = (live ? current.value : "") + "+";
                byte[] result = store.compute(b(key), value -> b((value == null ? "" : s(value)) + "+"));
                assertThat(s(result)).isEqualTo(updated);
                model.put(key, new Model(updated, live ? current.expireAt : NO_EXPIRY));
            }
            case AdvanceClock(long millis) -> clock.advance(millis);
            case Flush() -> store.flush();
            case Compact() -> store.compact();
            case Clear() -> {
                store.clear();
                model.clear();
            }
            case Scan(String from, String to, int limit) -> {
                List<String> expected = new ArrayList<>();
                for (Map.Entry<String, Model> entry : model.subMap(from, true, to, false).entrySet()) {
                    if (expected.size() < limit && isLive(entry.getValue(), now)) {
                        expected.add(entry.getKey() + "=" + entry.getValue().value);
                    }
                }
                assertThat(render(store.scan(b(from), b(to), limit))).as("%s", op).isEqualTo(expected);
            }
            case Restart() -> throw new IllegalStateException("handled by the caller");
        }
    }

    private static void check(LsmKeyValueStore store, TreeMap<String, Model> model, long now, Op after) {
        List<String> expected = new ArrayList<>();
        for (String key : KEYS) {
            Model entry = model.get(key);
            String expectedValue = isLive(entry, now) ? entry.value : null;
            assertThat(s(store.get(b(key)))).as("key %s after %s", key, after).isEqualTo(expectedValue);
            if (expectedValue != null) {
                expected.add(key + "=" + expectedValue);
            }
        }
        assertThat(render(store.scan(null, null, Integer.MAX_VALUE))).as("full scan after %s", after)
                .isEqualTo(expected);
        assertThat(store.size()).as("size after %s", after).isEqualTo(expected.size());
    }

    private static List<String> render(List<KeyValue> entries) {
        return entries.stream().map(entry -> s(entry.key()) + "=" + s(entry.value())).toList();
    }

    private static boolean isLive(Model model, long now) {
        return model != null && (model.expireAt == NO_EXPIRY || model.expireAt > now);
    }

    record Model(String value, long expireAt) {
    }

    @Provide
    Arbitrary<List<Op>> operations() {
        Arbitrary<String> keys = Arbitraries.of(KEYS);
        Arbitrary<String> values = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(4);
        Arbitrary<Long> ttls = Arbitraries.oneOf(Arbitraries.just(0L), Arbitraries.longs().between(1, 50));
        Arbitrary<Op> put = Combinators.combine(keys, values, ttls, Arbitraries.of(WriteCondition.class))
                .as(Put::new);
        Arbitrary<Op> putAll = Combinators.combine(keys.list().ofMinSize(1).ofMaxSize(4), values).as(PutAll::new);
        Arbitrary<Op> delete = keys.list().ofMinSize(1).ofMaxSize(4).map(Delete::new);
        Arbitrary<Op> append = keys.map(Append::new);
        Arbitrary<Op> advance = Arbitraries.longs().between(1, 30).map(AdvanceClock::new);
        Arbitrary<Op> scan = Combinators.combine(keys, keys, Arbitraries.integers().between(0, 5))
                .as((from, to, limit) -> from.compareTo(to) <= 0 ? new Scan(from, to, limit) : new Scan(to, from, limit));
        return Arbitraries.frequencyOf(
                        Tuple.of(10, put),
                        Tuple.of(3, putAll),
                        Tuple.of(4, delete),
                        Tuple.of(3, append),
                        Tuple.of(3, advance),
                        Tuple.of(2, Arbitraries.just(new Flush())),
                        Tuple.of(1, Arbitraries.just(new Compact())),
                        Tuple.of(2, Arbitraries.just(new Restart())),
                        Tuple.of(1, Arbitraries.just(new Clear())),
                        Tuple.of(2, scan))
                .list().ofMaxSize(50);
    }
}
