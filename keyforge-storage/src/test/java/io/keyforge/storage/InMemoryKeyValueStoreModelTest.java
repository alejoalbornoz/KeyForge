package io.keyforge.storage;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

/**
 * Model-based test: random operation sequences run against the store and against an obviously correct model (a
 * {@code HashMap} of values with expiry instants), with the clock moving and active expiry running in between. After
 * every step, every key must read the same from both. Few keys and few segments force collisions, overwrites and
 * swap-removals in the expiry index.
 */
class InMemoryKeyValueStoreModelTest {

    private static final long START = 1_000_000L;
    private static final List<String> KEYS = List.of("a", "b", "c", "d", "e", "f");

    sealed interface Op {
    }

    record Put(String key, String value, long ttl, WriteCondition condition) implements Op {
    }

    record PutAll(List<String> keys, String value) implements Op {
    }

    record Delete(List<String> keys) implements Op {
    }

    record Increment(String key) implements Op {
    }

    record AdvanceClock(long millis) implements Op {
    }

    record ActiveExpiry() implements Op {
    }

    record Clear() implements Op {
    }

    @Property(tries = 300)
    void storeBehavesLikeTheModel(@ForAll("operations") List<Op> operations) {
        ManualClock clock = new ManualClock(START);
        Map<String, Model> model = new HashMap<>();
        try (InMemoryKeyValueStore store = new InMemoryKeyValueStore(new InMemoryKeyValueStore.Options(
                2, Duration.ZERO, clock))) {
            for (Op op : operations) {
                apply(op, store, model, clock);
                for (String key : KEYS) {
                    Model expected = model.get(key);
                    String expectedValue = expected != null && expected.liveAt(clock.millis()) ? expected.value : null;
                    byte[] actual = store.get(b(key));
                    assertThat(actual == null ? null : new String(actual, StandardCharsets.UTF_8))
                            .as("key %s after %s", key, op)
                            .isEqualTo(expectedValue);
                }
            }
        }
    }

    private static void apply(Op op, InMemoryKeyValueStore store, Map<String, Model> model, ManualClock clock) {
        long now = clock.millis();
        switch (op) {
            case Put(String key, String value, long ttl, WriteCondition condition) -> {
                boolean live = model.containsKey(key) && model.get(key).liveAt(now);
                boolean shouldWrite = switch (condition) {
                    case ALWAYS -> true;
                    case IF_ABSENT -> !live;
                    case IF_PRESENT -> live;
                };
                long expireAt = ttl == 0 ? NO_EXPIRY : now + ttl;
                assertThat(store.put(b(key), b(value), expireAt, condition)).isEqualTo(shouldWrite);
                if (shouldWrite) {
                    model.put(key, new Model(value, expireAt));
                }
            }
            case PutAll(List<String> keys, String value) -> {
                store.putAll(keys.stream().map(InMemoryKeyValueStoreModelTest::b).toList(),
                        keys.stream().map(k -> b(value)).toList());
                keys.forEach(key -> model.put(key, new Model(value, NO_EXPIRY)));
            }
            case Delete(List<String> keys) -> {
                int expected = 0;
                for (String key : keys) {
                    Model removed = model.remove(key);
                    if (removed != null && removed.liveAt(now)) {
                        expected++;
                    }
                }
                assertThat(store.delete(keys.stream().map(InMemoryKeyValueStoreModelTest::b).toList()))
                        .isEqualTo(expected);
            }
            case Increment(String key) -> {
                // Appends a marker instead of parsing numbers: the point is the atomic read-modify-write path.
                Model current = model.get(key);
                boolean live = current != null && current.liveAt(now);
                String updated = (live ? current.value : "") + "+";
                store.compute(b(key), value -> b((value == null ? "" : new String(value, StandardCharsets.UTF_8)) + "+"));
                model.put(key, new Model(updated, live ? current.expireAt : NO_EXPIRY));
            }
            case AdvanceClock(long millis) -> clock.advance(millis);
            case ActiveExpiry() -> store.expireCycle(Duration.ofSeconds(1).toNanos());
            case Clear() -> {
                store.clear();
                model.clear();
            }
        }
    }

    record Model(String value, long expireAt) {
        boolean liveAt(long now) {
            return expireAt == NO_EXPIRY || expireAt > now;
        }
    }

    @Provide
    Arbitrary<List<Op>> operations() {
        Arbitrary<String> keys = Arbitraries.of(KEYS);
        Arbitrary<String> values = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(3);
        Arbitrary<Long> ttls = Arbitraries.oneOf(Arbitraries.just(0L), Arbitraries.longs().between(1, 50));
        Arbitrary<Op> put = Combinators.combine(keys, values, ttls, Arbitraries.of(WriteCondition.class))
                .as(Put::new);
        Arbitrary<Op> putAll = Combinators.combine(keys.list().ofMinSize(1).ofMaxSize(4), values).as(PutAll::new);
        Arbitrary<Op> delete = keys.list().ofMinSize(1).ofMaxSize(4).map(Delete::new);
        Arbitrary<Op> increment = keys.map(Increment::new);
        Arbitrary<Op> advance = Arbitraries.longs().between(1, 30).map(AdvanceClock::new);
        Arbitrary<Op> activeExpiry = Arbitraries.just(new ActiveExpiry());
        Arbitrary<Op> clear = Arbitraries.just(new Clear());
        return Arbitraries.frequencyOf(
                        Tuple.of(8, put),
                        Tuple.of(2, putAll),
                        Tuple.of(3, delete),
                        Tuple.of(3, increment),
                        Tuple.of(4, advance),
                        Tuple.of(3, activeExpiry),
                        Tuple.of(1, clear))
                .list().ofMaxSize(60);
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
