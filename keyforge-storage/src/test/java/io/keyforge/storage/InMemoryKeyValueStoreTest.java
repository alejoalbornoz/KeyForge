package io.keyforge.storage;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.WriteCondition.ALWAYS;
import static io.keyforge.storage.WriteCondition.IF_ABSENT;
import static io.keyforge.storage.WriteCondition.IF_PRESENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InMemoryKeyValueStoreTest {

    private static final long START = 1_700_000_000_000L;

    private final ManualClock clock = new ManualClock(START);
    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            InMemoryKeyValueStore.Options.defaults().withSegments(16).withActiveExpireInterval(Duration.ZERO)
                    .withClock(clock));

    @AfterEach
    void close() {
        store.close();
    }

    @Test
    void getReturnsWhatPutStored() {
        assertThat(store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS)).isTrue();

        assertThat(store.get(b("k"))).isEqualTo(b("v"));
        assertThat(store.get(b("missing"))).isNull();
    }

    @Test
    void keysAreComparedByContentNotIdentity() {
        store.put(b("k"), b("v1"), NO_EXPIRY, ALWAYS);
        store.put(b("k"), b("v2"), NO_EXPIRY, ALWAYS);

        assertThat(store.get(b("k"))).isEqualTo(b("v2"));
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void ifAbsentOnlyWritesMissingKeys() {
        assertThat(store.put(b("k"), b("first"), NO_EXPIRY, IF_ABSENT)).isTrue();
        assertThat(store.put(b("k"), b("second"), NO_EXPIRY, IF_ABSENT)).isFalse();

        assertThat(store.get(b("k"))).isEqualTo(b("first"));
    }

    @Test
    void ifPresentOnlyWritesExistingKeys() {
        assertThat(store.put(b("k"), b("v"), NO_EXPIRY, IF_PRESENT)).isFalse();
        assertThat(store.get(b("k"))).isNull();

        store.put(b("k"), b("v1"), NO_EXPIRY, ALWAYS);
        assertThat(store.put(b("k"), b("v2"), NO_EXPIRY, IF_PRESENT)).isTrue();
        assertThat(store.get(b("k"))).isEqualTo(b("v2"));
    }

    @Test
    void keysDisappearAtTheirExpiryInstant() {
        store.put(b("k"), b("v"), START + 100, ALWAYS);

        clock.advance(99);
        assertThat(store.get(b("k"))).isEqualTo(b("v"));
        clock.advance(1);
        assertThat(store.get(b("k"))).isNull();
        assertThat(store.size()).as("lazy expiry removed it").isZero();
    }

    @Test
    void expiredKeysCountAsAbsentForConditionalWrites() {
        store.put(b("k"), b("old"), START + 10, ALWAYS);
        clock.advance(10);

        assertThat(store.put(b("k"), b("v"), NO_EXPIRY, IF_PRESENT)).isFalse();
        assertThat(store.put(b("k"), b("new"), NO_EXPIRY, IF_ABSENT)).isTrue();
        assertThat(store.get(b("k"))).isEqualTo(b("new"));
    }

    @Test
    void aPlainPutRemovesAPreviousExpiry() {
        store.put(b("k"), b("v1"), START + 10, ALWAYS);
        store.put(b("k"), b("v2"), NO_EXPIRY, ALWAYS);

        clock.advance(1_000);
        assertThat(store.get(b("k"))).isEqualTo(b("v2"));
    }

    @Test
    void getAllReturnsValuesInOrderWithNullsForMissingKeys() {
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);
        store.put(b("c"), b("3"), NO_EXPIRY, ALWAYS);
        store.put(b("expired"), b("x"), START, ALWAYS);

        List<byte[]> values = store.getAll(List.of(b("a"), b("b"), b("c"), b("expired"), b("a")));

        assertThat(values).containsExactly(b("1"), null, b("3"), null, b("1"));
    }

    @Test
    void putAllWritesEveryPairAndClearsExpiries() {
        store.put(b("a"), b("old"), START + 10, ALWAYS);

        store.putAll(List.of(b("a"), b("b"), b("a")), List.of(b("1"), b("2"), b("3")));

        clock.advance(1_000);
        assertThat(store.get(b("a"))).isEqualTo(b("3"));
        assertThat(store.get(b("b"))).isEqualTo(b("2"));
    }

    @Test
    void putAllRejectsMismatchedLists() {
        assertThatThrownBy(() -> store.putAll(List.of(b("a")), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deleteCountsOnlyLiveKeysOnce() {
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);
        store.put(b("b"), b("2"), NO_EXPIRY, ALWAYS);
        store.put(b("expired"), b("x"), START, ALWAYS);

        assertThat(store.delete(List.of(b("a"), b("a"), b("expired"), b("missing")))).isEqualTo(1);
        assertThat(store.get(b("a"))).isNull();
        assertThat(store.get(b("b"))).isEqualTo(b("2"));
    }

    @Test
    void countExistingCountsDuplicates() {
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);

        assertThat(store.countExisting(List.of(b("a"), b("a"), b("missing")))).isEqualTo(2);
    }

    @Test
    void computeCreatesUpdatesAndRemoves() {
        assertThat(store.compute(b("k"), current -> b(current == null ? "created" : "never"))).isEqualTo(b("created"));
        assertThat(store.compute(b("k"), current -> concat(current, b("+")))).isEqualTo(b("created+"));
        assertThat(store.compute(b("k"), current -> null)).isNull();
        assertThat(store.get(b("k"))).isNull();
    }

    @Test
    void computeKeepsTheExistingExpiry() {
        store.put(b("k"), b("v"), START + 50, ALWAYS);

        store.compute(b("k"), current -> b("updated"));

        assertThat(store.get(b("k"))).isEqualTo(b("updated"));
        clock.advance(50);
        assertThat(store.get(b("k"))).isNull();
    }

    @Test
    void computeSeesExpiredKeysAsAbsent() {
        store.put(b("k"), b("old"), START + 5, ALWAYS);
        clock.advance(5);

        assertThat(store.compute(b("k"), current -> current == null ? b("fresh") : current)).isEqualTo(b("fresh"));
        clock.advance(1_000);
        assertThat(store.get(b("k"))).as("the new key has no expiry").isEqualTo(b("fresh"));
    }

    @Test
    void aFailingComputeLeavesTheValueUnchanged() {
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);

        assertThatThrownBy(() -> store.compute(b("k"), current -> {
            throw new NumberFormatException("not a number");
        })).isInstanceOf(NumberFormatException.class);
        assertThat(store.get(b("k"))).isEqualTo(b("v"));
    }

    @Test
    void sizeAndClear() {
        for (int i = 0; i < 100; i++) {
            store.put(b("k" + i), b("v"), NO_EXPIRY, ALWAYS);
        }
        assertThat(store.size()).isEqualTo(100);

        store.clear();

        assertThat(store.size()).isZero();
        assertThat(store.get(b("k1"))).isNull();
    }

    @Test
    void activeExpiryRemovesExpiredKeysThatAreNeverRead() {
        for (int i = 0; i < 1_000; i++) {
            store.put(b("short" + i), b("v"), START + 10, ALWAYS);
            store.put(b("long" + i), b("v"), START + 10_000, ALWAYS);
            store.put(b("forever" + i), b("v"), NO_EXPIRY, ALWAYS);
        }
        clock.advance(10);

        // Sampling is probabilistic: one cycle stops on a segment once few sampled keys are stale, so the last
        // stragglers are reclaimed by later cycles.
        int removed = store.expireCycle(Duration.ofSeconds(1).toNanos());
        assertThat(removed).as("first cycle").isGreaterThan(500);
        for (int cycle = 0; cycle < 500 && store.size() > 2_000; cycle++) {
            removed += store.expireCycle(Duration.ofSeconds(1).toNanos());
        }

        assertThat(removed).isEqualTo(1_000);
        assertThat(store.size()).isEqualTo(2_000);
        assertThat(store.get(b("long1"))).isNotNull();
        assertThat(store.get(b("forever1"))).isNotNull();
    }

    @Test
    void activeExpiryLeavesKeysThatHaveNotExpired() {
        for (int i = 0; i < 500; i++) {
            store.put(b("k" + i), b("v"), START + 10, ALWAYS);
        }

        assertThat(store.expireCycle(Duration.ofSeconds(1).toNanos())).isZero();
        assertThat(store.size()).isEqualTo(500);
    }

    @Test
    void backgroundActiveExpiryRunsOnItsOwn() throws InterruptedException {
        try (InMemoryKeyValueStore background = new InMemoryKeyValueStore(InMemoryKeyValueStore.Options.defaults()
                .withActiveExpireInterval(Duration.ofMillis(10)))) {
            long now = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                background.put(b("k" + i), b("v"), now + 20, ALWAYS);
            }

            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (background.size() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            assertThat(background.size()).isZero();
        }
    }

    @Test
    void optionsRejectSegmentCountsThatAreNotPowersOfTwo() {
        assertThatThrownBy(() -> InMemoryKeyValueStore.Options.defaults().withSegments(3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
