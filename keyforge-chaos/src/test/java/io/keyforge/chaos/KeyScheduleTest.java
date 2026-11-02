package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class KeyScheduleTest {

    @Test
    void everyKeyGetsExactlyItsShareOfOperationsBeforeItIsReplaced() {
        KeySchedule keys = new KeySchedule("k", 3, 10);
        SplittableRandom random = new SplittableRandom(1);
        Map<String, Integer> uses = new HashMap<>();

        for (int i = 0; i < 300; i++) {
            uses.merge(keys.next(random), 1, Integer::sum);
        }

        List<String> issued = keys.issued();
        assertThat(issued).first().isEqualTo("k0");
        assertThat(issued).doesNotHaveDuplicates().allMatch(key -> key.startsWith("k"));
        // Retired keys got exactly 10 operations, keys still in use fewer; 3 keys are always in use.
        assertThat(uses.values()).allMatch(count -> count <= 10);
        long retired = uses.values().stream().filter(count -> count == 10).count();
        assertThat(retired).as("each slot retires a key per 10 of its operations").isBetween(28L, 30L);
        assertThat(issued).hasSize(3 + (int) retired).containsAll(uses.keySet());
    }

    @Test
    void keysInUseAreSharedSafelyBetweenThreads() throws InterruptedException {
        KeySchedule keys = new KeySchedule("k", 4, 25);
        Map<String, AtomicInteger> uses = new ConcurrentHashMap<>();
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            SplittableRandom random = new SplittableRandom(t);
            threads[t] = Thread.ofPlatform().start(() -> {
                for (int i = 0; i < 1000; i++) {
                    uses.computeIfAbsent(keys.next(random), key -> new AtomicInteger()).incrementAndGet();
                }
            });
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(uses.values()).allMatch(count -> count.get() <= 25);
        assertThat(uses.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(8000);
    }
}
