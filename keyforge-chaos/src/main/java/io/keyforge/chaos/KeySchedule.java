package io.keyforge.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.random.RandomGenerator;

/**
 * Which key each operation uses: a small set of keys in use at once, so that clients contend on them, each retired
 * after a fixed number of operations and replaced with a fresh key (Jepsen's "independent keys").
 *
 * <p>Checking linearizability is NP-hard in the length of a key's history, and an indeterminate write stays open until
 * the end of it. Rotating keys bounds both: the checker verifies many short histories, one per key, instead of a few
 * long ones. Thread-safe.
 */
final class KeySchedule {

    private final String prefix;
    private final int opsPerKey;
    private final String[] active;
    private final int[] uses;
    private final List<String> issued = new ArrayList<>();

    /**
     * @param prefix    starts every key name, to keep runs apart
     * @param slots     keys in use at once
     * @param opsPerKey operations a key receives before it is replaced
     */
    KeySchedule(String prefix, int slots, int opsPerKey) {
        this.prefix = prefix;
        this.opsPerKey = opsPerKey;
        this.active = new String[slots];
        this.uses = new int[slots];
        for (int slot = 0; slot < slots; slot++) {
            active[slot] = fresh();
        }
    }

    /** The key for the next operation: one of the keys in use, chosen at random. */
    synchronized String next(RandomGenerator random) {
        int slot = random.nextInt(active.length);
        String key = active[slot];
        if (++uses[slot] >= opsPerKey) {
            active[slot] = fresh();
            uses[slot] = 0;
        }
        return key;
    }

    /** Every key handed out so far, and those in use, in the order they were created. */
    synchronized List<String> issued() {
        return List.copyOf(issued);
    }

    private String fresh() {
        String key = prefix + issued.size();
        issued.add(key);
        return key;
    }
}
