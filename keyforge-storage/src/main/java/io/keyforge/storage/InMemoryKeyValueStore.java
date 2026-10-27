package io.keyforge.storage;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;
import java.util.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory {@link KeyValueStore}: a hash table split into independently locked segments.
 *
 * <p><b>Concurrency.</b> Each key hashes to one segment, a plain {@link HashMap} guarded by its own lock. Single-key
 * operations lock one segment, so commands from different connections rarely contend. Multi-key operations lock every
 * segment they touch in ascending index order, which makes them atomic and rules out deadlock between them.
 *
 * <p><b>Expiry.</b> Expired keys are removed lazily, when an operation finds them, and actively, by a background task
 * adapted from Redis' {@code activeExpireCycle}: every interval it samples random keys that carry an expiry, deletes
 * the expired ones, and keeps sampling a segment while more than {@value #EXPIRE_ACCEPTABLE_STALE_PERCENT}% of a sample
 * was stale, within a time budget of {@value #EXPIRE_TIME_BUDGET_PERCENT}% of the interval. To sample in O(1) each
 * segment keeps the entries that carry an expiry in a dense array; every entry knows its slot, so removal is a swap with
 * the last slot.
 */
public final class InMemoryKeyValueStore implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    /** Keys checked per sampling round (Redis: {@code ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP}). */
    static final int EXPIRE_SAMPLE_SIZE = 20;
    /** Sampling continues while a round found more stale keys than this (Redis: {@code ..._ACCEPTABLE_STALE}). */
    static final int EXPIRE_ACCEPTABLE_STALE_PERCENT = 10;
    /** Share of each interval active expiry may use (Redis: {@code ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC}). */
    static final int EXPIRE_TIME_BUDGET_PERCENT = 25;

    /**
     * Construction options.
     *
     * @param segments             number of lock segments; a power of two
     * @param activeExpireInterval how often active expiry runs; {@link Duration#ZERO} disables it (lazy expiry only)
     * @param clock                wall clock that decides whether a key has expired
     */
    public record Options(int segments, Duration activeExpireInterval, InstantSource clock) {

        public static final int DEFAULT_SEGMENTS = 256;
        public static final Duration DEFAULT_ACTIVE_EXPIRE_INTERVAL = Duration.ofMillis(100);

        public Options {
            if (segments < 1 || segments > (1 << 16) || Integer.bitCount(segments) != 1) {
                throw new IllegalArgumentException("segments must be a power of two in [1, 65536]: " + segments);
            }
            Objects.requireNonNull(activeExpireInterval, "activeExpireInterval");
            if (activeExpireInterval.isNegative()) {
                throw new IllegalArgumentException("activeExpireInterval must not be negative");
            }
            Objects.requireNonNull(clock, "clock");
        }

        public static Options defaults() {
            return new Options(DEFAULT_SEGMENTS, DEFAULT_ACTIVE_EXPIRE_INTERVAL, InstantSource.system());
        }

        public Options withSegments(int newSegments) {
            return new Options(newSegments, activeExpireInterval, clock);
        }

        public Options withActiveExpireInterval(Duration newInterval) {
            return new Options(segments, newInterval, clock);
        }

        public Options withClock(InstantSource newClock) {
            return new Options(segments, activeExpireInterval, newClock);
        }
    }

    private final Segment[] segments;
    private final int segmentMask;
    private final InstantSource clock;
    private final ScheduledExecutorService activeExpiry;

    /** Next segment active expiry visits. Guarded by {@code this}. */
    private int expireCursor;

    public InMemoryKeyValueStore() {
        this(Options.defaults());
    }

    public InMemoryKeyValueStore(Options options) {
        segments = new Segment[options.segments()];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment();
        }
        segmentMask = segments.length - 1;
        clock = options.clock();

        long periodNanos = options.activeExpireInterval().toNanos();
        if (periodNanos == 0) {
            activeExpiry = null;
        } else {
            activeExpiry = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("keyforge-active-expire").factory());
            long budgetNanos = periodNanos * EXPIRE_TIME_BUDGET_PERCENT / 100;
            activeExpiry.scheduleWithFixedDelay(
                    () -> runActiveExpiry(budgetNanos), periodNanos, periodNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        Key k = new Key(key);
        Segment segment = segmentFor(k);
        segment.lock.lock();
        try {
            Entry entry = segment.live(k, clock.millis());
            return entry == null ? null : entry.value;
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public List<byte[]> getAll(List<byte[]> keys) {
        Key[] ks = keysOf(keys);
        int[] locked = lockSegmentsOf(ks);
        try {
            long now = clock.millis();
            List<byte[]> values = new ArrayList<>(ks.length);
            for (Key k : ks) {
                Entry entry = segmentFor(k).live(k, now);
                values.add(entry == null ? null : entry.value);
            }
            return values;
        } finally {
            unlock(locked);
        }
    }

    @Override
    public boolean put(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
        Objects.requireNonNull(value, "value");
        Key k = new Key(key);
        Segment segment = segmentFor(k);
        segment.lock.lock();
        try {
            Entry entry = segment.live(k, clock.millis());
            boolean allowed = switch (condition) {
                case ALWAYS -> true;
                case IF_ABSENT -> entry == null;
                case IF_PRESENT -> entry != null;
            };
            if (!allowed) {
                return false;
            }
            if (entry == null) {
                entry = new Entry(k, value);
                segment.entries.put(k, entry);
            } else {
                entry.value = value;
            }
            segment.setExpiry(entry, expireAtMillis);
            return true;
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values) {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(keys.size() + " keys but " + values.size() + " values");
        }
        Key[] ks = keysOf(keys);
        int[] locked = lockSegmentsOf(ks);
        try {
            for (int i = 0; i < ks.length; i++) {
                byte[] value = Objects.requireNonNull(values.get(i), "value");
                Segment segment = segmentFor(ks[i]);
                Entry entry = segment.entries.get(ks[i]);
                if (entry == null) {
                    segment.entries.put(ks[i], new Entry(ks[i], value));
                } else {
                    entry.value = value;
                    segment.setExpiry(entry, NO_EXPIRY);
                }
            }
        } finally {
            unlock(locked);
        }
    }

    @Override
    public byte[] compute(byte[] key, UnaryOperator<byte[]> remapping) {
        Key k = new Key(key);
        Segment segment = segmentFor(k);
        segment.lock.lock();
        try {
            Entry entry = segment.live(k, clock.millis());
            byte[] updated = remapping.apply(entry == null ? null : entry.value);
            if (updated == null) {
                if (entry != null) {
                    segment.remove(entry);
                }
            } else if (entry == null) {
                segment.entries.put(k, new Entry(k, updated));
            } else {
                entry.value = updated;
            }
            return updated;
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public int delete(List<byte[]> keys) {
        Key[] ks = keysOf(keys);
        int[] locked = lockSegmentsOf(ks);
        try {
            long now = clock.millis();
            int deleted = 0;
            for (Key k : ks) {
                Segment segment = segmentFor(k);
                Entry entry = segment.live(k, now);
                if (entry != null) {
                    segment.remove(entry);
                    deleted++;
                }
            }
            return deleted;
        } finally {
            unlock(locked);
        }
    }

    @Override
    public int countExisting(List<byte[]> keys) {
        Key[] ks = keysOf(keys);
        int[] locked = lockSegmentsOf(ks);
        try {
            long now = clock.millis();
            int existing = 0;
            for (Key k : ks) {
                if (segmentFor(k).live(k, now) != null) {
                    existing++;
                }
            }
            return existing;
        } finally {
            unlock(locked);
        }
    }

    @Override
    public long size() {
        lockAll();
        try {
            long size = 0;
            for (Segment segment : segments) {
                size += segment.entries.size();
            }
            return size;
        } finally {
            unlockAll();
        }
    }

    @Override
    public void clear() {
        lockAll();
        try {
            for (Segment segment : segments) {
                segment.clear();
            }
        } finally {
            unlockAll();
        }
    }

    @Override
    public void close() {
        if (activeExpiry == null) {
            return;
        }
        activeExpiry.shutdownNow();
        try {
            if (!activeExpiry.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Active expiry task did not stop within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Runs one active-expiry cycle and returns how many keys it removed. Segments are visited round-robin, resuming
     * where the previous cycle stopped; the cycle ends after visiting every segment once or when the budget is spent.
     * Locks are held for one sampling round at a time, so foreground commands interleave with the cycle.
     */
    synchronized int expireCycle(long budgetNanos) {
        long deadline = System.nanoTime() + budgetNanos;
        int removed = 0;
        for (int visited = 0; visited < segments.length; visited++) {
            Segment segment = segments[expireCursor];
            expireCursor = (expireCursor + 1) & segmentMask;
            removed += expireSegment(segment, deadline);
            if (System.nanoTime() - deadline >= 0) {
                break;
            }
        }
        return removed;
    }

    private int expireSegment(Segment segment, long deadline) {
        int removed = 0;
        while (true) {
            int expired;
            boolean exhaustive;
            segment.lock.lock();
            try {
                long now = clock.millis();
                exhaustive = segment.volatileCount <= EXPIRE_SAMPLE_SIZE;
                expired = exhaustive
                        ? segment.expireAll(now)
                        : segment.expireSample(now, EXPIRE_SAMPLE_SIZE, ThreadLocalRandom.current());
            } finally {
                segment.lock.unlock();
            }
            removed += expired;
            boolean mostlyFresh = expired * 100 <= EXPIRE_SAMPLE_SIZE * EXPIRE_ACCEPTABLE_STALE_PERCENT;
            if (exhaustive || mostlyFresh || System.nanoTime() - deadline >= 0) {
                return removed;
            }
        }
    }

    private void runActiveExpiry(long budgetNanos) {
        try {
            int removed = expireCycle(budgetNanos);
            if (removed > 0 && log.isTraceEnabled()) {
                log.trace("Active expiry removed {} keys", removed);
            }
        } catch (RuntimeException e) {
            // An exception would cancel the periodic task; log it and try again next interval.
            log.error("Active expiry cycle failed", e);
        }
    }

    private Segment segmentFor(Key key) {
        return segments[segmentIndex(key)];
    }

    private int segmentIndex(Key key) {
        // Use the high bits of a multiplicative hash so the segment choice is independent of the HashMap bucket.
        return ((key.hash * 0x9E3779B9) >>> 16) & segmentMask;
    }

    private static Key[] keysOf(List<byte[]> keys) {
        Key[] result = new Key[keys.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = new Key(keys.get(i));
        }
        return result;
    }

    /** Locks the segments of {@code keys} in ascending order and returns their indexes for {@link #unlock}. */
    private int[] lockSegmentsOf(Key[] keys) {
        int[] indexes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            indexes[i] = segmentIndex(keys[i]);
        }
        Arrays.sort(indexes);
        int distinct = 0;
        for (int i = 0; i < indexes.length; i++) {
            if (i == 0 || indexes[i] != indexes[i - 1]) {
                indexes[distinct++] = indexes[i];
            }
        }
        int[] order = distinct == indexes.length ? indexes : Arrays.copyOf(indexes, distinct);
        for (int index : order) {
            segments[index].lock.lock();
        }
        return order;
    }

    private void unlock(int[] order) {
        for (int i = order.length - 1; i >= 0; i--) {
            segments[order[i]].lock.unlock();
        }
    }

    private void lockAll() {
        for (Segment segment : segments) {
            segment.lock.lock();
        }
    }

    private void unlockAll() {
        for (int i = segments.length - 1; i >= 0; i--) {
            segments[i].lock.unlock();
        }
    }

    /** A key with its hash cached; byte arrays have identity equality, so they cannot be map keys directly. */
    static final class Key {
        final byte[] bytes;
        final int hash;

        Key(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "key");
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Key that && hash == that.hash && Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** A stored value. Mutable, and only touched while its segment's lock is held. */
    static final class Entry {
        final Key key;
        byte[] value;
        long expireAt = NO_EXPIRY;
        /** Index in {@link Segment#volatileEntries}, or -1 if the entry has no expiry. */
        int volatileSlot = -1;

        Entry(Key key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        boolean isExpiredAt(long now) {
            return expireAt != NO_EXPIRY && expireAt <= now;
        }
    }

    static final class Segment {
        private static final Entry[] NO_ENTRIES = new Entry[0];
        private static final int MIN_VOLATILE_CAPACITY = 16;

        final ReentrantLock lock = new ReentrantLock();
        final HashMap<Key, Entry> entries = new HashMap<>();
        /** Dense array of the entries that carry an expiry, for O(1) random sampling. */
        Entry[] volatileEntries = NO_ENTRIES;
        int volatileCount;

        /** Returns the live entry for {@code key}, removing it first if it has expired. */
        Entry live(Key key, long now) {
            Entry entry = entries.get(key);
            if (entry != null && entry.isExpiredAt(now)) {
                remove(entry);
                return null;
            }
            return entry;
        }

        void remove(Entry entry) {
            entries.remove(entry.key);
            untrack(entry);
        }

        void setExpiry(Entry entry, long expireAt) {
            entry.expireAt = expireAt;
            if (expireAt == NO_EXPIRY) {
                untrack(entry);
            } else {
                track(entry);
            }
        }

        int expireAll(long now) {
            int expired = 0;
            // Walk backwards: removal swaps the last entry into the freed slot, and that one was already checked.
            for (int i = volatileCount - 1; i >= 0; i--) {
                Entry entry = volatileEntries[i];
                if (entry.isExpiredAt(now)) {
                    remove(entry);
                    expired++;
                }
            }
            return expired;
        }

        int expireSample(long now, int samples, RandomGenerator random) {
            int expired = 0;
            for (int i = 0; i < samples && volatileCount > 0; i++) {
                Entry entry = volatileEntries[random.nextInt(volatileCount)];
                if (entry.isExpiredAt(now)) {
                    remove(entry);
                    expired++;
                }
            }
            return expired;
        }

        void clear() {
            entries.clear();
            volatileEntries = NO_ENTRIES;
            volatileCount = 0;
        }

        private void track(Entry entry) {
            if (entry.volatileSlot >= 0) {
                return;
            }
            if (volatileCount == volatileEntries.length) {
                volatileEntries = Arrays.copyOf(volatileEntries, Math.max(MIN_VOLATILE_CAPACITY, volatileCount * 2));
            }
            volatileEntries[volatileCount] = entry;
            entry.volatileSlot = volatileCount++;
        }

        private void untrack(Entry entry) {
            int slot = entry.volatileSlot;
            if (slot < 0) {
                return;
            }
            Entry last = volatileEntries[--volatileCount];
            volatileEntries[slot] = last;
            last.volatileSlot = slot;
            volatileEntries[volatileCount] = null;
            entry.volatileSlot = -1;
            if (volatileEntries.length > 4 * MIN_VOLATILE_CAPACITY && volatileCount < volatileEntries.length / 4) {
                volatileEntries = Arrays.copyOf(volatileEntries, volatileEntries.length / 2);
            }
        }
    }
}
