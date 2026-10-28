package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.WriteCondition.ALWAYS;
import static io.keyforge.storage.WriteCondition.IF_ABSENT;
import static io.keyforge.storage.WriteCondition.IF_PRESENT;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.storage.ManualClock;
import io.keyforge.storage.lsm.LsmKeyValueStore.LogPosition;
import io.keyforge.storage.lsm.LsmKeyValueStore.StoredEntry;

/** The LSM engine as the state machine of a replicated log: no WAL, log positions, expiry at log time. */
@Timeout(60)
class LsmReplicatedModeTest {

    @TempDir
    Path dir;

    /** The local wall clock; replicated writes must ignore it. */
    private final ManualClock wallClock = new ManualClock(0);
    private final List<LsmKeyValueStore> opened = new ArrayList<>();

    @AfterEach
    void closeStores() {
        opened.forEach(LsmKeyValueStore::close);
    }

    private LsmKeyValueStore open(Path directory, long memtableBytes) {
        LsmKeyValueStore store = LsmKeyValueStore.open(LsmKeyValueStore.Options.defaults(directory)
                .withMemtableSizeBytes(memtableBytes)
                .withCompaction(1_000, 0)
                .withClock(wallClock)
                .withReplicated(true));
        opened.add(store);
        return store;
    }

    private LsmKeyValueStore open() {
        return open(dir, 1 << 20);
    }

    private static LogPosition at(long index) {
        return new LogPosition(index, 1_000 * index);
    }

    private long walFiles(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.toString().endsWith(".wal")).count();
        }
    }

    @Test
    void writesNoWriteAheadLog() throws IOException {
        LsmKeyValueStore store = open(dir, 2_000);
        for (int i = 1; i <= 200; i++) {
            store.put(at(i), b("key" + i), b("value" + i), NO_EXPIRY, ALWAYS);
        }
        store.flush();
        assertThat(walFiles(dir)).isZero();
        assertThat(store.durablePosition()).isEqualTo(at(200));
    }

    @Test
    void aCrashKeepsExactlyTheEntriesUpToTheDurablePosition() {
        // Small memtables: positions become durable as memtables are flushed, at arbitrary entry boundaries.
        LsmKeyValueStore store = open(dir, 3_000);
        for (int i = 1; i <= 300; i++) {
            if (i % 8 == 0) {
                // A read-modify-write: replaying it twice would count it twice.
                store.compute(at(i), b("counter"), current -> b(Integer.toString(
                        current == null ? 1 : Integer.parseInt(s(current)) + 1)));
            } else {
                store.put(at(i), b("key" + i), b("value" + i), NO_EXPIRY, ALWAYS);
            }
        }
        store.closeWithoutFlush();
        opened.remove(store);

        LsmKeyValueStore reopened = open(dir, 3_000);
        long durable = reopened.durablePosition().index();
        assertThat(durable).isBetween(1L, 300L);
        int increments = 0;
        for (int i = 1; i <= 300; i++) {
            boolean counterEntry = i % 8 == 0;
            if (counterEntry) {
                increments += i <= durable ? 1 : 0;
                continue;
            }
            assertThat(reopened.get(b("key" + i))).as("key %d with durable position %d", i, durable)
                    .isEqualTo(i <= durable ? b("value" + i) : null);
        }
        byte[] counter = reopened.get(b("counter"));
        assertThat(counter == null ? 0 : Integer.parseInt(s(counter))).isEqualTo(increments);
        assertThat(reopened.durablePosition().timeMillis()).isEqualTo(1_000 * durable);
    }

    @Test
    void conditionalWritesAndCountsDecideExpiryAtLogTimeNotTheWallClock() {
        LsmKeyValueStore store = open();
        store.put(new LogPosition(1, 500), b("k"), b("v"), 1_000, ALWAYS);

        // The wall clock is far past the expiry, but at log time 900 the key is still live.
        wallClock.advance(1_000_000);
        assertThat(store.put(new LogPosition(2, 900), b("k"), b("v2"), 1_000, IF_ABSENT)).isFalse();
        assertThat(store.put(new LogPosition(3, 950), b("k"), b("v3"), 1_000, IF_PRESENT)).isTrue();

        // The wall clock is far behind, but at log time 1,500 the key has expired.
        LsmKeyValueStore other = open(dir.resolve("other"), 1 << 20);
        wallClock.advance(-1_000_000);
        other.put(new LogPosition(1, 500), b("k"), b("v"), 1_000, ALWAYS);
        assertThat(other.delete(new LogPosition(2, 1_500), List.of(b("k")))).isZero();
        assertThat(other.compute(new LogPosition(3, 1_600), b("n"), current -> current == null ? b("1") : current))
                .isEqualTo(b("1"));
        // Reads never treat an applied log time as the future: k is gone although the wall clock says 0.
        assertThat(other.get(b("k"))).isNull();
    }

    @Test
    void flushesAndCompactionsDropOnlyWhatIsExpiredAtLogTime() {
        LsmKeyValueStore store = open();
        store.put(new LogPosition(1, 500), b("k"), b("v"), 1_000, ALWAYS);
        wallClock.advance(10_000_000); // a node whose clock runs far ahead
        store.compact();
        List<StoredEntry> exported = new ArrayList<>();
        store.export(exported::add);
        assertThat(exported).extracting(entry -> s(entry.key())).containsExactly("k");

        store.markApplied(new LogPosition(2, 2_000));
        store.compact();
        exported.clear();
        assertThat(store.export(exported::add)).isEqualTo(new LogPosition(2, 2_000));
        assertThat(exported).isEmpty();
    }

    @Test
    void exportAndRestoreRoundTrip() {
        LsmKeyValueStore source = open(dir.resolve("source"), 2_000);
        for (int i = 1; i <= 100; i++) {
            source.put(at(i), b("key" + i), b("value" + i), i % 2 == 0 ? NO_EXPIRY : 1_000_000, ALWAYS);
        }
        source.delete(at(101), List.of(b("key5"), b("key6")));
        List<StoredEntry> snapshot = new ArrayList<>();
        LogPosition position = source.export(snapshot::add);
        assertThat(position).isEqualTo(at(101));
        assertThat(snapshot).hasSize(98);

        LsmKeyValueStore target = open(dir.resolve("target"), 2_000);
        target.put(at(1), b("stale"), b("junk"), NO_EXPIRY, ALWAYS);
        target.restore(position, snapshot.iterator());
        assertThat(target.durablePosition()).isEqualTo(position);
        assertThat(target.get(b("stale"))).isNull();
        assertThat(target.get(b("key7"))).isEqualTo(b("value7"));
        assertThat(target.get(b("key5"))).isNull();
        target.closeWithoutFlush();
        opened.remove(target);

        LsmKeyValueStore reopened = open(dir.resolve("target"), 2_000);
        assertThat(reopened.durablePosition()).isEqualTo(position);
        List<StoredEntry> restored = new ArrayList<>();
        reopened.export(restored::add);
        assertThat(restored).extracting(entry -> s(entry.key()))
                .containsExactlyElementsOf(snapshot.stream().map(entry -> s(entry.key())).toList());
        assertThat(restored).extracting(StoredEntry::expireAtMillis)
                .containsExactlyElementsOf(snapshot.stream().map(StoredEntry::expireAtMillis).toList());
    }

    @Test
    void aClearIsDurableWithItsPositionAtOnce() {
        LsmKeyValueStore store = open();
        store.put(at(1), b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.clear(at(2));
        store.closeWithoutFlush();
        opened.remove(store);

        LsmKeyValueStore reopened = open();
        assertThat(reopened.durablePosition()).isEqualTo(at(2));
        assertThat(reopened.get(b("k"))).isNull();
    }

    @Test
    void aStandaloneStoreRecordsNoPosition() {
        LsmKeyValueStore store = LsmKeyValueStore.open(LsmKeyValueStore.Options.defaults(dir));
        opened.add(store);
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.flush();
        assertThat(store.durablePosition()).isEqualTo(LogPosition.NONE);
    }
}
