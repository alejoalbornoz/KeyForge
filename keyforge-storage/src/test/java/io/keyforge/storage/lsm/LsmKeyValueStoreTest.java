package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.WriteCondition.ALWAYS;
import static io.keyforge.storage.WriteCondition.IF_ABSENT;
import static io.keyforge.storage.WriteCondition.IF_PRESENT;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.storage.ManualClock;

@Timeout(60)
class LsmKeyValueStoreTest {

    private static final long START = 1_700_000_000_000L;
    /** Disables background compaction, so tests control the table layout. */
    private static final int NO_BACKGROUND_COMPACTION = 1_000;

    @TempDir
    Path dir;

    private final ManualClock clock = new ManualClock(START);
    private final List<LsmKeyValueStore> opened = new ArrayList<>();

    @AfterEach
    void closeStores() {
        opened.forEach(LsmKeyValueStore::close);
    }

    // --- The KeyValueStore contract, as for the in-memory engine -----------------------------------------------------

    @Test
    void getReturnsWhatPutStored() {
        LsmKeyValueStore store = open();
        assertThat(store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS)).isTrue();

        assertThat(store.get(b("k"))).isEqualTo(b("v"));
        assertThat(store.get(b("missing"))).isNull();
    }

    @Test
    void keysAreComparedByContentNotIdentity() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v1"), NO_EXPIRY, ALWAYS);
        store.put(b("k"), b("v2"), NO_EXPIRY, ALWAYS);

        assertThat(store.get(b("k"))).isEqualTo(b("v2"));
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void conditionalWritesSeeKeysWhereverTheyLive() {
        LsmKeyValueStore store = open();
        assertThat(store.put(b("k"), b("first"), NO_EXPIRY, IF_ABSENT)).isTrue();
        assertThat(store.put(b("k"), b("second"), NO_EXPIRY, IF_ABSENT)).isFalse();
        assertThat(store.put(b("other"), b("v"), NO_EXPIRY, IF_PRESENT)).isFalse();
        store.flush();

        assertThat(store.put(b("k"), b("third"), NO_EXPIRY, IF_ABSENT)).as("key in an SSTable").isFalse();
        assertThat(store.put(b("k"), b("fourth"), NO_EXPIRY, IF_PRESENT)).isTrue();
        assertThat(store.get(b("k"))).isEqualTo(b("fourth"));
        assertThat(store.get(b("other"))).isNull();
    }

    @Test
    void keysDisappearAtTheirExpiryInstant() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v"), START + 100, ALWAYS);

        clock.advance(99);
        assertThat(store.get(b("k"))).isEqualTo(b("v"));
        clock.advance(1);
        assertThat(store.get(b("k"))).isNull();
        assertThat(store.size()).as("expired keys are not counted").isZero();
    }

    @Test
    void expiredKeysCountAsAbsentForConditionalWrites() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("old"), START + 10, ALWAYS);
        clock.advance(10);

        assertThat(store.put(b("k"), b("v"), NO_EXPIRY, IF_PRESENT)).isFalse();
        assertThat(store.put(b("k"), b("new"), NO_EXPIRY, IF_ABSENT)).isTrue();
        assertThat(store.get(b("k"))).isEqualTo(b("new"));
    }

    @Test
    void aPlainPutRemovesAPreviousExpiry() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v1"), START + 10, ALWAYS);
        store.put(b("k"), b("v2"), NO_EXPIRY, ALWAYS);

        clock.advance(1_000);
        assertThat(store.get(b("k"))).isEqualTo(b("v2"));
    }

    @Test
    void getAllReturnsValuesInOrderWithNullsForMissingKeys() {
        LsmKeyValueStore store = open();
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.put(b("c"), b("3"), NO_EXPIRY, ALWAYS);
        store.put(b("expired"), b("x"), START, ALWAYS);

        List<byte[]> values = store.getAll(List.of(b("a"), b("b"), b("c"), b("expired"), b("a")));

        assertThat(values).containsExactly(b("1"), null, b("3"), null, b("1"));
    }

    @Test
    void putAllWritesEveryPairAndClearsExpiries() {
        LsmKeyValueStore store = open();
        store.put(b("a"), b("old"), START + 10, ALWAYS);

        store.putAll(List.of(b("a"), b("b"), b("a")), List.of(b("1"), b("2"), b("3")));

        clock.advance(1_000);
        assertThat(store.get(b("a"))).as("later duplicates win").isEqualTo(b("3"));
        assertThat(store.get(b("b"))).isEqualTo(b("2"));
    }

    @Test
    void putAllRejectsMismatchedLists() {
        LsmKeyValueStore store = open();

        assertThatThrownBy(() -> store.putAll(List.of(b("a")), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deleteCountsOnlyLiveKeysOnce() {
        LsmKeyValueStore store = open();
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);
        store.put(b("b"), b("2"), NO_EXPIRY, ALWAYS);
        store.put(b("expired"), b("x"), START, ALWAYS);
        store.flush();

        assertThat(store.delete(List.of(b("a"), b("a"), b("expired"), b("missing")))).isEqualTo(1);
        assertThat(store.get(b("a"))).isNull();
        assertThat(store.get(b("b"))).isEqualTo(b("2"));
        assertThat(store.delete(List.of(b("a")))).isZero();
    }

    @Test
    void countExistingCountsDuplicates() {
        LsmKeyValueStore store = open();
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);

        assertThat(store.countExisting(List.of(b("a"), b("a"), b("missing")))).isEqualTo(2);
    }

    @Test
    void computeCreatesUpdatesAndRemoves() {
        LsmKeyValueStore store = open();
        assertThat(store.compute(b("k"), current -> b(current == null ? "created" : "never"))).isEqualTo(b("created"));
        store.flush();
        assertThat(store.compute(b("k"), current -> concat(current, b("+")))).as("reads the value from a table")
                .isEqualTo(b("created+"));
        assertThat(store.compute(b("k"), current -> null)).isNull();
        assertThat(store.get(b("k"))).isNull();
        assertThat(store.compute(b("absent"), current -> null)).isNull();
    }

    @Test
    void computeKeepsTheExistingExpiry() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v"), START + 50, ALWAYS);

        store.compute(b("k"), current -> b("updated"));

        assertThat(store.get(b("k"))).isEqualTo(b("updated"));
        clock.advance(50);
        assertThat(store.get(b("k"))).isNull();
    }

    @Test
    void computeSeesExpiredKeysAsAbsent() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("old"), START + 5, ALWAYS);
        clock.advance(5);

        assertThat(store.compute(b("k"), current -> current == null ? b("fresh") : current)).isEqualTo(b("fresh"));
        clock.advance(1_000);
        assertThat(store.get(b("k"))).as("the new key has no expiry").isEqualTo(b("fresh"));
    }

    @Test
    void aFailingComputeLeavesTheValueUnchangedAndKeepsTheExceptionType() {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);

        assertThatThrownBy(() -> store.compute(b("k"), current -> {
            throw new NumberFormatException("not a number");
        })).isInstanceOf(NumberFormatException.class).hasMessage("not a number");
        assertThat(store.get(b("k"))).isEqualTo(b("v"));
        assertThat(store.put(b("k"), b("still writable"), NO_EXPIRY, ALWAYS)).isTrue();
    }

    @Test
    void sizeAndClear() {
        LsmKeyValueStore store = open();
        for (int i = 0; i < 100; i++) {
            store.put(b("k" + i), b("v"), NO_EXPIRY, ALWAYS);
        }
        store.flush();
        for (int i = 50; i < 150; i++) {
            store.put(b("k" + i), b("v2"), NO_EXPIRY, ALWAYS);
        }
        assertThat(store.size()).isEqualTo(150);

        store.clear();

        assertThat(store.size()).isZero();
        assertThat(store.get(b("k1"))).isNull();
        assertThat(store.tableStats()).isEmpty();
    }

    // --- Ordered scans -----------------------------------------------------------------------------------------------

    @Test
    void scanReturnsLiveKeysInOrderAcrossMemtablesAndTables() {
        LsmKeyValueStore store = open();
        for (int i = 0; i < 20; i += 2) {
            store.put(key(i), b("table-" + i), NO_EXPIRY, ALWAYS);
        }
        store.flush();
        for (int i = 1; i < 20; i += 2) {
            store.put(key(i), b("memtable-" + i), NO_EXPIRY, ALWAYS);
        }
        store.put(key(4), b("overwritten"), NO_EXPIRY, ALWAYS);
        store.delete(List.of(key(6)));
        store.put(key(8), b("expiring"), START + 1, ALWAYS);
        clock.advance(1);

        List<String> all = values(store.scan(null, null, Integer.MAX_VALUE));
        assertThat(all).hasSize(18).startsWith("table-0", "memtable-1", "table-2", "memtable-3", "overwritten",
                "memtable-5", "memtable-7", "memtable-9");

        assertThat(values(store.scan(key(10), key(13), 100))).containsExactly("table-10", "memtable-11", "table-12");
        assertThat(values(store.scan(key(10), null, 2))).containsExactly("table-10", "memtable-11");
        assertThat(store.scan(key(10), null, 0)).isEmpty();
        assertThat(s(store.scan(key(19), null, 10).getFirst().key())).isEqualTo(s(key(19)));
    }

    @Test
    void scanRejectsANegativeLimit() {
        LsmKeyValueStore store = open();

        assertThatThrownBy(() -> store.scan(null, null, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    // --- Flush, compaction and tombstones ----------------------------------------------------------------------------

    @Test
    void aFullMemtableIsFlushedToAnSSTableInTheBackground() throws InterruptedException {
        LsmKeyValueStore store = open(options().withMemtableSizeBytes(16 * 1024)
                .withCompaction(NO_BACKGROUND_COMPACTION, 0));
        for (int i = 0; i < 2_000; i++) {
            store.put(key(i), b("value-" + i), NO_EXPIRY, ALWAYS);
        }
        eventually(() -> store.immutableMemtableCount() == 0 && countFiles(".wal") == 1);

        assertThat(store.tableStats()).hasSizeGreaterThan(5);
        for (int i = 0; i < 2_000; i++) {
            assertThat(s(store.get(key(i)))).isEqualTo("value-" + i);
        }
        assertThat(countFiles(".wal")).as("flushed WALs are deleted").isEqualTo(1);
    }

    @Test
    void aTombstoneShadowsAnOlderValueInATable() {
        LsmKeyValueStore store = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0));
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.delete(List.of(b("k")));
        store.flush();

        assertThat(store.get(b("k"))).isNull();
        assertThat(store.tableStats()).hasSize(2);
        assertThat(store.tableStats().getFirst().tombstoneCount()).as("the newer table keeps the tombstone")
                .isEqualTo(1);
    }

    @Test
    void theFirstFlushDropsTombstonesBecauseNothingOlderExists() {
        LsmKeyValueStore store = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0));
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.delete(List.of(b("k")));
        store.put(b("kept"), b("v"), NO_EXPIRY, ALWAYS);
        store.flush();

        assertThat(store.tableStats()).hasSize(1);
        assertThat(store.tableStats().getFirst().entryCount()).isEqualTo(1);
        assertThat(store.tableStats().getFirst().tombstoneCount()).isZero();
    }

    @Test
    void majorCompactionMergesEverythingAndDropsTombstonesAndExpiredValues() {
        LsmKeyValueStore store = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0));
        store.putAll(List.of(b("a"), b("b"), b("c")), List.of(b("1"), b("2"), b("3")));
        store.flush();
        store.delete(List.of(b("b")));
        store.put(b("c"), b("3-expiring"), START + 10, ALWAYS);
        store.put(b("d"), b("4"), NO_EXPIRY, ALWAYS);
        store.flush();
        clock.advance(10);

        store.compact();

        assertThat(store.tableStats()).hasSize(1);
        TableStats merged = store.tableStats().getFirst();
        assertThat(merged.entryCount()).as("only a and d survive").isEqualTo(2);
        assertThat(merged.tombstoneCount()).isZero();
        assertThat(values(store.scan(null, null, 10))).containsExactly("1", "4");
        assertThat(store.get(b("c"))).as("an expired newer value must not resurrect the older one").isNull();
    }

    @Test
    void writeTableKeepsTombstonesAboveOlderDataAndDropsThemAtTheBottom() throws IOException {
        List<InternalEntry> entries = List.of(
                InternalEntry.put(b("a"), 1, b("live"), NO_EXPIRY),
                InternalEntry.tombstone(b("b"), 2),
                InternalEntry.put(b("c"), 3, b("expired"), START));

        SSTable above = LsmKeyValueStore.writeTable(dir.resolve("000001.sst"), 1, entries.iterator(), 3, false,
                START, () -> false);
        SSTable bottom = LsmKeyValueStore.writeTable(dir.resolve("000002.sst"), 2, entries.iterator(), 3, true,
                START, () -> false);
        try {
            assertThat(above.stats().entryCount()).isEqualTo(3);
            assertThat(above.stats().tombstoneCount()).as("the expired value became a tombstone").isEqualTo(2);
            assertThat(above.get(b("c")).isTombstone()).isTrue();
            assertThat(bottom.stats().entryCount()).isEqualTo(1);
            assertThat(bottom.stats().tombstoneCount()).isZero();
        } finally {
            above.release();
            bottom.release();
        }
        assertThat(LsmKeyValueStore.writeTable(dir.resolve("000003.sst"), 3,
                List.of(InternalEntry.tombstone(b("x"), 1)).iterator(), 1, true, START, () -> false))
                .as("nothing survives").isNull();
        assertThat(dir.resolve("000003.sst")).doesNotExist();
    }

    @Test
    void backgroundCompactionMergesSimilarTables() throws InterruptedException {
        LsmKeyValueStore store = open(options().withCompaction(4, 1 << 20));
        for (int batch = 0; batch < 8; batch++) {
            for (int i = 0; i < 50; i++) {
                store.put(key(batch * 50 + i), b("v" + batch), NO_EXPIRY, ALWAYS);
            }
            store.flush();
        }

        eventually(() -> store.tableStats().size() < 4 && countFiles(".sst") == store.tableStats().size());

        assertThat(store.tableStats()).hasSizeLessThan(4);
        assertThat(store.size()).isEqualTo(400);
        assertThat(s(store.get(key(399)))).isEqualTo("v7");
        assertThat(countFiles(".sst")).as("merged inputs are deleted").isEqualTo(store.tableStats().size());
    }

    // --- Durability and recovery -------------------------------------------------------------------------------------

    @Test
    void everythingSurvivesARestart() {
        LsmKeyValueStore store = open();
        store.put(b("plain"), b("1"), NO_EXPIRY, ALWAYS);
        store.put(b("expiring"), b("2"), START + 1_000, ALWAYS);
        store.putAll(List.of(b("m1"), b("m2")), List.of(b("x"), b("y")));
        store.flush();
        store.put(b("deleted"), b("gone"), NO_EXPIRY, ALWAYS);
        store.delete(List.of(b("deleted"), b("m2")));
        store.compute(b("counter"), current -> b("41"));
        store.compute(b("counter"), current -> b(Integer.toString(Integer.parseInt(s(current)) + 1)));
        long sequence = store.publishedSequence();
        store.close();

        LsmKeyValueStore reopened = open();

        assertThat(s(reopened.get(b("plain")))).isEqualTo("1");
        assertThat(s(reopened.get(b("expiring")))).isEqualTo("2");
        assertThat(s(reopened.get(b("m1")))).isEqualTo("x");
        assertThat(reopened.get(b("m2"))).isNull();
        assertThat(reopened.get(b("deleted"))).isNull();
        assertThat(s(reopened.get(b("counter")))).isEqualTo("42");
        assertThat(reopened.publishedSequence()).isEqualTo(sequence);
        clock.advance(1_000);
        assertThat(reopened.get(b("expiring"))).as("the absolute expiry instant was persisted").isNull();
    }

    @Test
    void sequenceNumbersKeepGrowingAcrossRestarts() {
        LsmKeyValueStore store = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0));
        store.put(b("filler"), b("1"), NO_EXPIRY, ALWAYS);
        store.put(b("k"), b("old"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.close();
        LsmKeyValueStore reopened = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0));

        reopened.put(b("k"), b("new"), NO_EXPIRY, ALWAYS);
        reopened.flush();
        reopened.compact();

        assertThat(s(reopened.get(b("k")))).as("the merge must rank the newer write higher").isEqualTo("new");
    }

    @Test
    void openingACopyOfALiveDirectoryRecoversEveryAcknowledgedWrite() throws IOException {
        LsmKeyValueStore store = open(options().withCompaction(NO_BACKGROUND_COMPACTION, 0)
                .withGroupCommit(Duration.ofSeconds(60), Long.MAX_VALUE));
        for (int i = 0; i < 100; i++) {
            store.put(key(i), b("flushed-" + i), NO_EXPIRY, ALWAYS);
        }
        store.flush();
        for (int i = 50; i < 150; i++) {
            store.put(key(i), b("logged-" + i), NO_EXPIRY, ALWAYS);
        }

        // The copy is what a crash would leave behind: written but unsynced WAL data is in the OS's page cache.
        Path copy = Files.createDirectory(dir.resolve("crash-copy"));
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                if (!file.getFileName().toString().equals(DataDirectory.LOCK)) {
                    Files.write(copy.resolve(file.getFileName()), Files.readAllBytes(file));
                }
            }
        }
        LsmKeyValueStore recovered = open(options(copy));

        for (int i = 0; i < 150; i++) {
            assertThat(s(recovered.get(key(i)))).isEqualTo(i < 50 ? "flushed-" + i : "logged-" + i);
        }
    }

    @Test
    void aTornRecordAtTheEndOfTheWalIsDropped() throws IOException {
        LsmKeyValueStore store = open();
        store.put(b("k1"), b("v1"), NO_EXPIRY, ALWAYS);
        store.put(b("k2"), b("v2"), NO_EXPIRY, ALWAYS);
        store.close();
        Path wal = onlyFile(".wal");
        WalTest.append(wal, WalTest.frame(WalTest.records(1).getFirst()), 11);

        LsmKeyValueStore reopened = open();

        assertThat(s(reopened.get(b("k1")))).isEqualTo("v1");
        assertThat(s(reopened.get(b("k2")))).isEqualTo("v2");
        assertThat(reopened.get(b("key1"))).isNull();
    }

    @Test
    void corruptionInTheMiddleOfTheWalStopsRecovery() throws IOException {
        LsmKeyValueStore store = open();
        store.put(b("k1"), b("v1"), NO_EXPIRY, ALWAYS);
        store.put(b("k2"), b("v2"), NO_EXPIRY, ALWAYS);
        store.close();
        WalTest.flipByte(onlyFile(".wal"), 12);

        assertThatThrownBy(this::open).isInstanceOf(CorruptionException.class);
        assertThat(DataDirectory.open(dir)).as("the failed open released the directory lock").isNotNull()
                .satisfies(DataDirectory::close);
    }

    @Test
    void aMissingTableIsCorruption() throws IOException {
        LsmKeyValueStore store = open();
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.close();
        Files.delete(onlyFile(".sst"));

        assertThatThrownBy(this::open)
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void dataFilesWithoutAManifestAreCorruption() throws IOException {
        Files.write(dir.resolve("000004.wal"), new byte[0]);

        assertThatThrownBy(this::open)
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("no MANIFEST");
    }

    @Test
    void openingDeletesFilesTheManifestDoesNotReference() throws IOException {
        open().close();
        Files.write(dir.resolve("000999.sst"), new byte[] {1, 2, 3});
        Files.write(dir.resolve(DataDirectory.MANIFEST_TEMP), new byte[] {1});

        open();

        assertThat(dir.resolve("000999.sst")).doesNotExist();
        assertThat(dir.resolve(DataDirectory.MANIFEST_TEMP)).doesNotExist();
    }

    @Test
    void clearIsDurable() throws IOException {
        LsmKeyValueStore store = open();
        store.put(b("a"), b("1"), NO_EXPIRY, ALWAYS);
        store.flush();
        store.put(b("b"), b("2"), NO_EXPIRY, ALWAYS);
        store.clear();
        store.put(b("c"), b("3"), NO_EXPIRY, ALWAYS);
        store.close();

        LsmKeyValueStore reopened = open();

        assertThat(reopened.get(b("a"))).isNull();
        assertThat(reopened.get(b("b"))).isNull();
        assertThat(s(reopened.get(b("c")))).isEqualTo("3");
        try (Stream<Path> tables = Files.list(dir).filter(file -> file.toString().endsWith(".sst"))) {
            assertThat(tables.count()).as("only the table recovery made from the WAL").isEqualTo(1);
        }
    }

    @Test
    void groupCommitSyncsTheWalInTheBackgroundAfterTheInterval() throws InterruptedException {
        LsmKeyValueStore store = open(options().withGroupCommit(Duration.ofMillis(20), Long.MAX_VALUE));
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);

        eventually(() -> store.unsyncedWalBytes() == 0);

        assertThat(store.unsyncedWalBytes()).isZero();
    }

    @Test
    void groupCommitSyncsTheWalOnceEnoughBytesWait() throws InterruptedException {
        LsmKeyValueStore store = open(options().withGroupCommit(Duration.ofHours(1), 1_000));
        store.put(b("small"), b("v"), NO_EXPIRY, ALWAYS);
        Thread.sleep(50);
        assertThat(store.unsyncedWalBytes()).as("below the byte threshold, an hour before the deadline").isPositive();

        store.put(b("large"), new byte[2_000], NO_EXPIRY, ALWAYS);

        eventually(() -> store.unsyncedWalBytes() == 0);
        assertThat(store.unsyncedWalBytes()).isZero();
    }

    @Test
    void alwaysSyncsBeforeAcknowledging() {
        LsmKeyValueStore store = open(options().withFsyncPolicy(FsyncPolicy.ALWAYS));
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);

        assertThat(store.unsyncedWalBytes()).isZero();
    }

    @Test
    void alwaysPolicyWorksToo() {
        LsmKeyValueStore store = open(options().withFsyncPolicy(FsyncPolicy.ALWAYS));
        store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS);
        store.close();

        assertThat(s(open(options().withFsyncPolicy(FsyncPolicy.ALWAYS)).get(b("k")))).isEqualTo("v");
    }

    @Test
    void largeValuesRoundTrip() {
        byte[] large = new byte[2 << 20];
        Arrays.fill(large, (byte) 7);
        LsmKeyValueStore store = open();
        store.put(b("large"), large, NO_EXPIRY, ALWAYS);
        store.flush();
        store.close();

        assertThat(open().get(b("large"))).isEqualTo(large);
    }

    // --- Lifecycle ---------------------------------------------------------------------------------------------------

    @Test
    void aDirectoryCanOnlyBeOpenedOnce() {
        open();

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already in use");
    }

    @Test
    void aClosedStoreRejectsEveryOperation() {
        LsmKeyValueStore store = open();
        store.close();
        store.close();

        assertThatThrownBy(() -> store.get(b("k"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.put(b("k"), b("v"), NO_EXPIRY, ALWAYS))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(store::flush).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void optionsValidateTheirValues() {
        LsmKeyValueStore.Options defaults = LsmKeyValueStore.Options.defaults(dir);

        assertThat(defaults.fsyncPolicy()).isEqualTo(FsyncPolicy.GROUP_COMMIT);
        assertThat(defaults.memtableSizeBytes()).isEqualTo(64L << 20);
        assertThatThrownBy(() -> defaults.withMemtableSizeBytes(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withCompaction(1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withGroupCommit(Duration.ofMillis(-1), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withGroupCommit(Duration.ZERO, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fsyncPolicyNamesParse() {
        assertThat(FsyncPolicy.fromConfigName("Always")).isEqualTo(FsyncPolicy.ALWAYS);
        assertThat(FsyncPolicy.fromConfigName(" group-commit ")).isEqualTo(FsyncPolicy.GROUP_COMMIT);
        assertThatThrownBy(() -> FsyncPolicy.fromConfigName("everysec")).isInstanceOf(IllegalArgumentException.class);
    }

    // --- Helpers -----------------------------------------------------------------------------------------------------

    private LsmKeyValueStore.Options options() {
        return options(dir);
    }

    private LsmKeyValueStore.Options options(Path directory) {
        return LsmKeyValueStore.Options.defaults(directory).withClock(clock);
    }

    private LsmKeyValueStore open() {
        return open(options());
    }

    private LsmKeyValueStore open(LsmKeyValueStore.Options options) {
        LsmKeyValueStore store = LsmKeyValueStore.open(options);
        opened.add(store);
        return store;
    }

    private Path onlyFile(String suffix) throws IOException {
        try (Stream<Path> files = Files.list(dir).filter(file -> file.toString().endsWith(suffix))) {
            List<Path> matches = files.toList();
            assertThat(matches).hasSize(1);
            return matches.getFirst();
        }
    }

    private long countFiles(String suffix) {
        try (Stream<Path> files = Files.list(dir).filter(file -> file.toString().endsWith(suffix))) {
            return files.count();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Background threads finish their work shortly after the state the test waits for becomes visible. */
    private static void eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private static byte[] key(int i) {
        return b(String.format("key-%06d", i));
    }

    private static List<String> values(List<KeyValue> entries) {
        return entries.stream().map(entry -> s(entry.value())).toList();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
