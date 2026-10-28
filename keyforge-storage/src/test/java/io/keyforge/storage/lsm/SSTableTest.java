package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static io.keyforge.storage.lsm.MemtableTest.values;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableTest {

    @TempDir
    Path dir;

    @Test
    void getFindsEveryKeyAndOnlyThoseKeys() throws IOException {
        SSTable table = write(1, entries(5_000));
        try {
            for (int i = 0; i < 5_000; i++) {
                InternalEntry entry = table.get(key(i));
                assertThat(entry).as("key %d", i).isNotNull();
                assertThat(s(entry.value())).isEqualTo("value-" + i);
                assertThat(entry.seq()).isEqualTo(i + 1);
            }
            assertThat(table.get(b("key-"))).isNull();
            assertThat(table.get(b("key-99999999"))).isNull();
            assertThat(table.get(b("a"))).as("before the first key").isNull();
            assertThat(table.get(b("z"))).as("after the last key").isNull();
        } finally {
            table.release();
        }
    }

    @Test
    void dataIsSplitIntoBlocksOfAboutFourKilobytes() throws IOException {
        SSTable table = write(1, entries(5_000));
        try {
            // 5,000 entries of about 40 bytes: about 200 KB, so roughly 50 blocks of 4 KiB each.
            long dataBytes = table.fileSize();
            assertThat(dataBytes).isGreaterThan(150_000);
            assertThat(table.stats().entryCount()).isEqualTo(5_000);
        } finally {
            table.release();
        }
    }

    @Test
    void storesTombstonesExpiryAndStatistics() throws IOException {
        List<InternalEntry> entries = List.of(
                InternalEntry.put(b("a"), 3, b("1"), NO_EXPIRY),
                InternalEntry.tombstone(b("b"), 4),
                InternalEntry.put(b("c"), 9, b("3"), 1_700_000_000_000L));
        SSTable table = write(7, entries);
        try {
            assertThat(table.get(b("b")).isTombstone()).isTrue();
            assertThat(table.get(b("c")).expireAt()).isEqualTo(1_700_000_000_000L);
            TableStats stats = table.stats();
            assertThat(stats.entryCount()).isEqualTo(3);
            assertThat(stats.tombstoneCount()).isEqualTo(1);
            assertThat(stats.expiringCount()).isEqualTo(1);
            assertThat(stats.minSeq()).isEqualTo(3);
            assertThat(stats.maxSeq()).isEqualTo(9);
            assertThat(s(stats.minKey())).isEqualTo("a");
            assertThat(s(stats.maxKey())).isEqualTo("c");
            assertThat(table.fileNumber()).isEqualTo(7);
        } finally {
            table.release();
        }
    }

    @Test
    void iteratesInOrderWithinBounds() throws IOException {
        SSTable table = write(1, entries(1_000));
        try {
            List<String> all = values(table.iterator(null, null));
            assertThat(all).hasSize(1_000).startsWith("value-0").endsWith("value-999");

            List<String> range = values(table.iterator(key(100), key(105)));
            assertThat(range).containsExactly("value-100", "value-101", "value-102", "value-103", "value-104");

            assertThat(values(table.iterator(b("zzz"), null))).isEmpty();
            assertThat(values(table.iterator(key(950), null))).hasSize(50);
        } finally {
            table.release();
        }
    }

    @Test
    void valuesLargerThanABlockWork() throws IOException {
        byte[] big = new byte[100_000];
        Arrays.fill(big, (byte) 'x');
        SSTable table = write(1, List.of(
                InternalEntry.put(b("big"), 1, big, NO_EXPIRY),
                InternalEntry.put(b("small"), 2, b("s"), NO_EXPIRY)));
        try {
            assertThat(table.get(b("big")).value()).isEqualTo(big);
            assertThat(s(table.get(b("small")).value())).isEqualTo("s");
        } finally {
            table.release();
        }
    }

    @Test
    void writerRejectsKeysOutOfOrder() throws IOException {
        try (SSTableWriter writer = new SSTableWriter(dir.resolve("bad.sst"), 2)) {
            writer.add(InternalEntry.put(b("b"), 1, b("v"), NO_EXPIRY));

            assertThatThrownBy(() -> writer.add(InternalEntry.put(b("a"), 2, b("v"), NO_EXPIRY)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> writer.add(InternalEntry.put(b("b"), 3, b("v"), NO_EXPIRY)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(dir.resolve("bad.sst")).as("an unfinished table is deleted").doesNotExist();
    }

    @Test
    void theFooterEndsWithTheFormatVersionAndMagic() throws IOException {
        write(1, entries(10)).release();
        byte[] file = Files.readAllBytes(dir.resolve("000001.sst"));

        ByteReader tail = new ByteReader(file, file.length - 12, 12, "footer");
        assertThat(tail.readInt()).isEqualTo(SSTableWriter.FORMAT_VERSION);
        assertThat(tail.readLong()).isEqualTo(0x4B455946_4F524745L);
    }

    @Test
    void rejectsABadMagicNumber() throws IOException {
        write(1, entries(10)).release();
        Path file = dir.resolve("000001.sst");
        overwrite(file, Files.size(file) - 1, (byte) 0);

        assertThatThrownBy(() -> SSTable.open(file, 1))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void rejectsAnUnknownFormatVersion() throws IOException {
        write(1, entries(10)).release();
        Path file = dir.resolve("000001.sst");
        overwrite(file, Files.size(file) - 9, (byte) 99);

        assertThatThrownBy(() -> SSTable.open(file, 1))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("format version");
    }

    @Test
    void detectsADamagedDataBlock() throws IOException {
        write(1, entries(1_000)).release();
        Path file = dir.resolve("000001.sst");
        overwrite(file, 10, (byte) 0x7F);

        SSTable table = SSTable.open(file, 1);
        try {
            assertThatThrownBy(() -> table.get(key(0)))
                    .isInstanceOf(CorruptionException.class)
                    .hasMessageContaining("CRC mismatch");
        } finally {
            table.release();
        }
    }

    @Test
    void rejectsATruncatedFile() throws IOException {
        Path file = dir.resolve("short.sst");
        Files.write(file, new byte[10]);

        assertThatThrownBy(() -> SSTable.open(file, 1)).isInstanceOf(CorruptionException.class);
    }

    @Test
    void anObsoleteTableIsDeletedWhenTheLastReferenceGoes() throws IOException {
        SSTable table = write(1, entries(10));
        Path file = dir.resolve("000001.sst");
        table.retain();
        table.markObsolete();

        table.release();
        assertThat(file).as("still referenced").exists();
        table.release();

        assertThat(file).doesNotExist();
        assertThatThrownBy(table::retain).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aReleasedTableThatIsStillLiveKeepsItsFile() throws IOException {
        write(1, entries(10)).release();

        assertThat(dir.resolve("000001.sst")).exists();
    }

    private SSTable write(long number, List<InternalEntry> entries) throws IOException {
        Path path = dir.resolve(DataDirectory.fileName(number, DataDirectory.TABLE_SUFFIX));
        try (SSTableWriter writer = new SSTableWriter(path, entries.size())) {
            for (InternalEntry entry : entries) {
                writer.add(entry);
            }
            writer.finish();
        }
        return SSTable.open(path, number);
    }

    static List<InternalEntry> entries(int count) {
        List<InternalEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(InternalEntry.put(key(i), i + 1, b("value-" + i), NO_EXPIRY));
        }
        return entries;
    }

    static byte[] key(int i) {
        return b(String.format("key-%010d", i));
    }

    private static void overwrite(Path file, long offset, byte value) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[] {value}), offset);
        }
    }
}
