package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalTest {

    @TempDir
    Path dir;

    @Test
    void recordsRoundTrip() throws IOException {
        Path file = dir.resolve("000001.wal");
        List<WalRecord> written = List.of(
                new WalRecord(1, List.of(InternalEntry.put(b("a"), 1, b("1"), NO_EXPIRY))),
                new WalRecord(2, List.of(InternalEntry.put(b("b"), 2, b("2"), 1_700_000_000_000L),
                        InternalEntry.tombstone(b("c"), 2))),
                new WalRecord(3, List.of(InternalEntry.put(b(""), 3, new byte[0], NO_EXPIRY))));
        write(file, written);

        List<WalRecord> read = new ArrayList<>();
        WalReader.Result result = WalReader.replay(file, true, read::add);

        assertThat(read).isEqualTo(written);
        assertThat(result.records()).isEqualTo(3);
        assertThat(result.truncatedBytes()).isZero();
    }

    @Test
    void theFrameIsLengthThenCrc32cThenPayload() {
        ByteWriter out = new ByteWriter(64);
        WalRecord.encode(out, 42, List.of(InternalEntry.tombstone(b("k"), 42)));
        byte[] bytes = out.toByteArray();

        ByteReader in = new ByteReader(bytes, "test");
        int length = in.readInt();
        int crc = in.readInt();
        assertThat(length).isEqualTo(bytes.length - WalRecord.HEADER_BYTES);
        assertThat(crc).isEqualTo(WalRecord.crc32c(bytes, WalRecord.HEADER_BYTES, length));
        assertThat(in.readLong()).as("payload starts with the sequence number").isEqualTo(42);
    }

    @Test
    void aRecordCutShortAtTheEndIsTruncated() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(3));
        long intact = Files.size(file);
        append(file, frame(records(1).getFirst()), 5);

        List<WalRecord> read = new ArrayList<>();
        WalReader.Result result = WalReader.replay(file, true, read::add);

        assertThat(read).hasSize(3);
        assertThat(result.truncatedBytes()).isEqualTo(5);
        assertThat(Files.size(file)).as("the torn bytes are gone from the file").isEqualTo(intact);
    }

    @Test
    void aHeaderCutShortIsTruncated() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(2));
        append(file, new byte[] {0, 0, 1}, 3);

        WalReader.Result result = WalReader.replay(file, true, record -> { });

        assertThat(result.records()).isEqualTo(2);
        assertThat(result.truncatedBytes()).isEqualTo(3);
    }

    @Test
    void aCrcMismatchInTheLastRecordIsTruncated() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(3));
        flipByte(file, Files.size(file) - 1);

        List<WalRecord> read = new ArrayList<>();
        WalReader.replay(file, true, read::add);

        assertThat(read).hasSize(2);
    }

    @Test
    void aZeroFilledTailIsTruncated() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(2));
        append(file, new byte[100], 100);

        WalReader.Result result = WalReader.replay(file, true, record -> { });

        assertThat(result.records()).isEqualTo(2);
        assertThat(result.truncatedBytes()).isEqualTo(100);
    }

    @Test
    void aCrcMismatchInTheMiddleIsFatal() throws IOException {
        Path file = dir.resolve("000001.wal");
        List<WalRecord> records = records(3);
        write(file, records);
        flipByte(file, frame(records.getFirst()).length - 1);

        assertThatThrownBy(() -> WalReader.replay(file, true, record -> { }))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    void aNegativeLengthInTheMiddleIsFatal() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(3));
        flipByte(file, 0);

        assertThatThrownBy(() -> WalReader.replay(file, true, record -> { }))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("invalid record length");
    }

    @Test
    void aTornTailInAnOlderWalIsFatal() throws IOException {
        Path file = dir.resolve("000001.wal");
        write(file, records(2));
        append(file, frame(records(1).getFirst()), 4);

        assertThatThrownBy(() -> WalReader.replay(file, false, record -> { }))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("not the newest");
    }

    @Test
    void anEmptyWalHasNoRecords() throws IOException {
        Path file = dir.resolve("000001.wal");
        Files.createFile(file);

        assertThat(WalReader.replay(file, true, record -> { }).records()).isZero();
    }

    @Test
    void aValidCrcOverAnUndecodablePayloadIsFatal() throws IOException {
        Path file = dir.resolve("000001.wal");
        byte[] payload = {0, 0, 0, 0, 0, 0, 0, 1, 1, 9, 0};   // seq 1, one entry of unknown type 9 with an empty key
        ByteWriter out = new ByteWriter(32);
        out.writeInt(payload.length);
        out.writeInt(WalRecord.crc32c(payload, 0, payload.length));
        out.writeBytes(payload);
        Files.write(file, out.toByteArray());

        assertThatThrownBy(() -> WalReader.replay(file, true, record -> { }))
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("unknown entry type");
    }

    @Test
    void writerTracksUnsyncedBytes() throws IOException {
        try (WalWriter writer = WalWriter.create(dir.resolve("000005.wal"), 5)) {
            byte[] frame = frame(records(1).getFirst());
            writer.write(frame, frame.length);
            assertThat(writer.unsyncedBytes()).isEqualTo(frame.length);

            writer.sync();

            assertThat(writer.unsyncedBytes()).isZero();
            assertThat(writer.number()).isEqualTo(5);
        }
        assertThatThrownBy(() -> WalWriter.create(dir.resolve("000005.wal"), 5))
                .as("an existing WAL is never reopened for writing")
                .isInstanceOf(IOException.class);
    }

    @Test
    void aSyncAfterCloseDoesNothing() throws IOException {
        WalWriter writer = WalWriter.create(dir.resolve("000006.wal"), 6);
        byte[] frame = frame(records(1).getFirst());
        writer.write(frame, frame.length);
        writer.close();

        writer.sync();

        assertThat(writer.unsyncedBytes()).as("nothing was forced").isEqualTo(frame.length);
    }

    static List<WalRecord> records(int count) {
        List<WalRecord> records = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            records.add(new WalRecord(i, List.of(InternalEntry.put(b("key" + i), i, b("value" + i), NO_EXPIRY))));
        }
        return records;
    }

    static byte[] frame(WalRecord record) {
        ByteWriter out = new ByteWriter(64);
        WalRecord.encode(out, record.seq(), record.entries());
        return out.toByteArray();
    }

    static void write(Path file, List<WalRecord> records) throws IOException {
        try (WalWriter writer = WalWriter.create(file, 1)) {
            for (WalRecord record : records) {
                byte[] frame = frame(record);
                writer.write(frame, frame.length);
            }
            writer.sync();
        }
    }

    static void append(Path file, byte[] bytes, int length) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(bytes, 0, length));
        }
    }

    static void flipByte(Path file, long offset) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, offset);
            one.put(0, (byte) (one.get(0) ^ 0xFF));
            one.rewind();
            channel.write(one, offset);
        }
    }
}
