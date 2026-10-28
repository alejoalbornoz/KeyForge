package io.keyforge.storage.lsm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays a WAL file during recovery.
 *
 * <p><b>Torn tails.</b> A crash can interrupt the last append, leaving a record that is cut short, has a bad checksum,
 * or is followed only by zero bytes (file systems may extend a file before its data reaches the disk). Such a record is
 * <em>at the tail</em> when nothing but zero bytes follows it, and nothing at all when it is cut short. That record was
 * never acknowledged (the writer acknowledges only after the whole write returned), so recovery truncates the file
 * there and carries on.
 *
 * <p><b>Corruption.</b> A bad record followed by more data cannot be the product of a crash during an append. Skipping
 * it could drop acknowledged writes and replaying past it could apply writes out of order, so recovery fails with
 * {@link CorruptionException}. The same applies to a torn tail in any WAL but the newest, because the writer syncs and
 * closes a WAL before starting the next one.
 *
 * <p>A record whose length field points past the end of the file is indistinguishable from a cut-short append and is
 * treated as a torn tail.
 */
final class WalReader {

    private static final Logger log = LoggerFactory.getLogger(WalReader.class);

    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    /** What one replay found. {@code truncatedBytes} is how much torn tail was cut off (0 if none). */
    record Result(long records, long validLength, long truncatedBytes) {
    }

    private WalReader() {
    }

    /**
     * Feeds every intact record of {@code file} to {@code sink}, in order.
     *
     * @param newest whether this is the newest WAL, the only one that may end in a torn record
     * @throws CorruptionException if a record in the middle is damaged, or an older WAL has a torn tail
     */
    static Result replay(Path file, boolean newest, Consumer<WalRecord> sink) throws IOException {
        long records = 0;
        long position = 0;
        long size;
        long tornAt = -1;
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
             Arena arena = Arena.ofConfined()) {
            size = channel.size();
            MemorySegment data = size == 0 ? MemorySegment.NULL : channel.map(FileChannel.MapMode.READ_ONLY, 0, size,
                    arena);
            while (position < size) {
                if (size - position < WalRecord.HEADER_BYTES) {
                    tornAt = position;
                    break;
                }
                int length = data.get(INT, position);
                int crc = data.get(INT, position + 4);
                if (length <= 0) {
                    if (allZero(data, position, size)) {
                        tornAt = position;
                        break;
                    }
                    throw corrupt(file, position, "invalid record length " + length);
                }
                long end = position + WalRecord.HEADER_BYTES + length;
                if (end > size) {
                    tornAt = position;
                    break;
                }
                byte[] payload = new byte[length];
                MemorySegment.copy(data, ValueLayout.JAVA_BYTE, position + WalRecord.HEADER_BYTES, payload, 0, length);
                if (WalRecord.crc32c(payload, 0, length) != crc) {
                    if (allZero(data, end, size)) {
                        tornAt = position;
                        break;
                    }
                    throw corrupt(file, position, "CRC mismatch in a record followed by more data");
                }
                sink.accept(WalRecord.decode(payload, file + " at offset " + position));
                records++;
                position = end;
            }
        }
        if (tornAt < 0) {
            return new Result(records, size, 0);
        }
        if (!newest) {
            throw corrupt(file, tornAt, "torn record in a WAL that is not the newest");
        }
        // The mapping is closed by now: Windows cannot truncate a file while a mapped view of it is open.
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(tornAt);
            channel.force(true);
        }
        log.warn("Truncated a torn record at the end of {}: kept {} records ({} bytes), dropped {} bytes",
                file, records, tornAt, size - tornAt);
        return new Result(records, tornAt, size - tornAt);
    }

    private static boolean allZero(MemorySegment data, long from, long to) {
        for (long i = from; i < to; i++) {
            if (data.get(ValueLayout.JAVA_BYTE, i) != 0) {
                return false;
            }
        }
        return true;
    }

    private static CorruptionException corrupt(Path file, long offset, String problem) {
        return new CorruptionException(file + " at offset " + offset + ": " + problem);
    }
}
