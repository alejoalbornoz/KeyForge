package io.keyforge.raft.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.zip.CRC32C;

/** Crash-safe file helpers shared by the log and the snapshot store. */
final class DurableFiles {

    /** Windows cannot open a directory as a channel, so directories cannot be fsynced there; NTFS journals metadata. */
    private static final boolean CAN_SYNC_DIRECTORIES =
            !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    private DurableFiles() {
    }

    /** Makes renames, creations and deletions in {@code directory} durable, where the platform allows it. */
    static void syncDirectory(Path directory) throws IOException {
        if (CAN_SYNC_DIRECTORIES) {
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
                channel.force(true);
            }
        }
    }

    /** Forces {@code temporary} to disk, renames it over {@code target} atomically and syncs the directory. */
    static void commit(Path temporary, Path target) throws IOException {
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory(target.getParent());
    }

    static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
