package io.keyforge.storage.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The files of one store and the operations on them that must be crash-safe.
 *
 * <pre>
 * LOCK            held with an OS file lock while a store has the directory open
 * MANIFEST        the live tables (see {@link Manifest}); replaced atomically
 * NNNNNN.wal      write-ahead logs, one per memtable
 * NNNNNN.sst      SSTables
 * </pre>
 *
 * WAL and SSTable files share one increasing file-number sequence.
 */
final class DataDirectory implements AutoCloseable {

    static final String LOCK = "LOCK";
    static final String MANIFEST = "MANIFEST";
    static final String MANIFEST_TEMP = "MANIFEST.tmp";
    static final String WAL_SUFFIX = ".wal";
    static final String TABLE_SUFFIX = ".sst";

    private static final Pattern NUMBERED_FILE = Pattern.compile("(\\d+)\\.(wal|sst)");
    /** Windows cannot open a directory as a channel, so directories cannot be fsynced there; NTFS journals metadata. */
    private static final boolean CAN_SYNC_DIRECTORIES =
            !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    private final Path path;
    private final FileChannel lockChannel;
    private final FileLock lock;

    private DataDirectory(Path path, FileChannel lockChannel, FileLock lock) {
        this.path = path;
        this.lockChannel = lockChannel;
        this.lock = lock;
    }

    /** Creates the directory if needed and locks it against other stores, in this process or any other. */
    static DataDirectory open(Path path) throws IOException {
        Files.createDirectories(path);
        FileChannel channel = FileChannel.open(path.resolve(LOCK), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            lock = null;
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
        if (lock == null) {
            channel.close();
            throw new IllegalStateException("Data directory " + path + " is already in use by another store");
        }
        return new DataDirectory(path, channel, lock);
    }

    Path path() {
        return path;
    }

    Path walPath(long number) {
        return path.resolve(fileName(number, WAL_SUFFIX));
    }

    Path tablePath(long number) {
        return path.resolve(fileName(number, TABLE_SUFFIX));
    }

    static String fileName(long number, String suffix) {
        return String.format(Locale.ROOT, "%06d%s", number, suffix);
    }

    /** Numbers of the WAL files present, ascending. */
    TreeSet<Long> walNumbers() throws IOException {
        return numbers("wal");
    }

    /** Numbers of the SSTable files present, ascending. */
    TreeSet<Long> tableNumbers() throws IOException {
        return numbers("sst");
    }

    /** The manifest, or {@code null} if the directory has never been initialised. */
    Manifest readManifest() throws IOException {
        Path file = path.resolve(MANIFEST);
        try {
            return Manifest.decode(Files.readAllBytes(file), file.toString());
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /** Replaces the manifest atomically: temporary file, fsync, rename over the old one, fsync the directory. */
    void writeManifest(Manifest manifest) throws IOException {
        Path temp = path.resolve(MANIFEST_TEMP);
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(manifest.encode());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        Files.move(temp, path.resolve(MANIFEST), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        sync();
    }

    /** Makes file creations, renames and deletions in the directory durable, where the platform allows it. */
    void sync() throws IOException {
        if (!CAN_SYNC_DIRECTORIES) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    void deleteIfExists(Path file) throws IOException {
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            lockChannel.close();
        }
    }

    private TreeSet<Long> numbers(String extension) throws IOException {
        TreeSet<Long> numbers = new TreeSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(path)) {
            for (Path file : files) {
                Matcher matcher = NUMBERED_FILE.matcher(file.getFileName().toString());
                if (matcher.matches() && matcher.group(2).equals(extension)) {
                    numbers.add(Long.parseLong(matcher.group(1)));
                }
            }
        }
        return numbers;
    }
}
