package io.keyforge.storage.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Appends framed records ({@link WalRecord}) to one WAL file.
 *
 * <p>Only the store's writer thread calls {@link #write}. {@link #sync} and {@link #close} may also be called from the
 * WAL sync thread, which forces the file under the group-commit policy while the writer keeps appending; they are
 * synchronised with each other, and a sync after close does nothing. {@code FileChannel} allows a write and a force to
 * run concurrently: the force covers at least everything written before it started.
 */
final class WalWriter implements AutoCloseable {

    private final long number;
    private final FileChannel channel;
    /** Written by the writer thread only. */
    private volatile long writtenBytes;
    private volatile long syncedBytes;
    private volatile long lastSyncNanos = System.nanoTime();
    private boolean closed;

    private WalWriter(long number, FileChannel channel) {
        this.number = number;
        this.channel = channel;
    }

    /** Creates a new, empty WAL file; fails if the file exists. */
    static WalWriter create(Path path, long number) throws IOException {
        return new WalWriter(number, FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
    }

    long number() {
        return number;
    }

    /** Writes {@code length} bytes of already framed records with as few system calls as the OS allows. */
    void write(byte[] records, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(records, 0, length);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        writtenBytes = writtenBytes + length;
    }

    /** Forces everything written before the call to stable storage. Does nothing once closed. */
    synchronized void sync() throws IOException {
        if (closed) {
            return;
        }
        long target = writtenBytes;
        if (target > syncedBytes) {
            channel.force(false);
            syncedBytes = target;
        }
        lastSyncNanos = System.nanoTime();
    }

    /** Bytes written but not yet covered by a completed sync. */
    long unsyncedBytes() {
        return writtenBytes - syncedBytes;
    }

    /** {@link System#nanoTime()} at the end of the last sync (or at creation). */
    long lastSyncNanos() {
        return lastSyncNanos;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        channel.close();
    }
}
