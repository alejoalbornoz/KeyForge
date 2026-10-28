package io.keyforge.storage.lsm;

/**
 * Persistent data failed validation: a checksum mismatch, a bad magic number or format version, or a structure that
 * cannot have been written by this engine. Recovery stops rather than guess, because continuing could silently drop
 * acknowledged writes. The one exception is a torn record at the very end of the newest WAL, which recovery truncates.
 */
public final class CorruptionException extends RuntimeException {

    public CorruptionException(String message) {
        super(message);
    }
}
