package io.keyforge.raft.storage;

/** Raft's durable state is damaged in a way recovery must not paper over. */
public final class StorageCorruptionException extends RuntimeException {

    public StorageCorruptionException(String message) {
        super(message);
    }
}
