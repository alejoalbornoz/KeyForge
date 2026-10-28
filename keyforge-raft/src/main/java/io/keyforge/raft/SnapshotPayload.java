package io.keyforge.raft;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * The contents of a snapshot travelling in an {@link RaftMessage.InstallSnapshot} message. The core never reads it; it
 * hands it to the driver in a {@link RaftAction.RestoreSnapshot} action. In production it is a file the gRPC transport
 * streamed to disk, so a large snapshot never has to fit in memory.
 */
public interface SnapshotPayload {

    /** Size in bytes. */
    long size();

    /** Opens the contents for reading; may be called more than once. */
    InputStream open() throws IOException;

    /** Releases resources such as a temporary file once the message has been handled. The default does nothing. */
    default void discard() {
    }

    /** An in-memory payload. */
    static SnapshotPayload of(byte[] bytes) {
        return new SnapshotPayload() {
            @Override
            public long size() {
                return bytes.length;
            }

            @Override
            public InputStream open() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public String toString() {
                return "SnapshotPayload[" + bytes.length + " bytes]";
            }
        };
    }
}
