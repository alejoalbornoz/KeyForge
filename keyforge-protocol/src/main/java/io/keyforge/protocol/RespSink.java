package io.keyforge.protocol;

/**
 * Where {@link RespEncoder} writes bytes. Each transport adapts its own buffer type (a Netty {@code ByteBuf}, a socket
 * output buffer) so the encoder stays independent of any I/O library.
 */
public interface RespSink {

    void writeByte(int value);

    void writeBytes(byte[] source, int offset, int length);

    default void writeBytes(byte[] source) {
        writeBytes(source, 0, source.length);
    }
}
