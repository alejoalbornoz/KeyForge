package io.keyforge.chaos;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;

import io.keyforge.protocol.RespDecoder;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespValue;

/**
 * A blocking RESP2 connection for one client process: one request at a time, each reply awaited with a timeout. Not
 * thread-safe.
 *
 * <p>{@link #open} fails before anything is sent; {@link #call} fails after the request may have reached the server.
 * The workload relies on that difference to tell failed operations from indeterminate ones.
 */
public final class RespConnection implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final RespDecoder decoder = new RespDecoder();
    private final byte[] chunk = new byte[16 * 1024];
    private ByteBuffer received = ByteBuffer.allocate(0);

    private RespConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    /**
     * Connects to a server.
     *
     * @param readTimeout how long {@link #call} waits for each reply
     * @throws IOException if the connection could not be established; nothing was sent
     */
    public static RespConnection open(InetSocketAddress address, Duration connectTimeout, Duration readTimeout)
            throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(Math.toIntExact(Math.max(1, readTimeout.toMillis())));
            InetSocketAddress resolved = address.isUnresolved()
                    ? new InetSocketAddress(address.getHostString(), address.getPort()) : address;
            socket.connect(resolved, Math.toIntExact(Math.max(1, connectTimeout.toMillis())));
            return new RespConnection(socket);
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
    }

    /**
     * Sends a command and waits for its reply.
     *
     * @throws java.net.SocketTimeoutException if no complete reply arrived within the read timeout
     * @throws IOException                     if the connection broke; the request may or may not have been received
     */
    public RespValue call(String... arguments) throws IOException {
        out.write(RespEncoder.encodeCommand(arguments));
        out.flush();
        while (true) {
            RespValue value = decoder.decode(received);
            if (value != null) {
                return value;
            }
            int read = in.read(chunk);
            if (read < 0) {
                throw new EOFException("the server closed the connection");
            }
            ByteBuffer combined = ByteBuffer.allocate(received.remaining() + read);
            combined.put(received).put(chunk, 0, read).flip();
            received = combined;
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            // Nothing to do: the connection is abandoned either way.
        }
    }
}
