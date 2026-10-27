package io.keyforge.server.vthreads;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.keyforge.protocol.ByteArrayRespSink;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespProtocolException;
import io.keyforge.protocol.RespRequestDecoder;
import io.keyforge.server.command.ClientSession;
import io.keyforge.server.command.CommandDispatcher;

/**
 * Serves one client connection with blocking I/O: read, execute every complete request, write the replies, repeat.
 *
 * <p>It runs on the connection's own virtual thread. Blocking in {@code read} or {@code write} unmounts the virtual
 * thread from its carrier, so a connection that waits costs no platform thread. Nothing here takes a lock: the session,
 * decoder and buffers are confined to this thread, and the command layer locks only around in-memory work (ADR 0002).
 * See {@code docs/adr/0005-virtual-thread-server-and-pinning.md}.
 *
 * <p>It behaves like the Netty handler (ADR 0004):
 * <ul>
 *   <li><b>Batching.</b> Replies are encoded into one buffer and written once per read, so a pipelined batch costs one
 *       write. A batch whose replies pass {@link #FLUSH_THRESHOLD} is written in pieces, which bounds the buffer.</li>
 *   <li><b>Backpressure.</b> It comes from blocking: while the client does not read, {@code write} blocks, and a
 *       thread blocked in {@code write} reads no further requests. TCP flow control then pushes back on the
 *       client.</li>
 *   <li><b>Final replies.</b> After {@code QUIT} or a protocol error, requests already received are ignored, the final
 *       reply is written, and {@link #serve} returns so that the caller closes the socket.</li>
 * </ul>
 */
final class Connection {

    /** Replies are written as soon as this many bytes are pending, even in the middle of a batch. */
    static final int FLUSH_THRESHOLD = 32 * 1024;

    /** A reply buffer that grew beyond this (a large value) is dropped after the write instead of being kept. */
    private static final int MAX_RETAINED_REPLY_BUFFER = 2 * FLUSH_THRESHOLD;

    private final CommandDispatcher dispatcher;
    private final RespRequestDecoder decoder;
    private final ReceiveBuffer received;
    private final ClientSession session = new ClientSession();
    private ByteArrayRespSink replies = new ByteArrayRespSink();

    Connection(CommandDispatcher dispatcher, RespRequestDecoder decoder, int initialReceiveBuffer) {
        this.dispatcher = dispatcher;
        this.decoder = decoder;
        this.received = new ReceiveBuffer(initialReceiveBuffer);
    }

    /**
     * Serves requests until the client disconnects, sends {@code QUIT} or sends malformed RESP. Closes neither stream.
     *
     * @throws IOException if reading or writing fails, which includes the socket being closed by the server
     */
    void serve(InputStream in, OutputStream out) throws IOException {
        while (received.readFrom(in) >= 0) {
            boolean keepOpen = executeReceived(out);
            flush(out);
            if (!keepOpen) {
                return;
            }
        }
    }

    /** Executes every complete request received so far. Returns false once the connection must close. */
    private boolean executeReceived(OutputStream out) throws IOException {
        ByteBuffer bytes = received.unconsumed();
        try {
            byte[][] request;
            while ((request = decoder.decode(bytes)) != null) {
                RespEncoder.encode(dispatcher.dispatch(session, request), replies);
                if (session.isCloseRequested()) {
                    return false;
                }
                if (replies.size() >= FLUSH_THRESHOLD) {
                    flush(out);
                }
            }
            return true;
        } catch (RespProtocolException e) {
            RespEncoder.encode(RespError.err("Protocol error: " + e.getMessage()), replies);
            return false;
        }
    }

    private void flush(OutputStream out) throws IOException {
        int pending = replies.size();
        if (pending == 0) {
            return;
        }
        replies.writeTo(out);
        out.flush();
        if (pending > MAX_RETAINED_REPLY_BUFFER) {
            replies = new ByteArrayRespSink();
        } else {
            replies.reset();
        }
    }
}
