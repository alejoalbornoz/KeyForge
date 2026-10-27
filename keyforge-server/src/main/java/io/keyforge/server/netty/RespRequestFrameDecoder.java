package io.keyforge.server.netty;

import java.nio.ByteBuffer;
import java.util.List;

import io.keyforge.protocol.RespProtocolException;
import io.keyforge.protocol.RespRequestDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Netty adapter for {@link RespRequestDecoder}: emits one {@code byte[][]} per request, as many per read as the
 * cumulated bytes contain (pipelining).
 *
 * <p>On malformed input it emits a single {@link ProtocolError} and discards everything after it, so the handler can
 * still answer the requests that preceded the bad bytes, in order, before replying with the error and closing.
 */
final class RespRequestFrameDecoder extends ByteToMessageDecoder {

    private final RespRequestDecoder decoder;
    private boolean failed;

    RespRequestFrameDecoder(RespRequestDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (failed) {
            in.skipBytes(in.readableBytes());
            return;
        }
        // A view of the readable bytes; the cumulation is a single buffer, so this does not copy.
        ByteBuffer view = in.nioBuffer();
        int start = view.position();
        try {
            byte[][] request;
            while ((request = decoder.decode(view)) != null) {
                out.add(request);
            }
        } catch (RespProtocolException e) {
            failed = true;
            out.add(new ProtocolError(e.getMessage()));
            in.skipBytes(in.readableBytes());
            return;
        }
        in.skipBytes(view.position() - start);
    }

    /** Signals that the client sent invalid RESP; the connection must be closed after the error reply. */
    record ProtocolError(String message) {
    }
}
