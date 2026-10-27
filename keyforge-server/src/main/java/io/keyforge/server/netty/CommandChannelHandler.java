package io.keyforge.server.netty;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespValue;
import io.keyforge.server.command.ClientSession;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.netty.RespRequestFrameDecoder.ProtocolError;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Executes decoded requests on the channel's event loop and writes the replies.
 *
 * <p><b>Batching.</b> Replies are written as each request completes but flushed once per read, in
 * {@link #channelReadComplete}, so a pipelined batch costs one syscall.
 *
 * <p><b>Backpressure.</b> A client can pipeline requests without reading replies. When the outbound buffer passes the
 * channel's high water mark the channel turns unwritable and this handler stops reading from the socket
 * ({@code autoRead = false}); it resumes once the client has drained replies below the low water mark. Server memory per
 * connection stays bounded by the water marks plus one read's worth of requests.
 */
final class CommandChannelHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(CommandChannelHandler.class);

    private final CommandDispatcher dispatcher;
    private final ClientSession session = new ClientSession();
    /** Set once the final reply (QUIT or a protocol error) is queued; later requests are ignored. */
    private boolean closing;

    CommandChannelHandler(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (closing) {
            return;
        }
        switch (msg) {
            case byte[][] request -> {
                RespValue reply = dispatcher.dispatch(session, request);
                ctx.write(reply, ctx.voidPromise());
                if (session.isCloseRequested()) {
                    closeAfterFlush(ctx);
                }
            }
            case ProtocolError error -> {
                ctx.write(RespError.err("Protocol error: " + error.message()), ctx.voidPromise());
                closeAfterFlush(ctx);
            }
            default -> ctx.fireChannelRead(msg);
        }
        if (!closing && !ctx.channel().isWritable()) {
            ctx.channel().config().setAutoRead(false);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable() && !closing) {
            ctx.channel().config().setAutoRead(true);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            log.debug("Connection {} failed: {}", ctx.channel().remoteAddress(), cause.toString());
        } else {
            log.warn("Unexpected error on connection {}", ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }

    private void closeAfterFlush(ChannelHandlerContext ctx) {
        closing = true;
        ctx.channel().config().setAutoRead(false);
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
}
