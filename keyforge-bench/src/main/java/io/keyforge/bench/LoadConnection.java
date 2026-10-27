package io.keyforge.bench;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Drives one connection: at most one request in flight, sent on a fixed schedule (open loop) or as soon as the previous
 * reply arrives (closed loop).
 *
 * <p><b>Coordinated omission.</b> In open loop, request {@code k} of a connection is due at
 * {@code first + k * interval}, whether or not the server kept up. It is sent at that time, or as soon as the reply to
 * request {@code k - 1} arrives if that is later, and its latency is measured from the time it was <em>due</em>. A
 * server that stalls for one second therefore shows up as a second's worth of slow requests, instead of as one slow
 * request followed by a pause in which the load generator politely sends nothing (the approach of wrk2). How late the
 * timer of a scheduled request fires is recorded separately as <em>timer lag</em>: it is the load generator's own
 * contribution to the measured latency, and a large one means the load generator could not keep up.
 *
 * <p>All methods run on the channel's event loop.
 */
final class LoadConnection extends ByteToMessageDecoder {

    private final LoadGenerator.Run run;
    private final SplittableRandom random;
    private ChannelHandlerContext ctx;

    /** When the in-flight request (or, between requests, the next one) is due. */
    private long dueNanos;
    private long sentNanos;
    private boolean inFlight;

    LoadConnection(LoadGenerator.Run run, long seed) {
        this.run = run;
        this.random = new SplittableRandom(seed);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext context) {
        this.ctx = context;
    }

    Channel channel() {
        return ctx.channel();
    }

    /** Starts traffic; the first request is due at {@code firstDueNanos}. */
    void start(long firstDueNanos) {
        dueNanos = firstDueNanos;
        sendWhenDue(System.nanoTime());
    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out) {
        int reply;
        while ((reply = ReplyScanner.skipReply(in)) != ReplyScanner.INCOMPLETE) {
            onReply(reply == ReplyScanner.ERROR);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        if (run.running()) {
            run.recordDisconnect();
        }
        super.channelInactive(context);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        if (run.running()) {
            run.recordFailure(cause);
        }
        context.close();
    }

    private void onReply(boolean error) {
        if (!inFlight) {
            throw new IllegalStateException("Reply without a request");
        }
        long now = System.nanoTime();
        inFlight = false;
        if (run.openLoop()) {
            run.recordLatency(now - dueNanos, error);
            dueNanos += run.intervalNanos();
        } else {
            run.recordLatency(now - sentNanos, error);
            dueNanos = now;
        }
        sendWhenDue(now);
    }

    private void sendWhenDue(long now) {
        if (!run.running()) {
            return;
        }
        long delay = dueNanos - now;
        if (delay > 0) {
            ctx.executor().schedule(this::sendOnTimer, delay, TimeUnit.NANOSECONDS);
        } else {
            send(now);
        }
    }

    private void sendOnTimer() {
        long now = System.nanoTime();
        if (run.running()) {
            run.recordTimerLag(now - dueNanos);
        }
        send(now);
    }

    private void send(long now) {
        if (!run.running() || !ctx.channel().isActive()) {
            return;
        }
        inFlight = true;
        sentNanos = now;
        byte[] request = run.templates().next(random);
        ByteBuf buffer = ctx.alloc().ioBuffer(request.length).writeBytes(request);
        ctx.writeAndFlush(buffer, ctx.voidPromise());
    }
}
