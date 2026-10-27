package io.keyforge.server.netty;

import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespSink;
import io.keyforge.protocol.RespValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** Encodes {@link RespValue} replies into a buffer sized exactly for the reply. One instance per channel. */
final class RespReplyEncoder extends MessageToByteEncoder<RespValue> {

    private final ByteBufSink sink = new ByteBufSink();

    RespReplyEncoder() {
        super(RespValue.class);
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, RespValue reply, boolean preferDirect) {
        int size = (int) Math.min(RespEncoder.encodedLength(reply), Integer.MAX_VALUE - 8);
        return preferDirect ? ctx.alloc().ioBuffer(size) : ctx.alloc().heapBuffer(size);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RespValue reply, ByteBuf out) {
        sink.target = out;
        try {
            RespEncoder.encode(reply, sink);
        } finally {
            sink.target = null;
        }
    }

    private static final class ByteBufSink implements RespSink {
        ByteBuf target;

        @Override
        public void writeByte(int value) {
            target.writeByte(value);
        }

        @Override
        public void writeBytes(byte[] source, int offset, int length) {
            target.writeBytes(source, offset, length);
        }
    }
}
